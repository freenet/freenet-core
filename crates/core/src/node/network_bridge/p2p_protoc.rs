use anyhow::anyhow;
use dashmap::DashSet;
use either::{Either, Left, Right};
use futures::FutureExt;
use futures::StreamExt;
use std::convert::Infallible;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::time::Duration;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, error::TryRecvError, Receiver, Sender};
use tokio::time::timeout;
use tracing::Instrument;

use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::contract::{ContractHandlerEvent, WaitingTransaction};
use crate::message::{NetMessageV1, QueryResult};
use crate::node::network_bridge::handshake::{
    Command as HandshakeCommand, CommandSender as HandshakeCommandSender, Event as HandshakeEvent,
    HandshakeHandler,
};
use crate::node::network_bridge::priority_select;
use crate::node::subscribe::SubscribeMsg;
use crate::node::{MessageProcessor, PeerId};
use crate::operations::{connect::ConnectMsg, get::GetMsg, put::PutMsg, update::UpdateMsg};
use crate::ring::Location;
use crate::transport::{
    create_connection_handler, OutboundConnectionHandler, PeerConnection, TransportError,
    TransportKeypair, TransportPublicKey,
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
use freenet_stdlib::client_api::{ContractResponse, HostResponse};

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
    conn_event_tx: Option<Sender<ConnEvent>>,
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

        // Initialize our peer identity before any connection attempts so join requests can
        // reference the correct address.
        let advertised_addr = {
            let advertised_ip = config
                .peer_id
                .as_ref()
                .map(|peer| peer.addr.ip())
                .or(config.config.network_api.public_address)
                .unwrap_or_else(|| {
                    if listener_ip.is_unspecified() {
                        IpAddr::V4(Ipv4Addr::LOCALHOST)
                    } else {
                        listener_ip
                    }
                });
            let advertised_port = config
                .peer_id
                .as_ref()
                .map(|peer| peer.addr.port())
                .or(config.config.network_api.public_port)
                .unwrap_or(listen_port);
            SocketAddr::new(advertised_ip, advertised_port)
        };
        bridge
            .op_manager
            .ring
            .connection_manager
            .try_set_peer_key(advertised_addr);

        Ok(P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            event_listener: Box::new(event_listener),
            connections: HashMap::new(),
            conn_event_tx: None,
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
            conn_event_tx: _,
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

        let (outbound_conn_handler, inbound_conn_handler) = create_connection_handler::<UdpSocket>(
            key_pair.clone(),
            listening_ip,
            listening_port,
            is_gateway,
            bandwidth_limit,
            if is_gateway { &[] } else { &gateways },
        )
        .await?;

        tracing::info!(
            %listening_port,
            %listening_ip,
            %is_gateway,
            key = %key_pair.public(),
            "Opening network listener - will receive from channel"
        );

        let mut state = EventListenerState::new(outbound_conn_handler.clone());

        let (conn_event_tx, conn_event_rx) = mpsc::channel(1024);

        // For non-gateway peers, pass the peer_ready flag so it can be set after first handshake
        // For gateways, pass None (they're always ready)
        let peer_ready = if !is_gateway {
            Some(bridge.op_manager.peer_ready.clone())
        } else {
            None
        };

        let (handshake_handler, handshake_cmd_sender) = HandshakeHandler::new(
            inbound_conn_handler,
            outbound_conn_handler.clone(),
            bridge.op_manager.ring.connection_manager.clone(),
            bridge.op_manager.ring.router.clone(),
            this_location,
            is_gateway,
            peer_ready,
        );

        let select_stream = priority_select::ProductionPrioritySelectStream::new(
            notification_channel.notifications_receiver,
            notification_channel.op_execution_receiver,
            conn_bridge_rx,
            handshake_handler,
            node_controller,
            client_wait_for_transaction,
            executor_listener,
            conn_event_rx,
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
            conn_event_tx: Some(conn_event_tx.clone()),
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

        while let Some(result) = select_stream.as_mut().next().await {
            // Process the result using the existing handler
            let event = ctx
                .process_select_result(result, &mut state, &handshake_cmd_sender)
                .await?;

            match event {
                EventResult::Continue => continue,
                EventResult::Event(event) => {
                    match *event {
                        ConnEvent::InboundMessage(inbound) => {
                            let remote = inbound.remote_addr;
                            let mut msg = inbound.msg;
                            tracing::info!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                remote = ?remote,
                                peer = %ctx.bridge.op_manager.ring.connection_manager.get_peer_key().unwrap(),
                                "Received inbound message from peer - processing"
                            );
                            // Only the hop that owns the transport socket (gateway/first hop in
                            // practice) knows the UDP source address; tag the connect request here
                            // so downstream relays don't guess at the joiner's address.
                            if let (
                                Some(remote_addr),
                                NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                                    payload,
                                    ..
                                })),
                            ) = (remote, &mut msg)
                            {
                                if payload.observed_addr.is_none() {
                                    payload.observed_addr = Some(remote_addr);
                                }
                            }
                            ctx.handle_inbound_message(msg, &op_manager, &mut state)
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

                            // Check if message targets self - if so, process locally instead of sending over network
                            let self_peer_id = ctx
                                .bridge
                                .op_manager
                                .ring
                                .connection_manager
                                .get_peer_key()
                                .unwrap();
                            if target_peer.peer == self_peer_id {
                                tracing::error!(
                                    tx = %msg.id(),
                                    msg_type = %msg,
                                    target_peer = %target_peer,
                                    self_peer = %self_peer_id,
                                    "BUG: OutboundMessage targets self! This indicates a routing logic error - messages should not reach OutboundMessage handler if they target self"
                                );
                                // Convert to InboundMessage and process locally
                                ctx.handle_inbound_message(msg, &op_manager, &mut state)
                                    .await?;
                                continue;
                            }

                            tracing::info!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                target_peer = %target_peer,
                                "Sending outbound message to peer"
                            );
                            // IMPORTANT: Use a single get() call to avoid TOCTOU race
                            // between contains_key() and get(). The connection can be
                            // removed by another task between those two calls.
                            let peer_connection = ctx
                                .connections
                                .get(&target_peer.peer)
                                .or_else(|| {
                                    if target_peer.peer.addr.ip().is_unspecified() {
                                        ctx.connection_entry_by_pub_key(&target_peer.peer.pub_key)
                                            .map(|(existing_peer, sender)| {
                                                tracing::info!(
                                                    tx = %msg.id(),
                                                    target_peer = %target_peer.peer,
                                                    resolved_addr = %existing_peer.addr,
                                                    "Resolved outbound connection using peer public key due to unspecified address"
                                                );
                                                sender
                                            })
                                    } else {
                                        None
                                    }
                                });
                            tracing::debug!(
                                tx = %msg.id(),
                                self_peer = %ctx.bridge.op_manager.ring.connection_manager.pub_key,
                                target = %target_peer.peer,
                                conn_map_size = ctx.connections.len(),
                                has_connection = peer_connection.is_some(),
                                "[CONN_TRACK] LOOKUP: Checking for existing connection in HashMap"
                            );
                            match peer_connection {
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
                                    let target_peer_id = target_peer.peer.clone();
                                    let msg_clone = msg.clone();
                                    let bridge_sender = ctx.bridge.ev_listener_tx.clone();
                                    let self_peer_id = ctx
                                        .bridge
                                        .op_manager
                                        .ring
                                        .connection_manager
                                        .get_peer_key();

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

                                    tracing::info!(
                                        tx = %tx,
                                        target = %target_peer_id,
                                        "connect_peer: dispatched connect request, waiting asynchronously"
                                    );

                                    tokio::spawn(async move {
                                        match timeout(Duration::from_secs(20), result.recv()).await
                                        {
                                            Ok(Some(Ok(_))) => {
                                                tracing::info!(
                                                    tx = %tx,
                                                    target = %target_peer_id,
                                                    self_peer = ?self_peer_id,
                                                    "connect_peer: connection established, rescheduling message send"
                                                );
                                                if let Err(e) = bridge_sender
                                                    .send(Left((
                                                        target_peer_id.clone(),
                                                        Box::new(msg_clone),
                                                    )))
                                                    .await
                                                {
                                                    tracing::error!(
                                                        tx = %tx,
                                                        target = %target_peer_id,
                                                        "connect_peer: failed to reschedule message after connection: {:?}",
                                                        e
                                                    );
                                                }
                                            }
                                            Ok(Some(Err(e))) => {
                                                tracing::error!(
                                                    tx = %tx,
                                                    target = %target_peer_id,
                                                    "connect_peer: connection attempt returned error: {:?}",
                                                    e
                                                );
                                            }
                                            Ok(None) => {
                                                tracing::error!(
                                                    tx = %tx,
                                                    target = %target_peer_id,
                                                    "connect_peer: response channel closed before connection result"
                                                );
                                            }
                                            Err(_) => {
                                                tracing::error!(
                                                    tx = %tx,
                                                    target = %target_peer_id,
                                                    "connect_peer: timeout waiting for connection result"
                                                );
                                            }
                                        }
                                    });
                                }
                            }
                        }
                        ConnEvent::TransportClosed { remote_addr, error } => {
                            tracing::debug!(
                                remote = %remote_addr,
                                ?error,
                                "Transport closed event received in event loop"
                            );
                        }
                        ConnEvent::ClosedChannel(reason) => {
                            match reason {
                                ChannelCloseReason::Bridge
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
                                        tracing::debug!(self_peer = %ctx.bridge.op_manager.ring.connection_manager.pub_key, %peer, conn_map_size = ctx.connections.len(), "[CONN_TRACK] REMOVE: ClosedChannel cleanup - removing from connections HashMap");
                                        ctx.connections.remove(&peer);

                                        // Notify handshake handler to clean up
                                        if let Err(error) = handshake_cmd_sender
                                            .send(HandshakeCommand::DropConnection {
                                                peer: peer.clone(),
                                            })
                                            .await
                                        {
                                            tracing::warn!(
                                                %peer,
                                                ?error,
                                                "Failed to drop connection during cleanup"
                                            );
                                        }
                                    }

                                    // Clean up reservations for in-progress connections
                                    // These are connections that started handshake but haven't completed yet
                                    // Notifying the callbacks will trigger the calling code to clean up reservations
                                    tracing::debug!(
                                        awaiting_count = state.awaiting_connection.len(),
                                        "Cleaning up in-progress connection reservations"
                                    );

                                    for (addr, mut callbacks) in state.awaiting_connection.drain() {
                                        tracing::debug!(%addr, callbacks = callbacks.len(), "Notifying awaiting connection of shutdown");
                                        // Best effort notification - ignore errors since we're shutting down anyway
                                        // The callback sender will handle cleanup on their side
                                        for mut callback in callbacks.drain(..) {
                                            let _ = callback.send_result(Err(())).await;
                                        }
                                    }

                                    tracing::info!("Cleanup complete, exiting event loop");
                                    break;
                                }
                            }
                        }
                        ConnEvent::NodeAction(action) => match action {
                            NodeEvent::DropConnection(peer) => {
                                tracing::debug!(self_peer = %ctx.bridge.op_manager.ring.connection_manager.pub_key, %peer, conn_map_size = ctx.connections.len(), "[CONN_TRACK] REMOVE: DropConnection event - removing from connections HashMap");
                                if let Err(error) = handshake_cmd_sender
                                    .send(HandshakeCommand::DropConnection { peer: peer.clone() })
                                    .await
                                {
                                    tracing::warn!(
                                        %peer,
                                        ?error,
                                        "Failed to enqueue DropConnection command"
                                    );
                                }
                                if let Some(conn) = ctx.connections.remove(&peer) {
                                    // TODO: review: this could potentially leave garbage tasks in the background with peer listener
                                    match timeout(
                                        Duration::from_secs(1),
                                        conn.send(Right(ConnEvent::NodeAction(
                                            NodeEvent::DropConnection(peer),
                                        ))),
                                    )
                                    .await
                                    {
                                        Ok(Ok(())) => {}
                                        Ok(Err(send_error)) => {
                                            tracing::error!(
                                                ?send_error,
                                                "Failed to send drop connection message"
                                            );
                                        }
                                        Err(elapsed) => {
                                            tracing::error!(
                                                ?elapsed,
                                                "Timeout while sending drop connection message"
                                            );
                                        }
                                    }
                                }
                            }
                            NodeEvent::ConnectPeer {
                                peer,
                                tx,
                                callback,
                                is_gw: courtesy,
                            } => {
                                tracing::info!(
                                    tx = %tx,
                                    remote = %peer,
                                    remote_addr = %peer.addr,
                                    courtesy,
                                    "NodeEvent::ConnectPeer received"
                                );
                                ctx.handle_connect_peer(
                                    peer,
                                    Box::new(callback),
                                    tx,
                                    &handshake_cmd_sender,
                                    &mut state,
                                    courtesy,
                                )
                                .await?;
                            }
                            NodeEvent::ExpectPeerConnection { peer, courtesy } => {
                                tracing::debug!(%peer, courtesy, "ExpectPeerConnection event received; registering inbound expectation via handshake driver");
                                state.outbound_handler.expect_incoming(peer.addr);
                                if let Err(error) = handshake_cmd_sender
                                    .send(HandshakeCommand::ExpectInbound {
                                        peer: peer.clone(),
                                        transaction: None,
                                        courtesy,
                                    })
                                    .await
                                {
                                    tracing::warn!(
                                        %peer,
                                        ?error,
                                        "Failed to enqueue ExpectInbound command; inbound connection may be dropped"
                                    );
                                }
                            }
                            NodeEvent::QueryConnections { callback } => {
                                let connections = ctx.connections.keys().cloned().collect();
                                match timeout(
                                    Duration::from_secs(1),
                                    callback.send(QueryResult::Connections(connections)),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(send_error)) => {
                                        tracing::error!(
                                            ?send_error,
                                            "Failed to send connections query result"
                                        );
                                    }
                                    Err(elapsed) => {
                                        tracing::error!(
                                            ?elapsed,
                                            "Timeout while sending connections query result"
                                        );
                                    }
                                }
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

                                match timeout(
                                    Duration::from_secs(1),
                                    callback.send(QueryResult::NetworkDebug(debug_info)),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(send_error)) => {
                                        tracing::error!(
                                            ?send_error,
                                            "Failed to send subscriptions query result"
                                        );
                                    }
                                    Err(elapsed) => {
                                        tracing::error!(
                                            ?elapsed,
                                            "Timeout while sending subscriptions query result"
                                        );
                                    }
                                }
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

                                match timeout(
                                    Duration::from_secs(2),
                                    callback.send(QueryResult::NodeDiagnostics(response)),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(send_error)) => {
                                        tracing::error!(
                                            ?send_error,
                                            "Failed to send node diagnostics query result"
                                        );
                                    }
                                    Err(elapsed) => {
                                        tracing::error!(
                                            ?elapsed,
                                            "Timeout while sending node diagnostics query result"
                                        );
                                    }
                                }
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
                                tracing::debug!(%tx, %key, "local subscribe complete");

                                if !op_manager.is_sub_operation(tx) {
                                    let response = Ok(HostResponse::ContractResponse(
                                        ContractResponse::SubscribeResponse { key, subscribed },
                                    ));

                                    match op_manager.result_router_tx.send((tx, response)).await {
                                        Ok(()) => {
                                            tracing::debug!(%tx, "sent subscribe response to client");
                                            if let Some(clients) = state.tx_to_client.remove(&tx) {
                                                tracing::debug!(
                                                    "LocalSubscribeComplete removed {} waiting clients for transaction {}",
                                                    clients.len(),
                                                    tx
                                                );
                                            } else if let Some(pos) = state
                                                .client_waiting_transaction
                                                .iter()
                                                .position(|(waiting, _)| match waiting {
                                                    WaitingTransaction::Subscription {
                                                        contract_key,
                                                    } => contract_key == key.id(),
                                                    _ => false,
                                                })
                                            {
                                                let (_, clients) =
                                                    state.client_waiting_transaction.remove(pos);
                                                tracing::debug!(
                                                    "LocalSubscribeComplete for {} matched {} subscription waiters via contract {}",
                                                    tx,
                                                    clients.len(),
                                                    key
                                                );
                                            } else {
                                                tracing::warn!(
                                                    "LocalSubscribeComplete for {} found no waiting clients",
                                                    tx
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!(%tx, error = %e, "failed to send subscribe response")
                                        }
                                    }
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
        handshake_commands: &HandshakeCommandSender,
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
                    "PrioritySelect: connection events READY"
                );
                self.handle_transport_event(msg, state, handshake_commands)
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
                    Some(event) => {
                        self.handle_handshake_action(event, state).await?;
                        Ok(EventResult::Continue)
                    }
                    None => {
                        tracing::warn!(
                            "Handshake handler stream closed; notifying pending callbacks"
                        );
                        self.handle_handshake_stream_closed(state).await?;
                        Ok(EventResult::Continue)
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
        op_manager: &Arc<OpManager>,
        state: &mut EventListenerState,
    ) -> anyhow::Result<()> {
        let tx = *msg.id();
        tracing::debug!(
            %tx,
            tx_type = ?tx.transaction_type(),
            "Handling inbound NetMessage at event loop"
        );
        match msg {
            NetMessage::V1(NetMessageV1::Aborted(tx)) => {
                handle_aborted_op(tx, op_manager, &self.gateways).await?;
            }
            msg => {
                self.process_message(msg, op_manager, None, state).await;
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

    fn connection_entry_by_pub_key(
        &self,
        pub_key: &TransportPublicKey,
    ) -> Option<(&PeerId, &PeerConnChannelSender)> {
        self.connections
            .iter()
            .find(|(peer_id, _)| peer_id.pub_key == *pub_key)
    }

    async fn handle_connect_peer(
        &mut self,
        peer: PeerId,
        mut callback: Box<dyn ConnectResultSender>,
        tx: Transaction,
        handshake_commands: &HandshakeCommandSender,
        state: &mut EventListenerState,
        courtesy: bool,
    ) -> anyhow::Result<()> {
        let mut peer = peer;
        let mut peer_addr = peer.addr;

        if peer_addr.ip().is_unspecified() {
            if let Some((existing_peer, _)) = self.connection_entry_by_pub_key(&peer.pub_key) {
                peer_addr = existing_peer.addr;
                peer.addr = existing_peer.addr;
                tracing::info!(
                    tx = %tx,
                    remote = %peer,
                    fallback_addr = %peer_addr,
                    courtesy,
                    "ConnectPeer provided unspecified address; using existing connection address"
                );
            } else {
                tracing::debug!(
                    tx = %tx,
                    courtesy,
                    "ConnectPeer received unspecified address without existing connection reference"
                );
            }
        }

        tracing::info!(
            tx = %tx,
            remote = %peer,
            remote_addr = %peer_addr,
            courtesy,
            "Connecting to peer"
        );
        if let Some(blocked_addrs) = &self.blocked_addresses {
            if blocked_addrs.contains(&peer.addr) {
                tracing::info!(
                    tx = %tx,
                    remote = %peer.addr,
                    "Outgoing connection to peer blocked by local policy"
                );
                callback
                    .send_result(Err(()))
                    .await
                    .inspect_err(|error| {
                        tracing::debug!(
                            remote = %peer.addr,
                            ?error,
                            "Failed to notify caller about blocked connection"
                        );
                    })
                    .ok();
                return Ok(());
            }
            tracing::debug!(
                tx = %tx,
                "Blocked addresses: {:?}, peer addr: {}",
                blocked_addrs,
                peer.addr
            );
        }

        match state.awaiting_connection.entry(peer_addr) {
            std::collections::hash_map::Entry::Occupied(mut callbacks) => {
                let txs_entry = state.awaiting_connection_txs.entry(peer_addr).or_default();
                if !txs_entry.contains(&tx) {
                    txs_entry.push(tx);
                }
                tracing::debug!(
                    tx = %tx,
                    remote = %peer_addr,
                    pending = callbacks.get().len(),
                    courtesy,
                    "Connection already pending, queuing additional requester"
                );
                callbacks.get_mut().push(callback);
                tracing::info!(
                    tx = %tx,
                    remote = %peer_addr,
                    pending = callbacks.get().len(),
                    pending_txs = ?txs_entry,
                    courtesy,
                    "connect_peer: connection already pending, queued callback"
                );
                return Ok(());
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let txs_entry = state.awaiting_connection_txs.entry(peer_addr).or_default();
                txs_entry.push(tx);
                tracing::debug!(
                        tx = %tx,
                    remote = %peer_addr,
                    courtesy,
                    "connect_peer: registering new pending connection"
                );
                entry.insert(vec![callback]);
                tracing::info!(
                tx = %tx,
                remote = %peer_addr,
                pending = 1,
                    pending_txs = ?txs_entry,
                    courtesy,
                    "connect_peer: registered new pending connection"
                );
                state.outbound_handler.expect_incoming(peer_addr);
            }
        }

        if let Err(error) = handshake_commands
            .send(HandshakeCommand::Connect {
                peer: peer.clone(),
                transaction: tx,
                courtesy,
            })
            .await
        {
            tracing::warn!(
                tx = %tx,
                remote = %peer.addr,
                courtesy,
                ?error,
                "Failed to enqueue connect command"
            );
            self.bridge
                .op_manager
                .ring
                .connection_manager
                .prune_in_transit_connection(&peer);
            let pending_txs = state.awaiting_connection_txs.remove(&peer_addr);
            if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
                tracing::debug!(
                    tx = %tx,
                    remote = %peer_addr,
                    callbacks = callbacks.len(),
                    courtesy,
                    "Cleaning up callbacks after connect command failure"
                );
                for mut cb in callbacks {
                    cb.send_result(Err(()))
                        .await
                        .inspect_err(|send_err| {
                            tracing::debug!(
                                remote = %peer_addr,
                                ?send_err,
                                "Failed to deliver connect command failure to awaiting callback"
                            );
                        })
                        .ok();
                }
            }
            if let Some(pending_txs) = pending_txs {
                tracing::debug!(
                    remote = %peer_addr,
                    pending_txs = ?pending_txs,
                    "Removed pending transactions after connect command failure"
                );
            }
        } else {
            tracing::debug!(
                tx = %tx,
                remote = %peer_addr,
                courtesy,
                "connect_peer: handshake command dispatched"
            );
        }

        Ok(())
    }

    async fn handle_handshake_action(
        &mut self,
        event: HandshakeEvent,
        state: &mut EventListenerState,
    ) -> anyhow::Result<()> {
        tracing::info!(?event, "handle_handshake_action: received handshake event");
        match event {
            HandshakeEvent::InboundConnection {
                transaction,
                peer,
                connection,
                courtesy,
            } => {
                let remote_addr = connection.remote_addr();

                if let Some(blocked_addrs) = &self.blocked_addresses {
                    if blocked_addrs.contains(&remote_addr) {
                        tracing::info!(
                            remote = %remote_addr,
                            courtesy,
                            transaction = ?transaction,
                            "Inbound connection blocked by local policy"
                        );
                        return Ok(());
                    }
                }

                let peer_id = peer.unwrap_or_else(|| {
                    tracing::info!(
                        remote = %remote_addr,
                        courtesy,
                        transaction = ?transaction,
                        "Inbound connection arrived without matching expectation; accepting provisionally"
                    );
                    PeerId::new(
                        remote_addr,
                        (*self
                            .bridge
                            .op_manager
                            .ring
                            .connection_manager
                            .pub_key)
                            .clone(),
                    )
                });

                tracing::info!(
                    remote = %peer_id.addr,
                    courtesy,
                    transaction = ?transaction,
                    "Inbound connection established"
                );

                self.handle_successful_connection(
                    peer_id,
                    connection,
                    state,
                    select_stream,
                    None,
                    courtesy,
                )
                .await?;
            }
            HandshakeEvent::OutboundEstablished {
                transaction,
                peer,
                connection,
                courtesy,
            } => {
                tracing::info!(
                    remote = %peer.addr,
                    courtesy,
                    transaction = %transaction,
                    "Outbound connection established"
                );
                self.handle_successful_connection(
                    peer,
                    connection,
                    state,
                    select_stream,
                    None,
                    courtesy,
                )
                .await?;
            }
            HandshakeEvent::OutboundFailed {
                transaction,
                peer,
                error,
                courtesy,
            } => {
                tracing::info!(
                    remote = %peer.addr,
                    courtesy,
                    transaction = %transaction,
                    ?error,
                    "Outbound connection failed"
                );

                self.bridge
                    .op_manager
                    .ring
                    .connection_manager
                    .prune_in_transit_connection(&peer);

                let pending_txs = state
                    .awaiting_connection_txs
                    .remove(&peer.addr)
                    .unwrap_or_default();

                if let Some(callbacks) = state.awaiting_connection.remove(&peer.addr) {
                    tracing::debug!(
                        remote = %peer.addr,
                        callbacks = callbacks.len(),
                        pending_txs = ?pending_txs,
                        courtesy,
                        "Notifying callbacks after outbound failure"
                    );

                    let mut callbacks = callbacks.into_iter();
                    if let Some(mut cb) = callbacks.next() {
                        cb.send_result(Err(()))
                            .await
                            .inspect_err(|err| {
                                tracing::debug!(
                                    remote = %peer.addr,
                                    ?err,
                                    "Failed to deliver outbound failure notification"
                                );
                            })
                            .ok();
                    }
                    for mut cb in callbacks {
                        cb.send_result(Err(()))
                            .await
                            .inspect_err(|err| {
                                tracing::debug!(
                                    remote = %peer.addr,
                                    ?err,
                                    "Failed to deliver secondary outbound failure notification"
                                );
                            })
                            .ok();
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_handshake_stream_closed(
        &mut self,
        state: &mut EventListenerState,
    ) -> anyhow::Result<()> {
        if state.awaiting_connection.is_empty() {
            return Ok(());
        }

        tracing::warn!(
            awaiting = state.awaiting_connection.len(),
            "Handshake driver closed; notifying pending callbacks"
        );

        let awaiting = std::mem::take(&mut state.awaiting_connection);
        let awaiting_txs = std::mem::take(&mut state.awaiting_connection_txs);

        for (addr, callbacks) in awaiting {
            let pending_txs = awaiting_txs.get(&addr).cloned().unwrap_or_default();
            tracing::debug!(
                remote = %addr,
                callbacks = callbacks.len(),
                pending_txs = ?pending_txs,
                "Delivering handshake driver shutdown notification"
            );
            for mut cb in callbacks {
                cb.send_result(Err(()))
                    .await
                    .inspect_err(|err| {
                        tracing::debug!(
                            remote = %addr,
                            ?err,
                            "Failed to deliver handshake driver shutdown notification"
                        );
                    })
                    .ok();
            }
        }

        Ok(())
    }

    async fn handle_successful_connection(
        &mut self,
        peer_id: PeerId,
        connection: PeerConnection,
        state: &mut EventListenerState,
        remaining_checks: Option<usize>,
        courtesy: bool,
    ) -> anyhow::Result<()> {
        let pending_txs = state
            .awaiting_connection_txs
            .remove(&peer_id.addr)
            .unwrap_or_default();
        if let Some(callbacks) = state.awaiting_connection.remove(&peer_id.addr) {
            let connection_manager = &self.bridge.op_manager.ring.connection_manager;
            let resolved_peer_id = if let Some(peer_id) = connection_manager.get_peer_key() {
                peer_id
            } else {
                let self_addr = connection
                    .my_address()
                    .ok_or_else(|| anyhow::anyhow!("self addr should be set"))?;
                connection_manager.try_set_peer_key(self_addr);
                connection_manager
                    .get_peer_key()
                    .expect("peer key should be set after try_set_peer_key")
            };
            tracing::debug!(
                remote = %peer_id.addr,
                callbacks = callbacks.len(),
                "handle_successful_connection: notifying waiting callbacks"
            );
            tracing::info!(
                remote = %peer_id.addr,
                callbacks = callbacks.len(),
                pending_txs = ?pending_txs,
                remaining_checks = ?remaining_checks,
                "handle_successful_connection: connection established"
            );
            for mut cb in callbacks {
                match timeout(
                    Duration::from_secs(60),
                    cb.send_result(Ok((resolved_peer_id.clone(), remaining_checks))),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(())) => {
                        tracing::debug!(
                            remote = %peer_id.addr,
                            "Callback dropped before receiving connection result"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            remote = %peer_id.addr,
                            ?error,
                            "Failed to deliver connection result"
                        );
                    }
                }
            }
        } else {
            tracing::warn!(
                %peer_id,
                pending_txs = ?pending_txs,
                "No callback for connection established"
            );
        }

        // Only insert if connection doesn't already exist to avoid dropping existing channel
        let mut newly_inserted = false;
        if !self.connections.contains_key(&peer_id) {
            let (tx, rx) = mpsc::channel(10);
            tracing::debug!(self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key, %peer_id, conn_map_size = self.connections.len(), "[CONN_TRACK] INSERT: OutboundConnectionSuccessful - adding to connections HashMap");
            self.connections.insert(peer_id.clone(), tx);
            let Some(conn_events) = self.conn_event_tx.as_ref().cloned() else {
                anyhow::bail!("Connection event channel not initialized");
            };
            let listener_peer = peer_id.clone();
            tokio::spawn(async move {
                peer_connection_listener(rx, connection, listener_peer, conn_events).await;
            });
            newly_inserted = true;
        } else {
            tracing::debug!(self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key, %peer_id, conn_map_size = self.connections.len(), "[CONN_TRACK] SKIP INSERT: OutboundConnectionSuccessful - connection already exists in HashMap");
        }

        if newly_inserted {
            let pending_loc = self
                .bridge
                .op_manager
                .ring
                .connection_manager
                .prune_in_transit_connection(&peer_id);
            let loc = pending_loc.unwrap_or_else(|| Location::from_address(&peer_id.addr));
            let eviction_candidate = self
                .bridge
                .op_manager
                .ring
                .add_connection(loc, peer_id.clone(), false, courtesy)
                .await;
            if let Some(victim) = eviction_candidate {
                if victim == peer_id {
                    tracing::debug!(
                        %peer_id,
                        "Courtesy eviction candidate matched current connection; skipping drop"
                    );
                } else {
                    tracing::info!(
                        %victim,
                        %peer_id,
                        courtesy_limit = true,
                        "Courtesy connection budget exceeded; dropping oldest courtesy peer"
                    );
                    if let Err(error) = self.bridge.drop_connection(&victim).await {
                        tracing::warn!(
                            %victim,
                            ?error,
                            "Failed to drop courtesy connection after hitting budget"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_transport_event(
        &mut self,
        event: Option<ConnEvent>,
        state: &mut EventListenerState,
        handshake_commands: &HandshakeCommandSender,
    ) -> anyhow::Result<EventResult> {
        let _ = state;
        match event {
            Some(ConnEvent::InboundMessage(mut inbound)) => {
                let tx = *inbound.msg.id();

                if let Some(remote_addr) = inbound.remote_addr {
                    if let Some(sender_peer) = extract_sender_from_message(&inbound.msg) {
                        if sender_peer.peer.addr == remote_addr
                            || sender_peer.peer.addr.ip().is_unspecified()
                        {
                            let mut new_peer_id = sender_peer.peer.clone();
                            if new_peer_id.addr.ip().is_unspecified() {
                                new_peer_id.addr = remote_addr;
                                if let Some(sender_mut) =
                                    extract_sender_from_message_mut(&mut inbound.msg)
                                {
                                    if sender_mut.peer.addr.ip().is_unspecified() {
                                        sender_mut.peer.addr = remote_addr;
                                    }
                                }
                            }
                            if let Some(existing_key) = self
                                .connections
                                .keys()
                                .find(|peer| {
                                    peer.addr == remote_addr && peer.pub_key != new_peer_id.pub_key
                                })
                                .cloned()
                            {
                                if let Some(channel) = self.connections.remove(&existing_key) {
                                    tracing::info!(
                                        remote = %remote_addr,
                                        old_peer = %existing_key,
                                        new_peer = %new_peer_id,
                                        "Updating provisional peer identity after inbound message"
                                    );
                                    self.bridge.op_manager.ring.update_connection_identity(
                                        &existing_key,
                                        new_peer_id.clone(),
                                    );
                                    self.connections.insert(new_peer_id, channel);
                                }
                            }
                        }
                    }
                }

                tracing::debug!(
                    peer_addr = ?inbound.remote_addr,
                    %tx,
                    tx_type = ?tx.transaction_type(),
                    "Queueing inbound NetMessage from peer connection"
                );
                Ok(EventResult::Event(
                    ConnEvent::InboundMessage(inbound).into(),
                ))
            }
            Some(ConnEvent::TransportClosed { remote_addr, error }) => {
                tracing::debug!(
                    remote = %remote_addr,
                    ?error,
                    "peer_connection_listener reported transport closure"
                );
                if let Some(peer) = self
                    .connections
                    .keys()
                    .find_map(|k| (k.addr == remote_addr).then(|| k.clone()))
                {
                    tracing::debug!(self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key, %peer, socket_addr = %remote_addr, conn_map_size = self.connections.len(), "[CONN_TRACK] REMOVE: TransportClosed - removing from connections HashMap");
                    self.bridge
                        .op_manager
                        .ring
                        .prune_connection(peer.clone())
                        .await;
                    self.connections.remove(&peer);
                    if let Err(error) = handshake_commands
                        .send(HandshakeCommand::DropConnection { peer: peer.clone() })
                        .await
                    {
                        tracing::warn!(
                            remote = %remote_addr,
                            ?error,
                            "Failed to notify handshake driver about dropped connection"
                        );
                    }
                }
                Ok(EventResult::Continue)
            }
            Some(other) => {
                tracing::warn!(?other, "Unexpected event from peer connection listener");
                Ok(EventResult::Continue)
            }
            None => {
                tracing::error!("All peer connection event channels closed");
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

                    tracing::debug!(
                        tx = %msg.id(),
                        msg_type = %msg,
                        target_peer = %target,
                        self_peer = %self_peer,
                        target_equals_self = (target.peer == self_peer),
                        "[ROUTING] handle_notification_msg: Checking if message targets self"
                    );

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
                EventResult::Event(ConnEvent::InboundMessage(msg.into()).into())
            }
            Some(Right(action)) => {
                tracing::info!(
                    event = %action,
                    "handle_notification_msg: Received NodeEvent notification"
                );
                EventResult::Event(ConnEvent::NodeAction(action).into())
            }
            None => EventResult::Event(
                ConnEvent::ClosedChannel(ChannelCloseReason::Notification).into(),
            ),
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
                EventResult::Event(ConnEvent::InboundMessage(msg.into()).into())
            }
            None => {
                EventResult::Event(ConnEvent::ClosedChannel(ChannelCloseReason::OpExecution).into())
            }
        }
    }

    fn handle_bridge_msg(&self, msg: Option<P2pBridgeEvent>) -> EventResult {
        match msg {
            Some(Left((_target, msg))) => {
                EventResult::Event(ConnEvent::OutboundMessage(*msg).into())
            }
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
                let entry = state.tx_to_client.entry(tx).or_default();
                let inserted = entry.insert(client_id);
                tracing::debug!(
                    "tx_to_client: tx={} client={} inserted={} total_waiting_clients={}",
                    tx,
                    client_id,
                    inserted,
                    entry.len()
                );
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
        result: Result<(PeerId, Option<usize>), ()>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>>;
}

impl ConnectResultSender for mpsc::Sender<Result<(PeerId, Option<usize>), ()>> {
    fn send_result(
        &mut self,
        result: Result<(PeerId, Option<usize>), ()>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>> {
        async move { self.send(result).await.map_err(|_| ()) }.boxed()
    }
}

struct EventListenerState {
    outbound_handler: OutboundConnectionHandler,
    pending_from_executor: HashSet<Transaction>,
    // FIXME: we are potentially leaving trash here when transacrions are completed
    tx_to_client: HashMap<Transaction, HashSet<ClientId>>,
    client_waiting_transaction: Vec<(WaitingTransaction, HashSet<ClientId>)>,
    awaiting_connection: HashMap<SocketAddr, Vec<Box<dyn ConnectResultSender>>>,
    awaiting_connection_txs: HashMap<SocketAddr, Vec<Transaction>>,
    pending_op_results: HashMap<Transaction, Sender<NetMessage>>,
}

impl EventListenerState {
    fn new(outbound_handler: OutboundConnectionHandler) -> Self {
        Self {
            outbound_handler,
            pending_from_executor: HashSet::new(),
            tx_to_client: HashMap::new(),
            client_waiting_transaction: Vec::new(),
            awaiting_connection: HashMap::new(),
            pending_op_results: HashMap::new(),
            awaiting_connection_txs: HashMap::new(),
        }
    }
}

enum EventResult {
    Continue,
    Event(Box<ConnEvent>),
}

#[derive(Debug)]
pub(super) enum ConnEvent {
    InboundMessage(IncomingMessage),
    OutboundMessage(NetMessage),
    NodeAction(NodeEvent),
    ClosedChannel(ChannelCloseReason),
    TransportClosed {
        remote_addr: SocketAddr,
        error: TransportError,
    },
}

#[derive(Debug)]
pub(super) struct IncomingMessage {
    pub msg: NetMessage,
    pub remote_addr: Option<SocketAddr>,
}

impl IncomingMessage {
    fn with_remote(msg: NetMessage, remote_addr: SocketAddr) -> Self {
        Self {
            msg,
            remote_addr: Some(remote_addr),
        }
    }
}

impl From<NetMessage> for IncomingMessage {
    fn from(msg: NetMessage) -> Self {
        Self {
            msg,
            remote_addr: None,
        }
    }
}

#[derive(Debug)]
pub(super) enum ChannelCloseReason {
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

async fn handle_peer_channel_message(
    conn: &mut PeerConnection,
    msg: Either<NetMessage, ConnEvent>,
) -> Result<(), TransportError> {
    match msg {
        Left(msg) => {
            tracing::debug!(to=%conn.remote_addr() ,"Sending message to peer. Msg: {msg}");
            if let Err(error) = conn.send(msg).await {
                tracing::error!(
                    to = %conn.remote_addr(),
                    ?error,
                    "[CONN_LIFECYCLE] Failed to send message to peer"
                );
                return Err(error);
            }
            tracing::debug!(
                to = %conn.remote_addr(),
                "[CONN_LIFECYCLE] Message enqueued on transport socket"
            );
        }
        Right(action) => {
            tracing::debug!(to=%conn.remote_addr(), "Received action from channel");
            match action {
                ConnEvent::NodeAction(NodeEvent::DropConnection(peer)) => {
                    tracing::info!(
                        to = %conn.remote_addr(),
                        peer = %peer,
                        "[CONN_LIFECYCLE] Closing connection per DropConnection action"
                    );
                    return Err(TransportError::ConnectionClosed(conn.remote_addr()));
                }
                ConnEvent::ClosedChannel(reason) => {
                    tracing::info!(
                        to = %conn.remote_addr(),
                        reason = ?reason,
                        "[CONN_LIFECYCLE] Closing connection due to ClosedChannel action"
                    );
                    return Err(TransportError::ConnectionClosed(conn.remote_addr()));
                }
                other => {
                    unreachable!(
                        "Unexpected action from peer_connection_listener channel: {:?}",
                        other
                    );
                }
            }
        }
    }
    Ok(())
}

async fn notify_transport_closed(
    sender: &Sender<ConnEvent>,
    remote_addr: SocketAddr,
    error: TransportError,
) {
    if sender
        .send(ConnEvent::TransportClosed { remote_addr, error })
        .await
        .is_err()
    {
        tracing::debug!(
            remote = %remote_addr,
            "[CONN_LIFECYCLE] conn_events receiver dropped before handling closure event"
        );
    }
}

async fn peer_connection_listener(
    mut rx: PeerConnChannelRecv,
    mut conn: PeerConnection,
    peer_id: PeerId,
    conn_events: Sender<ConnEvent>,
) {
    const MAX_IMMEDIATE_SENDS: usize = 32;
    let remote_addr = conn.remote_addr();
    tracing::debug!(
        to = %remote_addr,
        peer = %peer_id,
        "[CONN_LIFECYCLE] Starting peer_connection_listener task"
    );
    loop {
        let mut drained = 0;
        loop {
            match rx.try_recv() {
                Ok(msg) => {
                    if let Err(error) = handle_peer_channel_message(&mut conn, msg).await {
                        tracing::debug!(
                            to = %remote_addr,
                            ?error,
                            "[CONN_LIFECYCLE] Shutting down connection after send failure"
                        );
                        notify_transport_closed(&conn_events, remote_addr, error).await;
                        return;
                    }
                    drained += 1;
                    if drained >= MAX_IMMEDIATE_SENDS {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    tracing::warn!(
                        to = %conn.remote_addr(),
                        "[CONN_LIFECYCLE] peer_connection_listener channel closed without explicit DropConnection"
                    );
                    notify_transport_closed(
                        &conn_events,
                        remote_addr,
                        TransportError::ConnectionClosed(remote_addr),
                    )
                    .await;
                    return;
                }
            }
        }

        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    tracing::warn!(
                        to = %conn.remote_addr(),
                        "[CONN_LIFECYCLE] peer_connection_listener channel closed without explicit DropConnection"
                    );
                    notify_transport_closed(
                        &conn_events,
                        remote_addr,
                        TransportError::ConnectionClosed(remote_addr),
                    )
                    .await;
                    return;
                };
                if let Err(error) = handle_peer_channel_message(&mut conn, msg).await {
                    tracing::debug!(
                        to = %remote_addr,
                        ?error,
                        "[CONN_LIFECYCLE] Connection closed after channel command"
                    );
                    notify_transport_closed(&conn_events, remote_addr, error).await;
                    return;
                }
            }
            msg = conn.recv() => {
                match msg {
                    Ok(msg) => {
                        match decode_msg(&msg) {
                            Ok(net_message) => {
                                let tx = *net_message.id();
                                tracing::debug!(
                                    from = %conn.remote_addr(),
                                    %tx,
                                    tx_type = ?tx.transaction_type(),
                                    msg_type = %net_message,
                                    "[CONN_LIFECYCLE] Received inbound NetMessage from peer"
                                );
                                if conn_events.send(ConnEvent::InboundMessage(IncomingMessage::with_remote(net_message, remote_addr))).await.is_err() {
                                    tracing::debug!(
                                        from = %remote_addr,
                                        "[CONN_LIFECYCLE] conn_events receiver dropped; stopping listener"
                                    );
                                    return;
                                }
                            }
                            Err(error) => {
                                tracing::error!(
                                    from = %conn.remote_addr(),
                                    ?error,
                                    "[CONN_LIFECYCLE] Failed to deserialize inbound message; closing connection"
                                );
                                let transport_error = TransportError::Other(anyhow!(
                                    "Failed to deserialize inbound message from {remote_addr}: {error:?}"
                                ));
                                notify_transport_closed(
                                    &conn_events,
                                    remote_addr,
                                    transport_error,
                                )
                                .await;
                                return;
                            }
                        }
                    }
                    Err(error) => {
                        tracing::debug!(
                            from = %conn.remote_addr(),
                            ?error,
                            "[CONN_LIFECYCLE] peer_connection_listener terminating after recv error"
                        );
                        notify_transport_closed(&conn_events, remote_addr, error).await;
                        return;
                    }
                }
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
            NetMessageV1::Connect(connect_msg) => match connect_msg {
                ConnectMsg::Response { sender, .. } => Some(sender.clone()),
                ConnectMsg::Request { from, .. } => Some(from.clone()),
                ConnectMsg::ObservedAddress { target, .. } => Some(target.clone()),
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

fn extract_sender_from_message_mut(msg: &mut NetMessage) -> Option<&mut PeerKeyLocation> {
    match msg {
        NetMessage::V1(msg_v1) => match msg_v1 {
            NetMessageV1::Connect(connect_msg) => match connect_msg {
                ConnectMsg::Response { sender, .. } => Some(sender),
                ConnectMsg::Request { from, .. } => Some(from),
                ConnectMsg::ObservedAddress { target, .. } => Some(target),
            },
            NetMessageV1::Get(get_msg) => match get_msg {
                GetMsg::SeekNode { sender, .. } => Some(sender),
                GetMsg::ReturnGet { sender, .. } => Some(sender),
                _ => None,
            },
            NetMessageV1::Put(put_msg) => match put_msg {
                PutMsg::SeekNode { sender, .. } => Some(sender),
                PutMsg::SuccessfulPut { sender, .. } => Some(sender),
                PutMsg::PutForward { sender, .. } => Some(sender),
                _ => None,
            },
            NetMessageV1::Update(update_msg) => match update_msg {
                UpdateMsg::SeekNode { sender, .. } => Some(sender),
                UpdateMsg::Broadcasting { sender, .. } => Some(sender),
                UpdateMsg::BroadcastTo { sender, .. } => Some(sender),
                _ => None,
            },
            NetMessageV1::Subscribe(subscribe_msg) => match subscribe_msg {
                SubscribeMsg::SeekNode { subscriber, .. } => Some(subscriber),
                SubscribeMsg::ReturnSub { sender, .. } => Some(sender),
                _ => None,
            },
            _ => None,
        },
    }
}

// TODO: add testing for the network loop, now it should be possible to do since we don't depend upon having real connections
