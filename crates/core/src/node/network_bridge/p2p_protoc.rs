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
use tokio::time::{sleep, timeout};
use tracing::Instrument;

use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::contract::{ContractHandlerEvent, WaitingTransaction};
use crate::message::{NetMessageV1, QueryResult};
use crate::node::network_bridge::handshake::{
    Command as HandshakeCommand, CommandSender as HandshakeCommandSender, Event as HandshakeEvent,
    HandshakeHandler,
};
use crate::node::network_bridge::priority_select;
use crate::node::MessageProcessor;
use crate::operations::connect::ConnectMsg;
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
    node::{
        handle_aborted_op, process_message_decoupled, NetEventRegister, NodeConfig, OpManager,
        PeerId,
    },
    ring::{PeerAddr, PeerKeyLocation},
    tracing::NetEventLog,
};
use freenet_stdlib::client_api::{ContractResponse, HostResponse};

type P2pBridgeEvent = Either<(PeerKeyLocation, Box<NetMessage>), NodeEvent>;

#[derive(Clone)]
pub(crate) struct P2pBridge {
    accepted_peers: Arc<DashSet<SocketAddr>>,
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
    async fn drop_connection(&mut self, peer_addr: SocketAddr) -> super::ConnResult<()> {
        if self.accepted_peers.remove(&peer_addr).is_some() {
            self.ev_listener_tx
                .send(Right(NodeEvent::DropConnection(peer_addr)))
                .await
                .map_err(|_| ConnectionError::SendNotCompleted(peer_addr))?;
            // Log disconnect with PeerKeyLocation if we can construct one
            if let Some(peer_loc) = self
                .op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(peer_addr)
            {
                self.log_register
                    .register_events(Either::Left(NetEventLog::disconnected(
                        &self.op_manager.ring,
                        &PeerId::new(
                            peer_loc.socket_addr().expect("peer should have address"),
                            peer_loc.pub_key().clone(),
                        ),
                    )))
                    .await;
            }
        }
        Ok(())
    }

    async fn send(&self, target_addr: SocketAddr, msg: NetMessage) -> super::ConnResult<()> {
        self.log_register
            .register_events(NetEventLog::from_outbound_msg(&msg, &self.op_manager.ring))
            .await;
        // Look up the full PeerKeyLocation from connection manager for transaction tracking
        let target_loc = self
            .op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(target_addr);
        if let Some(ref target) = target_loc {
            self.op_manager.sending_transaction(target, &msg);
            self.ev_listener_tx
                .send(Left((target.clone(), Box::new(msg))))
                .await
                .map_err(|_| ConnectionError::SendNotCompleted(target_addr))?;
        } else {
            // No known peer at this address - create a temporary PeerKeyLocation
            // using our own pub_key as placeholder
            tracing::warn!(
                %target_addr,
                "Sending to unknown peer address - creating temporary PeerKeyLocation"
            );
            let temp_peer = PeerKeyLocation::new(
                (*self.op_manager.ring.connection_manager.pub_key).clone(),
                target_addr,
            );
            self.op_manager.sending_transaction(&temp_peer, &msg);
            self.ev_listener_tx
                .send(Left((temp_peer, Box::new(msg))))
                .await
                .map_err(|_| ConnectionError::SendNotCompleted(target_addr))?;
        }
        Ok(())
    }
}

type PeerConnChannelSender = Sender<Either<NetMessage, ConnEvent>>;
type PeerConnChannelRecv = Receiver<Either<NetMessage, ConnEvent>>;

/// Entry in the connections HashMap, keyed by SocketAddr.
/// The pub_key is learned from the first message received on this connection.
#[derive(Debug)]
struct ConnectionEntry {
    sender: PeerConnChannelSender,
    /// The peer's public key, learned from the first message.
    /// None for transient connections before identity is established.
    pub_key: Option<TransportPublicKey>,
}

pub(in crate::node) struct P2pConnManager {
    pub(in crate::node) gateways: Vec<PeerKeyLocation>,
    pub(in crate::node) bridge: P2pBridge,
    conn_bridge_rx: Receiver<P2pBridgeEvent>,
    event_listener: Box<dyn NetEventRegister>,
    /// Connections indexed by socket address (the transport-level identifier).
    /// This is the source of truth for active connections.
    connections: HashMap<SocketAddr, ConnectionEntry>,
    /// Reverse lookup: public key -> socket address.
    /// Used to find connections when we only know the peer's identity.
    /// Must be kept in sync with `connections`.
    addr_by_pub_key: HashMap<TransportPublicKey, SocketAddr>,
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
        // Initialize our peer identity before any connection attempts so join requests can
        // reference the correct address.
        let advertised_addr = {
            let advertised_ip = config
                .own_addr
                .map(|addr| addr.ip())
                .or(config.config.network_api.public_address)
                .unwrap_or_else(|| {
                    if listener_ip.is_unspecified() {
                        IpAddr::V4(Ipv4Addr::LOCALHOST)
                    } else {
                        listener_ip
                    }
                });
            let advertised_port = config
                .own_addr
                .map(|addr| addr.port())
                .or(config.config.network_api.public_port)
                .unwrap_or(listen_port);
            SocketAddr::new(advertised_ip, advertised_port)
        };
        bridge
            .op_manager
            .ring
            .connection_manager
            .try_set_own_addr(advertised_addr);

        Ok(P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            event_listener: Box::new(event_listener),
            connections: HashMap::new(),
            addr_by_pub_key: HashMap::new(),
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
            addr_by_pub_key,
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
            addr_by_pub_key,
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
                                peer = ?ctx.bridge.op_manager.ring.connection_manager.get_own_addr(),
                                "Received inbound message from peer - processing"
                            );
                            // Only the hop that owns the transport socket (gateway/first hop in
                            // practice) knows the UDP source address; tag the connect request here
                            // so downstream relays don't guess at the joiner's address.
                            // The joiner creates the request with PeerAddr::Unknown because it
                            // doesn't know its own external address (especially behind NAT).
                            // We fill it in from the transport layer's observed source address.
                            if let (
                                Some(remote_addr),
                                NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                                    payload,
                                    ..
                                })),
                            ) = (remote, &mut msg)
                            {
                                if payload.joiner.peer_addr.is_unknown() {
                                    payload.joiner.peer_addr = PeerAddr::Known(remote_addr);
                                }
                            }
                            // Pass the source address through to operations for routing.
                            // This replaces the old rewrite_sender_addr hack - instead of mutating
                            // message contents, we pass the observed transport address separately.
                            ctx.handle_inbound_message(msg, remote, &op_manager, &mut state)
                                .await?;
                        }
                        ConnEvent::OutboundMessage(NetMessage::V1(NetMessageV1::Aborted(tx))) => {
                            // TODO: handle aborted transaction as internal message
                            tracing::error!(%tx, "Aborted transaction");
                        }
                        ConnEvent::OutboundMessage(msg) => {
                            // With hop-by-hop routing, messages should go through OutboundMessageWithTarget.
                            // This path should not be reached - if we get here, it means a message was sent
                            // without proper target extraction from operation state.
                            tracing::error!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                "OutboundMessage received but no target - this is a bug. \
                                 With hop-by-hop routing, all outbound messages should use \
                                 OutboundMessageWithTarget. Processing locally instead."
                            );
                            // Process locally as a fallback
                            ctx.handle_inbound_message(msg, None, &op_manager, &mut state)
                                .await?;
                        }
                        ConnEvent::OutboundMessageWithTarget { target_addr, msg } => {
                            // This variant uses an explicit target address from P2pBridge::send(),
                            // which is critical for NAT scenarios where the address in the message
                            // differs from the actual transport address we should send to.
                            tracing::info!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                target_addr = %target_addr,
                                "Sending outbound message with explicit target address (hop-by-hop routing)"
                            );

                            // Look up the connection using the explicit target address
                            let peer_connection = ctx.connections.get(&target_addr);

                            match peer_connection {
                                Some(peer_connection) => {
                                    if let Err(e) =
                                        peer_connection.sender.send(Left(msg.clone())).await
                                    {
                                        tracing::error!(
                                            tx = %msg.id(),
                                            target_addr = %target_addr,
                                            "Failed to send message to peer: {}", e
                                        );
                                    } else {
                                        tracing::info!(
                                            tx = %msg.id(),
                                            target_addr = %target_addr,
                                            "Message successfully sent to peer connection via explicit address"
                                        );
                                    }
                                }
                                None => {
                                    // No existing connection - this is unexpected for hop-by-hop routing
                                    // since we should have the connection from the original request
                                    tracing::error!(
                                        tx = %msg.id(),
                                        target_addr = %target_addr,
                                        connections = ?ctx.connections.keys().collect::<Vec<_>>(),
                                        "No connection found for target address - hop-by-hop routing failed"
                                    );
                                    ctx.bridge.op_manager.completed(*msg.id());
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
                                    let peers_to_cleanup: Vec<_> = ctx
                                        .connections
                                        .iter()
                                        .map(|(addr, entry)| (*addr, entry.pub_key.clone()))
                                        .collect();
                                    for (peer_addr, pub_key_opt) in peers_to_cleanup {
                                        tracing::debug!(%peer_addr, "Cleaning up active connection due to critical channel closure");

                                        // Clean up ring state - construct PeerKeyLocation with pub_key if available
                                        let peer = if let Some(pub_key) = pub_key_opt.clone() {
                                            PeerKeyLocation::new(pub_key, peer_addr)
                                        } else {
                                            // Use our own pub_key as placeholder if we don't know the peer's
                                            PeerKeyLocation::new(
                                                (*ctx
                                                    .bridge
                                                    .op_manager
                                                    .ring
                                                    .connection_manager
                                                    .pub_key)
                                                    .clone(),
                                                peer_addr,
                                            )
                                        };
                                        ctx.bridge
                                            .op_manager
                                            .ring
                                            .prune_connection(PeerId::new(
                                                peer_addr,
                                                peer.pub_key().clone(),
                                            ))
                                            .await;

                                        // Remove from connection map
                                        tracing::debug!(self_peer = %ctx.bridge.op_manager.ring.connection_manager.pub_key, %peer_addr, conn_map_size = ctx.connections.len(), "[CONN_TRACK] REMOVE: ClosedChannel cleanup - removing from connections HashMap");
                                        ctx.connections.remove(&peer_addr);
                                        if let Some(pub_key) = pub_key_opt {
                                            ctx.addr_by_pub_key.remove(&pub_key);
                                        }

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
                            NodeEvent::DropConnection(peer_addr) => {
                                // Look up the connection entry by address
                                if let Some(entry) = ctx.connections.get(&peer_addr) {
                                    // Construct PeerKeyLocation from stored pub_key or fallback
                                    let peer = if let Some(ref pub_key) = entry.pub_key {
                                        PeerKeyLocation::new(pub_key.clone(), peer_addr)
                                    } else {
                                        PeerKeyLocation::new(
                                            (*ctx
                                                .bridge
                                                .op_manager
                                                .ring
                                                .connection_manager
                                                .pub_key)
                                                .clone(),
                                            peer_addr,
                                        )
                                    };
                                    let pub_key_to_remove = entry.pub_key.clone();

                                    tracing::debug!(self_peer = %ctx.bridge.op_manager.ring.connection_manager.pub_key, %peer, conn_map_size = ctx.connections.len(), "[CONN_TRACK] REMOVE: DropConnection event - removing from connections HashMap");
                                    if let Err(error) = handshake_cmd_sender
                                        .send(HandshakeCommand::DropConnection {
                                            peer: peer.clone(),
                                        })
                                        .await
                                    {
                                        tracing::warn!(
                                            %peer,
                                            ?error,
                                            "Failed to enqueue DropConnection command"
                                        );
                                    }
                                    // Immediately prune topology counters so we don't leak open connection slots.
                                    ctx.bridge
                                        .op_manager
                                        .ring
                                        .prune_connection(PeerId::new(
                                            peer_addr,
                                            peer.pub_key().clone(),
                                        ))
                                        .await;

                                    // Clean up proximity cache for disconnected peer
                                    ctx.bridge
                                        .op_manager
                                        .proximity_cache
                                        .on_peer_disconnected(&peer_addr);
                                    if let Some(conn) = ctx.connections.remove(&peer_addr) {
                                        // Also remove from reverse lookup
                                        if let Some(pub_key) = pub_key_to_remove {
                                            ctx.addr_by_pub_key.remove(&pub_key);
                                        }
                                        // TODO: review: this could potentially leave garbage tasks in the background with peer listener
                                        match timeout(
                                            Duration::from_secs(1),
                                            conn.sender.send(Right(ConnEvent::NodeAction(
                                                NodeEvent::DropConnection(peer_addr),
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
                                } else {
                                    tracing::debug!(%peer_addr, "DropConnection for unknown address - ignoring");
                                }
                            }
                            NodeEvent::ConnectPeer {
                                peer,
                                tx,
                                callback,
                                is_gw: transient,
                            } => {
                                tracing::info!(
                                    tx = %tx,
                                    remote = %peer,
                                    remote_addr = ?peer.socket_addr(),
                                    transient,
                                    "NodeEvent::ConnectPeer received"
                                );
                                ctx.handle_connect_peer(
                                    peer,
                                    Box::new(callback),
                                    tx,
                                    &handshake_cmd_sender,
                                    &mut state,
                                    transient,
                                )
                                .await?;
                            }
                            NodeEvent::ExpectPeerConnection { addr } => {
                                tracing::debug!(%addr, "ExpectPeerConnection event received; registering inbound expectation via handshake driver");
                                state.outbound_handler.expect_incoming(addr);
                                // We don't know the peer's public key yet for expected inbound connections,
                                // so use our own pub_key as a placeholder. The handshake will update it.
                                let peer_placeholder = PeerKeyLocation::new(
                                    (*ctx.bridge.op_manager.ring.connection_manager.pub_key)
                                        .clone(),
                                    addr,
                                );
                                if let Err(error) = handshake_cmd_sender
                                    .send(HandshakeCommand::ExpectInbound {
                                        peer: peer_placeholder,
                                        transaction: None,
                                        transient: false,
                                    })
                                    .await
                                {
                                    tracing::warn!(
                                        %addr,
                                        ?error,
                                        "Failed to enqueue ExpectInbound command; inbound connection may be dropped"
                                    );
                                }
                            }
                            NodeEvent::QueryConnections { callback } => {
                                // Reconstruct PeerKeyLocations from stored connections
                                let connections: Vec<PeerKeyLocation> = ctx
                                    .connections
                                    .iter()
                                    .filter_map(|(addr, entry)| {
                                        // Only include connections where we know the public key
                                        entry.pub_key.as_ref().map(|pub_key| {
                                            PeerKeyLocation::new(pub_key.clone(), *addr)
                                        })
                                    })
                                    .collect();
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
                                // Convert PeerKeyLocations to SocketAddrs for NetworkDebugInfo
                                let network_subs: Vec<(
                                    freenet_stdlib::prelude::ContractKey,
                                    Vec<SocketAddr>,
                                )> = network_subs
                                    .into_iter()
                                    .map(|(key, peers)| {
                                        let addrs = peers
                                            .into_iter()
                                            .filter_map(|p| p.socket_addr())
                                            .collect();
                                        (key, addrs)
                                    })
                                    .collect();

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

                                // Reconstruct PeerKeyLocations from stored connections
                                let connections: Vec<PeerKeyLocation> = ctx
                                    .connections
                                    .iter()
                                    .filter_map(|(addr, entry)| {
                                        // Only include connections where we know the public key
                                        entry.pub_key.as_ref().map(|pub_key| {
                                            PeerKeyLocation::new(pub_key.clone(), *addr)
                                        })
                                    })
                                    .collect();
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
                                    // Calculate location and address if set
                                    let (addr, location) = if let Some(own_addr) =
                                        op_manager.ring.connection_manager.get_own_addr()
                                    {
                                        let location = Location::from_address(&own_addr);
                                        (Some(own_addr), Some(location))
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
                                    let cm = &op_manager.ring.connection_manager;
                                    let connections_by_loc = cm.get_connections_by_location();
                                    let mut connected_peers = Vec::new();
                                    for conns in connections_by_loc.values() {
                                        for conn in conns {
                                            connected_peers.push((
                                                conn.location.pub_key().to_string(),
                                                conn.location
                                                    .socket_addr()
                                                    .expect("connection should have address")
                                                    .to_string(),
                                            ));
                                        }
                                    }
                                    connected_peers.sort_by(|a, b| a.0.cmp(&b.0));
                                    connected_peers.dedup_by(|a, b| a.0 == b.0);

                                    response.network_info = Some(NetworkInfo {
                                        active_connections: connected_peers.len(),
                                        connected_peers,
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
                                                            .map(|pk| pk.pub_key().to_string())
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

                                // Collect topology-backed connection info (exclude transient transports).
                                let cm = &op_manager.ring.connection_manager;
                                let connections_by_loc = cm.get_connections_by_location();
                                let mut connected_peer_ids = Vec::new();
                                if config.include_detailed_peer_info {
                                    use freenet_stdlib::client_api::ConnectedPeerInfo;
                                    for conns in connections_by_loc.values() {
                                        for conn in conns {
                                            connected_peer_ids
                                                .push(conn.location.pub_key().to_string());
                                            response.connected_peers_detailed.push(
                                                ConnectedPeerInfo {
                                                    peer_id: conn.location.pub_key().to_string(),
                                                    address: conn
                                                        .location
                                                        .socket_addr()
                                                        .expect("connection should have address")
                                                        .to_string(),
                                                },
                                            );
                                        }
                                    }
                                } else {
                                    for conns in connections_by_loc.values() {
                                        connected_peer_ids.extend(
                                            conns.iter().map(|c| c.location.pub_key().to_string()),
                                        );
                                    }
                                }
                                connected_peer_ids.sort();
                                connected_peer_ids.dedup();

                                // Collect system metrics
                                if config.include_system_metrics {
                                    let seeding_contracts =
                                        op_manager.ring.all_network_subscriptions().len() as u32;
                                    response.system_metrics = Some(SystemMetrics {
                                        active_connections: connected_peer_ids.len() as u32,
                                        seeding_contracts,
                                    });
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

                                // If this is a child operation, complete it and let the parent flow handle result delivery.
                                if op_manager.is_sub_operation(tx) {
                                    tracing::info!(%tx, %key, "completing child subscribe operation");
                                    op_manager.completed(tx);
                                    continue;
                                }

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
                            NodeEvent::BroadcastProximityCache { message } => {
                                // Broadcast ProximityCache message to all connected peers
                                tracing::debug!(
                                    ?message,
                                    peer_count = ctx.connections.len(),
                                    "Broadcasting ProximityCache message to connected peers"
                                );

                                let msg = crate::message::NetMessage::V1(
                                    crate::message::NetMessageV1::ProximityCache {
                                        message: message.clone(),
                                    },
                                );

                                for peer_addr in ctx.connections.keys() {
                                    tracing::debug!(%peer_addr, "Sending ProximityCache to peer");
                                    if let Err(e) = ctx.bridge.send(*peer_addr, msg.clone()).await {
                                        tracing::warn!(%peer_addr, "Failed to send ProximityCache: {e}");
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
        source_addr: Option<SocketAddr>,
        op_manager: &Arc<OpManager>,
        state: &mut EventListenerState,
    ) -> anyhow::Result<()> {
        let tx = *msg.id();
        tracing::debug!(
            %tx,
            tx_type = ?tx.transaction_type(),
            ?source_addr,
            "Handling inbound NetMessage at event loop"
        );
        match msg {
            NetMessage::V1(NetMessageV1::Aborted(tx)) => {
                handle_aborted_op(tx, op_manager, &self.gateways).await?;
            }
            msg => {
                self.process_message(msg, source_addr, op_manager, None, state)
                    .await;
            }
        }
        Ok(())
    }

    async fn process_message(
        &self,
        msg: NetMessage,
        source_addr: Option<SocketAddr>,
        op_manager: &Arc<OpManager>,
        executor_callback_opt: Option<ExecutorToEventLoopChannel<crate::contract::Callback>>,
        state: &mut EventListenerState,
    ) {
        tracing::info!(
            tx = %msg.id(),
            tx_type = ?msg.id().transaction_type(),
            msg_type = %msg,
            ?source_addr,
            peer = ?op_manager.ring.connection_manager.get_own_addr(),
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
                source_addr,
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

    /// Looks up a connection by public key using the reverse lookup map.
    /// Returns the socket address and connection entry if found.
    fn connection_entry_by_pub_key(
        &self,
        pub_key: &TransportPublicKey,
    ) -> Option<(SocketAddr, &ConnectionEntry)> {
        self.addr_by_pub_key
            .get(pub_key)
            .and_then(|addr| self.connections.get(addr).map(|entry| (*addr, entry)))
    }

    async fn handle_connect_peer(
        &mut self,
        peer: PeerKeyLocation,
        mut callback: Box<dyn ConnectResultSender>,
        tx: Transaction,
        handshake_commands: &HandshakeCommandSender,
        state: &mut EventListenerState,
        transient: bool,
    ) -> anyhow::Result<()> {
        let mut peer = peer;
        let mut peer_addr = peer
            .socket_addr()
            .unwrap_or_else(|| SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0));

        if peer_addr.ip().is_unspecified() {
            if let Some((existing_addr, _)) = self.connection_entry_by_pub_key(peer.pub_key()) {
                peer_addr = existing_addr;
                peer = PeerKeyLocation::new(peer.pub_key().clone(), existing_addr);
                tracing::info!(
                    tx = %tx,
                    remote = %peer,
                    fallback_addr = %peer_addr,
                    transient,
                    "ConnectPeer provided unspecified address; using existing connection address"
                );
            } else {
                tracing::debug!(
                    tx = %tx,
                    transient,
                    "ConnectPeer received unspecified address without existing connection reference"
                );
            }
        }

        tracing::info!(
            tx = %tx,
            remote = %peer,
            remote_addr = %peer_addr,
            transient,
            "Connecting to peer"
        );
        if let Some(blocked_addrs) = &self.blocked_addresses {
            if blocked_addrs.contains(&peer_addr) {
                tracing::info!(
                    tx = %tx,
                    remote = %peer_addr,
                    "Outgoing connection to peer blocked by local policy"
                );
                callback
                    .send_result(Err(()))
                    .await
                    .inspect_err(|error| {
                        tracing::debug!(
                            remote = %peer_addr,
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
                peer_addr
            );
        }

        // If a transient transport already exists, promote it without dialing anew.
        if self.connections.contains_key(&peer_addr) {
            tracing::info!(
                tx = %tx,
                remote = %peer,
                transient,
                "connect_peer: reusing existing transport / promoting transient if present"
            );
            let connection_manager = &self.bridge.op_manager.ring.connection_manager;
            if let Some(entry) = connection_manager.drop_transient(peer_addr) {
                let loc = entry
                    .location
                    .unwrap_or_else(|| Location::from_address(&peer_addr));
                // Re-run admission + cap guard when promoting a transient connection.
                let should_accept = connection_manager.should_accept(loc, peer_addr);
                if !should_accept {
                    tracing::warn!(
                        tx = %tx,
                        %peer,
                        %loc,
                        "connect_peer: promotion rejected by admission logic"
                    );
                    callback
                        .send_result(Err(()))
                        .await
                        .inspect_err(|err| {
                            tracing::debug!(
                                tx = %tx,
                                remote = %peer,
                                ?err,
                                "connect_peer: failed to notify rejected-promotion callback"
                            );
                        })
                        .ok();
                    return Ok(());
                }
                let current = connection_manager.connection_count();
                if current >= connection_manager.max_connections {
                    tracing::warn!(
                        tx = %tx,
                        %peer,
                        current_connections = current,
                        max_connections = connection_manager.max_connections,
                        %loc,
                        "connect_peer: rejecting transient promotion to enforce cap"
                    );
                    callback
                        .send_result(Err(()))
                        .await
                        .inspect_err(|err| {
                            tracing::debug!(
                                tx = %tx,
                                remote = %peer,
                                ?err,
                                "connect_peer: failed to notify cap-rejection callback"
                            );
                        })
                        .ok();
                    return Ok(());
                }
                self.bridge
                    .op_manager
                    .ring
                    .add_connection(loc, PeerId::new(peer_addr, peer.pub_key().clone()), true)
                    .await;
                tracing::info!(tx = %tx, remote = %peer, "connect_peer: promoted transient");
            }

            // Now that we know the peer's identity (from ConnectRequest), update the
            // transport-level tracking so QueryConnections returns this peer.
            if let Some(entry) = self.connections.get_mut(&peer_addr) {
                if entry.pub_key.is_none() {
                    entry.pub_key = Some(peer.pub_key().clone());
                    self.addr_by_pub_key
                        .insert(peer.pub_key().clone(), peer_addr);
                    tracing::debug!(
                        tx = %tx,
                        %peer_addr,
                        pub_key = %peer.pub_key(),
                        "connect_peer: updated transport entry with peer identity"
                    );
                }
            }

            // Return the remote peer's address we are connected to.
            let resolved_addr = peer
                .socket_addr()
                .expect("connected peer should have socket address");
            callback
                .send_result(Ok((resolved_addr, None)))
                .await
                .inspect_err(|err| {
                    tracing::debug!(
                        tx = %tx,
                        remote = %peer,
                        ?err,
                        "connect_peer: failed to notify existing-connection callback"
                    );
                })
                .ok();
            return Ok(());
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
                    transient,
                    "Connection already pending, queuing additional requester"
                );
                callbacks.get_mut().push(callback);
                tracing::info!(
                    tx = %tx,
                    remote = %peer_addr,
                    pending = callbacks.get().len(),
                    pending_txs = ?txs_entry,
                    transient,
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
                    transient,
                    "connect_peer: registering new pending connection"
                );
                entry.insert(vec![callback]);
                tracing::info!(
                tx = %tx,
                remote = %peer_addr,
                pending = 1,
                    pending_txs = ?txs_entry,
                    transient,
                    "connect_peer: registered new pending connection"
                );
                state.outbound_handler.expect_incoming(peer_addr);
            }
        }

        if let Err(error) = handshake_commands
            .send(HandshakeCommand::Connect {
                peer: peer.clone(),
                transaction: tx,
                transient,
            })
            .await
        {
            tracing::warn!(
                tx = %tx,
                remote = %peer_addr,
                transient,
                ?error,
                "Failed to enqueue connect command"
            );
            self.bridge
                .op_manager
                .ring
                .connection_manager
                .prune_in_transit_connection(peer_addr);
            let pending_txs = state.awaiting_connection_txs.remove(&peer_addr);
            if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
                tracing::debug!(
                    tx = %tx,
                    remote = %peer_addr,
                    callbacks = callbacks.len(),
                    transient,
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
                transient,
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
                transient,
            } => {
                tracing::info!(provided = ?peer, transient, tx = ?transaction, "InboundConnection event");
                let _conn_manager = &self.bridge.op_manager.ring.connection_manager;
                let remote_addr = connection.remote_addr();

                if let Some(blocked_addrs) = &self.blocked_addresses {
                    if blocked_addrs.contains(&remote_addr) {
                        tracing::info!(
                            remote = %remote_addr,
                            transient,
                            transaction = ?transaction,
                            "Inbound connection blocked by local policy"
                        );
                        return Ok(());
                    }
                }

                // For transient inbound connections, peer may be None - we don't know the
                // peer's identity yet. The identity will be learned when the peer sends
                // its first message (e.g., ConnectRequest).
                if peer.is_none() {
                    tracing::info!(
                        remote = %remote_addr,
                        transient,
                        transaction = ?transaction,
                        "Inbound connection arrived without matching expectation; accepting provisionally"
                    );
                }

                tracing::info!(
                    remote = %remote_addr,
                    peer_known = peer.is_some(),
                    transient,
                    transaction = ?transaction,
                    "Inbound connection established"
                );

                // Treat only transient connections as transient. Normal inbound dials (including
                // gateway bootstrap from peers) should be promoted into the ring once established.
                let is_transient = transient;

                // Pass peer directly - it may be None for transient connections
                self.handle_successful_connection(peer, connection, state, None, is_transient)
                    .await?;
            }
            HandshakeEvent::OutboundEstablished {
                transaction,
                peer,
                connection,
                transient,
            } => {
                tracing::info!(
                    remote = %peer.socket_addr().expect("peer should have address"),
                    transient,
                    transaction = %transaction,
                    "Outbound connection established"
                );
                // For outbound connections, we always know who we're connecting to
                self.handle_successful_connection(Some(peer), connection, state, None, false)
                    .await?;
            }
            HandshakeEvent::OutboundFailed {
                transaction,
                peer,
                error,
                transient,
            } => {
                tracing::info!(
                    remote = %peer.socket_addr().expect("peer should have address"),
                    transient,
                    transaction = %transaction,
                    ?error,
                    "Outbound connection failed"
                );

                self.bridge
                    .op_manager
                    .ring
                    .connection_manager
                    .prune_in_transit_connection(
                        peer.socket_addr().expect("peer should have address"),
                    );

                let pending_txs = state
                    .awaiting_connection_txs
                    .remove(&peer.socket_addr().expect("peer should have address"))
                    .unwrap_or_default();

                if let Some(callbacks) = state
                    .awaiting_connection
                    .remove(&peer.socket_addr().expect("peer should have address"))
                {
                    tracing::debug!(
                        remote = %peer.socket_addr().expect("peer should have address"),
                        callbacks = callbacks.len(),
                        pending_txs = ?pending_txs,
                        transient,
                        "Notifying callbacks after outbound failure"
                    );

                    let mut callbacks = callbacks.into_iter();
                    if let Some(mut cb) = callbacks.next() {
                        cb.send_result(Err(()))
                            .await
                            .inspect_err(|err| {
                                tracing::debug!(
                                    remote = %peer.socket_addr().expect("peer should have address"),
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
                                    remote = %peer.socket_addr().expect("peer should have address"),
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
        peer_id: Option<PeerKeyLocation>,
        connection: PeerConnection,
        state: &mut EventListenerState,
        remaining_checks: Option<usize>,
        is_transient: bool,
    ) -> anyhow::Result<()> {
        let connection_manager = &self.bridge.op_manager.ring.connection_manager;
        // For transient connections, we may not know the peer's identity yet - use connection's remote_addr
        let peer_addr = peer_id
            .as_ref()
            .and_then(|p| p.socket_addr())
            .unwrap_or_else(|| connection.remote_addr());

        if is_transient && !connection_manager.try_register_transient(peer_addr, None) {
            tracing::warn!(
                remote = %peer_addr,
                budget = connection_manager.transient_budget(),
                current = connection_manager.transient_count(),
                "Transient connection budget exhausted; dropping inbound connection"
            );
            if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
                for mut cb in callbacks {
                    let _ = cb.send_result(Err(())).await;
                }
            }
            state.awaiting_connection_txs.remove(&peer_addr);
            return Ok(());
        }
        let pending_txs = state
            .awaiting_connection_txs
            .remove(&peer_addr)
            .unwrap_or_default();
        if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
            // The callback expects the remote peer's address (not our own)
            let resolved_addr = peer_addr;
            tracing::debug!(
                remote = %peer_addr,
                callbacks = callbacks.len(),
                "handle_successful_connection: notifying waiting callbacks"
            );
            tracing::info!(
                remote = %peer_addr,
                callbacks = callbacks.len(),
                pending_txs = ?pending_txs,
                remaining_checks = ?remaining_checks,
                "handle_successful_connection: connection established"
            );
            for mut cb in callbacks {
                match timeout(
                    Duration::from_secs(60),
                    cb.send_result(Ok((resolved_addr, remaining_checks))),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(())) => {
                        tracing::debug!(
                            remote = %peer_addr,
                            "Callback dropped before receiving connection result"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            remote = %peer_addr,
                            ?error,
                            "Failed to deliver connection result"
                        );
                    }
                }
            }
        } else {
            tracing::warn!(
                peer_id = ?peer_id,
                %peer_addr,
                pending_txs = ?pending_txs,
                "No callback for connection established"
            );
        }

        // Only insert if connection doesn't already exist to avoid dropping existing channel
        let mut newly_inserted = false;
        if !self.connections.contains_key(&peer_addr) {
            if is_transient {
                let cm = &self.bridge.op_manager.ring.connection_manager;
                let current = cm.transient_count();
                if current >= cm.transient_budget() {
                    tracing::warn!(
                        remote = %peer_addr,
                        budget = cm.transient_budget(),
                        current,
                        "Transient connection budget exhausted; dropping inbound connection before insert"
                    );
                    return Ok(());
                }
            }
            let (tx, rx) = mpsc::channel(10);
            tracing::debug!(
                self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key,
                peer_id = ?peer_id,
                %peer_addr,
                conn_map_size = self.connections.len(),
                "[CONN_TRACK] INSERT: adding connection to HashMap"
            );
            self.connections.insert(
                peer_addr,
                ConnectionEntry {
                    sender: tx,
                    // For transient connections, we don't know the pub_key yet - it will be learned
                    // when the peer sends its first message (e.g., ConnectRequest)
                    pub_key: peer_id.as_ref().map(|p| p.pub_key().clone()),
                },
            );
            // Only add to reverse lookup if we know the pub_key
            // For transient connections, this will be populated when identity is learned
            if let Some(ref peer) = peer_id {
                self.addr_by_pub_key
                    .insert(peer.pub_key().clone(), peer_addr);
            }
            let Some(conn_events) = self.conn_event_tx.as_ref().cloned() else {
                anyhow::bail!("Connection event channel not initialized");
            };
            tokio::spawn(async move {
                peer_connection_listener(rx, connection, peer_addr, conn_events).await;
            });
            // Yield to allow the spawned peer_connection_listener task to start.
            // This is important because on some runtimes (especially in tests with boxed_local
            // futures), spawned tasks may not be scheduled immediately, causing messages
            // sent to the channel to pile up without being processed.
            tokio::task::yield_now().await;
            newly_inserted = true;
        } else {
            tracing::debug!(
                self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key,
                peer_id = ?peer_id,
                %peer_addr,
                conn_map_size = self.connections.len(),
                "[CONN_TRACK] SKIP INSERT: connection already exists in HashMap"
            );
        }

        // Only promote to ring if we know the peer's identity.
        // For transient connections without known identity, we keep them as transport-only
        // until the peer identifies itself (e.g., via ConnectRequest).
        let promote_to_ring =
            peer_id.is_some() && (!is_transient || connection_manager.is_gateway());

        if newly_inserted {
            tracing::info!(peer_id = ?peer_id, %peer_addr, is_transient, "handle_successful_connection: inserted new connection entry");
            let pending_loc = connection_manager.prune_in_transit_connection(peer_addr);
            if promote_to_ring {
                // Safe to unwrap: promote_to_ring is only true when peer_id.is_some()
                let peer = peer_id
                    .as_ref()
                    .expect("promote_to_ring requires known peer_id");
                let loc = pending_loc.unwrap_or_else(|| Location::from_address(&peer_addr));
                tracing::info!(
                    %peer_addr,
                    %loc,
                    pending_loc_known = pending_loc.is_some(),
                    "handle_successful_connection: evaluating promotion to ring"
                );
                // Re-apply admission logic on promotion to avoid bypassing capacity/heuristic checks.
                let should_accept = connection_manager.should_accept(loc, peer_addr);
                if !should_accept {
                    tracing::warn!(
                        %peer_addr,
                        %loc,
                        "handle_successful_connection: promotion rejected by admission logic"
                    );
                    return Ok(());
                }
                let current = connection_manager.connection_count();
                if current >= connection_manager.max_connections {
                    tracing::warn!(
                        %peer_addr,
                        current_connections = current,
                        max_connections = connection_manager.max_connections,
                        %loc,
                        "handle_successful_connection: rejecting new connection to enforce cap"
                    );
                    return Ok(());
                }
                tracing::info!(%peer_addr, %loc, "handle_successful_connection: promoting connection into ring");
                self.bridge
                    .op_manager
                    .ring
                    .add_connection(loc, PeerId::new(peer_addr, peer.pub_key().clone()), true)
                    .await;
                if is_transient {
                    connection_manager.drop_transient(peer_addr);
                }
            } else {
                // Not promoting to ring - either unknown identity or transient on non-gateway.
                // Keep the connection as transient; budget was reserved before any work.
                let loc = pending_loc.unwrap_or_else(|| Location::from_address(&peer_addr));
                connection_manager.try_register_transient(peer_addr, Some(loc));
                tracing::info!(
                    peer_id = ?peer_id,
                    %peer_addr,
                    pending_loc_known = pending_loc.is_some(),
                    "Registered transient connection (not added to ring topology)"
                );
                let ttl = connection_manager.transient_ttl();
                let drop_tx = self.bridge.ev_listener_tx.clone();
                let cm = connection_manager.clone();
                tokio::spawn(async move {
                    sleep(ttl).await;
                    if cm.drop_transient(peer_addr).is_some() {
                        tracing::info!(%peer_addr, "Transient connection expired; dropping");
                        if let Err(err) = drop_tx
                            .send(Right(NodeEvent::DropConnection(peer_addr)))
                            .await
                        {
                            tracing::warn!(
                                %peer_addr,
                                ?err,
                                "Failed to dispatch DropConnection for expired transient"
                            );
                        }
                    }
                });
            }
        } else if is_transient {
            // We reserved budget earlier, but didn't take ownership of the connection.
            connection_manager.drop_transient(peer_addr);
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
                        if sender_peer
                            .socket_addr()
                            .map(|a| a == remote_addr)
                            .unwrap_or(false)
                            || sender_peer
                                .socket_addr()
                                .map(|a| a.ip().is_unspecified())
                                .unwrap_or(true)
                        {
                            let mut new_peer_id = PeerId::new(
                                sender_peer.socket_addr().unwrap_or(remote_addr),
                                sender_peer.pub_key().clone(),
                            );
                            if new_peer_id.addr.ip().is_unspecified() {
                                new_peer_id.addr = remote_addr;
                                if let Some(sender_mut) =
                                    extract_sender_from_message_mut(&mut inbound.msg)
                                {
                                    if sender_mut
                                        .socket_addr()
                                        .map(|a| a.ip().is_unspecified())
                                        .unwrap_or(true)
                                    {
                                        sender_mut.set_addr(remote_addr);
                                    }
                                }
                            }
                            // Check if we have a connection but with a different pub_key
                            if let Some(entry) = self.connections.get(&remote_addr) {
                                // If we don't have the pub_key stored yet or it differs from the new one, update it
                                let should_update = match &entry.pub_key {
                                    None => true,
                                    Some(old_pub_key) => old_pub_key != &new_peer_id.pub_key,
                                };
                                if should_update {
                                    let old_pub_key = entry.pub_key.clone();
                                    let is_first_identity = old_pub_key.is_none();
                                    tracing::info!(
                                        remote = %remote_addr,
                                        old_pub_key = ?old_pub_key,
                                        new_pub_key = %&new_peer_id.pub_key,
                                        is_first_identity,
                                        "Updating peer identity after inbound message"
                                    );
                                    // Remove old reverse lookup if it exists
                                    if let Some(old_key) = old_pub_key {
                                        self.addr_by_pub_key.remove(&old_key);
                                        // Update ring with old PeerId -> new PeerId
                                        let old_peer_id = PeerId::new(remote_addr, old_key);
                                        self.bridge.op_manager.ring.update_connection_identity(
                                            &old_peer_id,
                                            new_peer_id.clone(),
                                        );
                                    }
                                    // Update the entry's pub_key
                                    if let Some(entry) = self.connections.get_mut(&remote_addr) {
                                        entry.pub_key = Some(new_peer_id.pub_key.clone());
                                    }
                                    // Add new reverse lookup
                                    self.addr_by_pub_key
                                        .insert(new_peer_id.pub_key.clone().clone(), remote_addr);
                                    // Note: We do NOT automatically promote to ring here.
                                    // Transient connections are promoted only when the Connect
                                    // operation explicitly accepts via NodeEvent::ConnectPeer,
                                    // which is handled by handle_connect_peer().
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
                // Look up the connection directly by address
                if let Some(entry) = self.connections.remove(&remote_addr) {
                    // Construct PeerKeyLocation for prune_connection and DropConnection
                    let peer = if let Some(ref pub_key) = entry.pub_key {
                        PeerKeyLocation::new(pub_key.clone(), remote_addr)
                    } else {
                        PeerKeyLocation::new(
                            (*self.bridge.op_manager.ring.connection_manager.pub_key).clone(),
                            remote_addr,
                        )
                    };
                    // Remove from reverse lookup
                    if let Some(pub_key) = entry.pub_key {
                        self.addr_by_pub_key.remove(&pub_key);
                    }
                    tracing::debug!(self_peer = %self.bridge.op_manager.ring.connection_manager.pub_key, %peer, socket_addr = %remote_addr, conn_map_size = self.connections.len(), "[CONN_TRACK] REMOVE: TransportClosed - removing from connections HashMap");
                    self.bridge
                        .op_manager
                        .ring
                        .prune_connection(PeerId::new(remote_addr, peer.pub_key().clone()))
                        .await;
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
                // With hop-by-hop routing, messages no longer embed target.
                // For initial requests (GET, PUT, Subscribe), extract target from operation state.
                // For other messages, process locally (they'll be routed through network_bridge.send()).
                let tx = *msg.id();

                // Try to get target from operation state for initial outbound requests
                if let Ok(Some(op)) = self.bridge.op_manager.pop(&tx) {
                    if let Some(target_addr) = op.get_target_addr() {
                        // Put the operation back since we only peeked at it
                        let _ = futures::executor::block_on(self.bridge.op_manager.push(tx, op));

                        tracing::info!(
                            tx = %msg.id(),
                            msg_type = %msg,
                            target_addr = %target_addr,
                            "handle_notification_msg: Found target in operation state, routing as OutboundMessageWithTarget"
                        );
                        return EventResult::Event(
                            ConnEvent::OutboundMessageWithTarget { target_addr, msg }.into(),
                        );
                    } else {
                        // Put the operation back
                        let _ = futures::executor::block_on(self.bridge.op_manager.push(tx, op));
                    }
                }

                // Message has no target or couldn't get from op state - process locally
                tracing::debug!(
                    tx = %msg.id(),
                    msg_type = %msg,
                    "handle_notification_msg: No target found, processing locally"
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
            Some(Left((target, msg))) => {
                // Use OutboundMessageWithTarget to preserve the target address from
                // P2pBridge::send(). This is critical for NAT scenarios where
                // the address in the message differs from the actual transport address.
                // The target.socket_addr() contains the address that was used to look up
                // the peer in P2pBridge::send(), which is the correct transport address.
                if let Some(target_addr) = target.socket_addr() {
                    EventResult::Event(
                        ConnEvent::OutboundMessageWithTarget {
                            target_addr,
                            msg: *msg,
                        }
                        .into(),
                    )
                } else {
                    // Fall back to OutboundMessage if no explicit address
                    // (shouldn't happen in normal operation)
                    tracing::warn!(
                        tx = %msg.id(),
                        target_pub_key = %target.pub_key(),
                        "handle_bridge_msg: target has no socket address, falling back to msg.target()"
                    );
                    EventResult::Event(ConnEvent::OutboundMessage(*msg).into())
                }
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
        result: Result<(SocketAddr, Option<usize>), ()>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>>;
}

impl ConnectResultSender for mpsc::Sender<Result<(SocketAddr, Option<usize>), ()>> {
    fn send_result(
        &mut self,
        result: Result<(SocketAddr, Option<usize>), ()>,
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
    /// Outbound message with explicit target address from P2pBridge::send().
    /// Used when the target address differs from what's in the message (NAT scenarios).
    /// The target_addr is the actual transport address to send to.
    OutboundMessageWithTarget {
        target_addr: SocketAddr,
        msg: NetMessage,
    },
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

/// Drains and sends all pending outbound messages before shutting down.
///
/// This is critical for preventing message loss: when we detect an error condition
/// (transport error, channel closed, etc.), there may be messages that were queued
/// to the channel AFTER we started waiting in select! but BEFORE we detected the error.
/// Without this drain, those messages would be silently lost.
async fn drain_pending_before_shutdown(
    rx: &mut PeerConnChannelRecv,
    conn: &mut PeerConnection,
    remote_addr: SocketAddr,
) -> usize {
    let mut drained = 0;
    while let Ok(msg) = rx.try_recv() {
        // Best-effort send - connection may already be degraded
        if let Err(e) = handle_peer_channel_message(conn, msg).await {
            tracing::debug!(
                to = %remote_addr,
                ?e,
                drained,
                "[CONN_LIFECYCLE] Error during shutdown drain (expected if connection closed)"
            );
            break;
        }
        drained += 1;
    }
    if drained > 0 {
        tracing::info!(
            to = %remote_addr,
            drained,
            "[CONN_LIFECYCLE] Drained pending messages before shutdown"
        );
    }
    drained
}

/// Listens for messages on a peer connection using drain-then-select pattern.
///
/// On each iteration, drains all pending outbound messages via `try_recv()` before
/// waiting for either new outbound or inbound messages. This approach:
/// 1. Ensures queued outbound messages are sent promptly
/// 2. Avoids starving inbound (which would happen with biased select)
/// 3. No messages are lost due to poll ordering
///
/// IMPORTANT: Before exiting on any error path, we drain pending messages to prevent
/// loss of messages that arrived during the select! wait.
async fn peer_connection_listener(
    mut rx: PeerConnChannelRecv,
    mut conn: PeerConnection,
    peer_addr: SocketAddr,
    conn_events: Sender<ConnEvent>,
) {
    let remote_addr = conn.remote_addr();
    tracing::debug!(
        to = %remote_addr,
        %peer_addr,
        "[CONN_LIFECYCLE] Starting peer_connection_listener task"
    );

    loop {
        // Drain all pending outbound messages first before checking inbound.
        // This ensures queued outbound messages are sent promptly without starving
        // the inbound channel (which would happen with biased select).
        loop {
            match rx.try_recv() {
                Ok(msg) => {
                    if let Err(error) = handle_peer_channel_message(&mut conn, msg).await {
                        tracing::debug!(
                            to = %remote_addr,
                            ?error,
                            "[CONN_LIFECYCLE] Connection closed after channel command"
                        );
                        // Drain any messages that arrived after our try_recv() but before
                        // handle_peer_channel_message returned an error
                        drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                        notify_transport_closed(&conn_events, remote_addr, error).await;
                        return;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    tracing::warn!(
                        to = %remote_addr,
                        "[CONN_LIFECYCLE] peer_connection_listener channel closed without explicit DropConnection"
                    );
                    // Channel disconnected means no more messages can arrive
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

        // Now wait for either new outbound or inbound messages fairly
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(msg) => {
                        if let Err(error) = handle_peer_channel_message(&mut conn, msg).await {
                            tracing::debug!(
                                to = %remote_addr,
                                ?error,
                                "[CONN_LIFECYCLE] Connection closed after channel command"
                            );
                            // Drain any messages that arrived while we were processing this one
                            drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                            notify_transport_closed(&conn_events, remote_addr, error).await;
                            return;
                        }
                    }
                    None => {
                        tracing::warn!(
                            to = %remote_addr,
                            "[CONN_LIFECYCLE] peer_connection_listener channel closed without explicit DropConnection"
                        );
                        // Channel closed means no more messages can arrive
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

            msg = conn.recv() => {
                match msg {
                    Ok(msg) => match decode_msg(&msg) {
                        Ok(net_message) => {
                            let tx = *net_message.id();
                            tracing::debug!(
                                from = %remote_addr,
                                %tx,
                                tx_type = ?tx.transaction_type(),
                                msg_type = %net_message,
                                "[CONN_LIFECYCLE] Received inbound NetMessage from peer"
                            );
                            if conn_events
                                .send(ConnEvent::InboundMessage(IncomingMessage::with_remote(
                                    net_message,
                                    remote_addr,
                                )))
                                .await
                                .is_err()
                            {
                                tracing::debug!(
                                    from = %remote_addr,
                                    "[CONN_LIFECYCLE] conn_events receiver dropped; stopping listener"
                                );
                                // Drain pending messages - they may still be sendable even if
                                // the conn_events channel is closed
                                drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                                return;
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                from = %remote_addr,
                                ?error,
                                "[CONN_LIFECYCLE] Failed to deserialize inbound message; closing connection"
                            );
                            // Drain pending outbound messages before closing - they may still succeed
                            drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                            let transport_error = TransportError::Other(anyhow!(
                                "Failed to deserialize inbound message from {remote_addr}: {error:?}"
                            ));
                            notify_transport_closed(&conn_events, remote_addr, transport_error).await;
                            return;
                        }
                    },
                    Err(error) => {
                        tracing::debug!(
                            from = %remote_addr,
                            ?error,
                            "[CONN_LIFECYCLE] peer_connection_listener terminating after recv error"
                        );
                        // CRITICAL: Drain pending outbound messages before exiting.
                        // Messages may have been queued to the channel while we were
                        // waiting in select!, and they would be lost without this drain.
                        drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
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

/// Extract sender information from various message types.
/// Note: Most message types use connection-based routing (sender determined from socket),
/// so this only returns info for ObservedAddress which has a target field.
fn extract_sender_from_message(msg: &NetMessage) -> Option<PeerKeyLocation> {
    match msg {
        NetMessage::V1(msg_v1) => match msg_v1 {
            // All operations now use hop-by-hop routing via upstream_addr in operation state.
            // No sender/target is embedded in messages - routing is determined by transport layer.
            NetMessageV1::Connect(_)
            | NetMessageV1::Get(_)
            | NetMessageV1::Put(_)
            | NetMessageV1::Update(_)
            | NetMessageV1::Subscribe(_) => None,
            // Other message types don't have sender info
            _ => None,
        },
    }
}

fn extract_sender_from_message_mut(msg: &mut NetMessage) -> Option<&mut PeerKeyLocation> {
    match msg {
        NetMessage::V1(msg_v1) => match msg_v1 {
            // All operations now use hop-by-hop routing via upstream_addr in operation state.
            // No sender/target is embedded in messages - routing is determined by transport layer.
            NetMessageV1::Connect(_)
            | NetMessageV1::Get(_)
            | NetMessageV1::Put(_)
            | NetMessageV1::Update(_)
            | NetMessageV1::Subscribe(_) => None,
            _ => None,
        },
    }
}

// TODO: add testing for the network loop, now it should be possible to do since we don't depend upon having real connections

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, timeout, Duration};

    /// Regression test for message loss during shutdown.
    ///
    /// This test validates the drain-before-shutdown pattern that prevents message loss
    /// when an error occurs during select! wait. The bug scenario:
    ///
    /// 1. Listener enters select! waiting for inbound/outbound
    /// 2. Message is queued to the channel while select! is waiting
    /// 3. Error occurs (e.g., connection timeout, deserialize failure)
    /// 4. select! returns with the error
    /// 5. BUG: Without drain, we return immediately and the queued message is lost
    /// 6. FIX: Drain pending messages before returning
    ///
    /// This test simulates the pattern by:
    /// - Having a "listener" that waits on select! then encounters an error
    /// - Sending messages during the wait period
    /// - Verifying all messages are processed (drained) before exit
    #[tokio::test]
    async fn test_drain_before_shutdown_prevents_message_loss() {
        let (tx, mut rx) = mpsc::channel::<String>(10);
        let processed = Arc::new(AtomicUsize::new(0));
        let processed_clone = processed.clone();

        // Simulate peer_connection_listener pattern
        let listener = tokio::spawn(async move {
            // Drain-then-select loop (simplified)
            loop {
                // Phase 1: Drain pending messages (like the loop at start of peer_connection_listener)
                loop {
                    match rx.try_recv() {
                        Ok(msg) => {
                            tracing::debug!("Drained message: {}", msg);
                            processed_clone.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    }
                }

                // Phase 2: Wait for new messages or "error" (simulated by timeout)
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(m) => {
                                tracing::debug!("Received via select: {}", m);
                                processed_clone.fetch_add(1, Ordering::SeqCst);
                            }
                            None => return, // Channel closed
                        }
                    }
                    // Simulate error condition (e.g., connection timeout)
                    _ = sleep(Duration::from_millis(50)) => {
                        tracing::debug!("Simulated error occurred");

                        // CRITICAL: This is the fix - drain before shutdown
                        // Without this, messages queued during the 50ms wait would be lost
                        let mut drained = 0;
                        while let Ok(msg) = rx.try_recv() {
                            tracing::debug!("Drain before shutdown: {}", msg);
                            processed_clone.fetch_add(1, Ordering::SeqCst);
                            drained += 1;
                        }
                        tracing::debug!("Drained {} messages before shutdown", drained);
                        return;
                    }
                }
            }
        });

        // Send messages with timing that causes them to arrive DURING the select! wait
        // This is the race condition that causes message loss without the drain
        tokio::spawn(async move {
            // Wait for listener to enter select!
            sleep(Duration::from_millis(10)).await;

            // Send messages while listener is in select! waiting
            for i in 0..5 {
                tx.send(format!("msg{}", i)).await.unwrap();
                sleep(Duration::from_millis(5)).await;
            }
            // Don't close the channel - let the timeout trigger the "error"
        });

        // Wait for listener to complete
        let result = timeout(Duration::from_millis(200), listener).await;
        assert!(result.is_ok(), "Listener should complete");

        // All 5 messages should have been processed
        let count = processed.load(Ordering::SeqCst);
        assert_eq!(
            count, 5,
            "All messages should be processed (drained before shutdown). Got {} instead of 5. \
             If this fails, messages are being lost during the error-exit drain.",
            count
        );
    }

    /// Test that demonstrates what happens WITHOUT the drain (the bug we're preventing).
    /// This test intentionally shows the failure mode when drain is missing.
    ///
    /// The key is to ensure messages are queued AFTER select! starts but the error
    /// fires immediately, so select! returns with error before processing the messages.
    #[tokio::test]
    async fn test_message_loss_without_drain() {
        let (tx, mut rx) = mpsc::channel::<String>(10);
        let processed = Arc::new(AtomicUsize::new(0));
        let processed_clone = processed.clone();

        // Signal when listener is ready for messages
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

        // Buggy listener - NO drain before shutdown (demonstrates the bug)
        let listener = tokio::spawn(async move {
            // Drain at loop start (the original PR #2255 fix)
            loop {
                match rx.try_recv() {
                    Ok(msg) => {
                        tracing::debug!("Drained message: {}", msg);
                        processed_clone.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return,
                }
            }

            // Signal that we're about to enter select! and then immediately timeout
            let _ = ready_tx.send(());

            // Immediate timeout to simulate error - messages sent after this starts are lost
            tokio::select! {
                biased; // Ensure timeout wins if both are ready
                _ = sleep(Duration::from_millis(1)) => {
                    tracing::debug!("Error occurred - BUG: returning without drain!");
                    // BUG: No drain here! Messages queued during this 1ms window are lost.
                }
                msg = rx.recv() => {
                    if let Some(m) = msg {
                        tracing::debug!("Received via select: {}", m);
                        processed_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        // Wait for listener to be ready, then immediately send messages
        let _ = ready_rx.await;

        // Send messages - these arrive while select! is running with a very short timeout
        // Some or all will be lost because we return without draining
        for i in 0..5 {
            let _ = tx.send(format!("msg{}", i)).await;
        }

        let _ = timeout(Duration::from_millis(100), listener).await;

        // Without the drain fix, messages are lost
        let count = processed.load(Ordering::SeqCst);
        // The listener exits immediately due to timeout, so no messages should be processed
        assert!(
            count < 5,
            "Without drain, messages should be lost. Got {} (expected < 5). \
             This test validates the bug exists when drain is missing.",
            count
        );
    }
}
