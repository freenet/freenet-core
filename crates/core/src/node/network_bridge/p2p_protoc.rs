use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::message::{NetMessageV1, QueryResult};
use dashmap::DashSet;
use either::{Either, Left, Right};
use freenet_stdlib::client_api::ErrorKind;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
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
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot::{self};
use tokio::time::timeout;
use tracing::Instrument;

use crate::dev_tool::Location;
use crate::node::network_bridge::handshake::{
    EstablishConnection, Event as HandshakeEvent, ForwardInfo, HandshakeError, HandshakeHandler,
    OutboundMessage,
};
use crate::node::PeerId;
use crate::transport::{
    create_connection_handler, PeerConnection, TransportError, TransportKeypair,
};
use crate::{
    client_events::ClientId,
    config::GlobalExecutor,
    contract::{
        ClientResponsesSender, ContractHandlerChannel, ExecutorToEventLoopChannel,
        NetworkEventListenerHalve, WaitingResolution,
    },
    message::{MessageStats, NetMessage, NodeEvent, Transaction},
    node::{handle_aborted_op, process_message, NetEventRegister, NodeConfig, OpManager},
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
}

impl P2pConnManager {
    pub async fn build(
        config: &NodeConfig,
        op_manager: Arc<OpManager>,
        event_listener: impl NetEventRegister + Clone,
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
        })
    }

    #[tracing::instrument(name = "network_event_listener", fields(peer = %self.bridge.op_manager.ring.connection_manager.pub_key), skip_all)]
    pub async fn run_event_listener(
        mut self,
        op_manager: Arc<OpManager>,
        mut client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
        mut notification_channel: EventLoopNotificationsReceiver,
        mut executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
        cli_response_sender: ClientResponsesSender,
        mut node_controller: Receiver<NodeEvent>,
    ) -> anyhow::Result<Infallible> {
        tracing::info!(%self.listening_port, %self.listening_ip, %self.is_gateway, key = %self.key_pair.public(), "Opening network listener");

        let mut state = EventListenerState::new();

        let (outbound_conn_handler, inbound_conn_handler) = create_connection_handler::<UdpSocket>(
            self.key_pair.clone(),
            self.listening_ip,
            self.listening_port,
            self.is_gateway,
        )
        .await?;

        let (mut handshake_handler, establish_connection, outbound_message) = HandshakeHandler::new(
            inbound_conn_handler,
            outbound_conn_handler.clone(),
            self.bridge.op_manager.ring.connection_manager.clone(),
            self.bridge.op_manager.ring.router.clone(),
        );

        loop {
            let event = self
                .wait_for_event(
                    &mut state,
                    &mut handshake_handler,
                    &mut notification_channel,
                    &mut node_controller,
                    &mut client_wait_for_transaction,
                    &mut executor_listener,
                )
                .await;

            match event {
                EventResult::Continue => continue,
                EventResult::Event(event) => {
                    match event {
                        ConnEvent::InboundMessage(msg) => {
                            self.handle_inbound_message(
                                msg,
                                &outbound_message,
                                &op_manager,
                                &mut state,
                                &executor_listener,
                                &cli_response_sender,
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
                                self.bridge.op_manager.completed(id);
                                continue;
                            };
                            tracing::debug!(%target_peer, %msg, "Sending message to peer");
                            match self.connections.get(&target_peer.peer) {
                                Some(peer_connection) => {
                                    if let Err(e) = peer_connection.send(Left(msg)).await {
                                        tracing::error!("Failed to send message to peer: {}", e);
                                    }
                                }
                                None => {
                                    tracing::error!(
                                        id = %msg.id(),
                                        target = %target_peer.peer,
                                        "No existing outbound connection to forward the message"
                                    );
                                }
                            }
                        }

                        ConnEvent::HandshakeAction(action) => {
                            self.handle_handshake_action(action, &mut state).await?;
                        }
                        ConnEvent::ClosedChannel => {
                            tracing::info!("Notification channel closed");
                            break;
                        }
                        ConnEvent::NodeAction(action) => match action {
                            NodeEvent::DropConnection(peer) => {
                                tracing::debug!(%peer, "Dropping connection");
                                if let Some(conn) = self.connections.remove(&peer) {
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
                                self.handle_connect_peer(
                                    peer,
                                    Box::new(callback),
                                    tx,
                                    &establish_connection,
                                    &mut state,
                                    is_gw,
                                )
                                .await?;
                            }
                            NodeEvent::QueryConnections { callback } => {
                                let connections = self.connections.keys().cloned().collect();
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
                            NodeEvent::TransactionTimedOut(tx) => {
                                let Some(client) = state.tx_to_client.remove(&tx) else {
                                    continue;
                                };
                                cli_response_sender
                                    .send((client, Err(ErrorKind::FailedOperation.into())))?;
                            }
                            NodeEvent::Disconnect { cause } => {
                                tracing::info!(
                                    "Disconnecting from network{}",
                                    cause.map(|c| format!(": {}", c)).unwrap_or_default()
                                );
                                break;
                            }
                        },
                    }
                }
            }
        }
        Err(anyhow::anyhow!(
            "Network event listener exited unexpectedly"
        ))
    }

    async fn wait_for_event(
        &mut self,
        state: &mut EventListenerState,
        handshake_handler: &mut HandshakeHandler,
        notification_channel: &mut EventLoopNotificationsReceiver,
        node_controller: &mut Receiver<NodeEvent>,
        client_wait_for_transaction: &mut ContractHandlerChannel<WaitingResolution>,
        executor_listener: &mut ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    ) -> EventResult {
        select! {
            msg = state.peer_connections.next(), if !state.peer_connections.is_empty() => {
                self.handle_peer_connection_msg(msg, state).await
            }
            msg = notification_channel.0.recv() => {
                self.handle_notification_msg(msg)
            }
            msg = self.conn_bridge_rx.recv() => {
                self.handle_bridge_msg(msg)
            }
            msg = handshake_handler.wait_for_events() => {
                self.handle_handshake_msg(msg)
            }
            msg = node_controller.recv() => {
                self.handle_node_controller_msg(msg)
            }
            event_id = client_wait_for_transaction.relay_transaction_result_to_client() => {
                self.handle_client_transaction_result(event_id, state)
            }
            id = executor_listener.transaction_from_executor() => {
                self.handle_executor_transaction(id, state)
            }
        }
    }

    async fn handle_inbound_message(
        &self,
        msg: NetMessage,
        outbound_message: &OutboundMessage,
        op_manager: &Arc<OpManager>,
        state: &mut EventListenerState,
        executor_listener: &ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
        cli_response_sender: &ClientResponsesSender,
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
                    self.process_message(
                        msg,
                        op_manager,
                        executor_listener,
                        cli_response_sender,
                        state,
                    )
                    .await;
                }
            }
        }
        Ok(())
    }

    async fn process_message(
        &self,
        msg: NetMessage,
        op_manager: &Arc<OpManager>,
        executor_listener: &ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
        cli_response_sender: &ClientResponsesSender,
        state: &mut EventListenerState,
    ) {
        let executor_callback = state
            .pending_from_executor
            .remove(msg.id())
            .then(|| executor_listener.callback());
        let pending_client_req = state.tx_to_client.get(msg.id()).copied();
        let client_req_handler_callback = pending_client_req.map(|_| cli_response_sender.clone());

        let span = tracing::info_span!(
            "process_network_message",
            transaction = %msg.id(),
            tx_type = %msg.id().transaction_type()
        );

        GlobalExecutor::spawn(
            process_message(
                msg,
                op_manager.clone(),
                self.bridge.clone(),
                self.event_listener.trait_clone(),
                executor_callback,
                client_req_handler_callback,
                pending_client_req,
            )
            .instrument(span),
        );
    }

    async fn handle_connect_peer(
        &mut self,
        peer: PeerId,
        callback: Box<dyn ConnectResultSender>,
        tx: Transaction,
        establish_connection: &EstablishConnection,
        state: &mut EventListenerState,
        is_gw: bool,
    ) -> anyhow::Result<()> {
        tracing::info!(tx = %tx, remote = %peer, "Connecting to peer");
        state.awaiting_connection.insert(peer.addr, callback);
        let res = timeout(
            Duration::from_secs(10),
            establish_connection.establish_conn(peer.clone(), tx, is_gw),
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
    ) -> anyhow::Result<()> {
        match event {
            HandshakeEvent::InboundConnection {
                id,
                conn,
                joiner,
                op,
                forward_info,
            } => {
                let (tx, rx) = mpsc::channel(1);
                self.connections.insert(joiner.clone(), tx);
                let was_reserved = {
                    // this is an unexpected inbound request at a gateway so it didn't have a reserved spot
                    false
                };
                self.bridge
                    .op_manager
                    .ring
                    .add_connection(
                        Location::from_address(&joiner.addr),
                        joiner.clone(),
                        was_reserved,
                    )
                    .await;
                if let Some(op) = op {
                    self.bridge
                        .op_manager
                        .push(id, crate::operations::OpEnum::Connect(op))
                        .await?;
                }
                let task = peer_connection_listener(rx, conn).boxed();
                state.peer_connections.push(task);

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
                self.handle_successful_connection(peer_id, connection, state, None)
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
                    Some(remaining_checks),
                )
                .await?;
            }
            HandshakeEvent::OutboundConnectionFailed { peer_id, error } => {
                tracing::info!(%peer_id, "Connection failed: {:?}", error);
                if let Some(mut r) = state.awaiting_connection.remove(&peer_id.addr) {
                    r.send_result(Err(error)).await?;
                }
            }
            HandshakeEvent::RemoveTransaction(tx) => {
                state.transient_conn.remove(&tx);
            }
            HandshakeEvent::OutboundGatewayConnectionRejected { peer_id } => {
                tracing::info!(%peer_id, "Connection rejected by peer");
                if let Some(mut r) = state.awaiting_connection.remove(&peer_id.addr) {
                    r.send_result(Err(HandshakeError::ChannelClosed)).await?;
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
        state.peer_connections.push(task);
        Ok(())
    }

    async fn handle_peer_connection_msg(
        &mut self,
        msg: Option<Result<PeerConnectionInbound, TransportError>>,
        state: &mut EventListenerState,
    ) -> EventResult {
        match msg {
            Some(Ok(peer_conn)) => {
                let task = peer_connection_listener(peer_conn.rx, peer_conn.conn).boxed();
                state.peer_connections.push(task);
                EventResult::Event(ConnEvent::InboundMessage(peer_conn.msg))
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
                    }
                }
                EventResult::Continue
            }
            None => {
                tracing::error!("All peer connections closed");
                EventResult::Continue
            }
        }
    }

    fn handle_notification_msg(&self, msg: Option<Either<NetMessage, NodeEvent>>) -> EventResult {
        match msg {
            Some(Left(msg)) => EventResult::Event(ConnEvent::InboundMessage(msg)),
            Some(Right(action)) => EventResult::Event(ConnEvent::NodeAction(action)),
            None => EventResult::Continue,
        }
    }

    fn handle_bridge_msg(&self, msg: Option<P2pBridgeEvent>) -> EventResult {
        match msg {
            Some(Left((_, msg))) => EventResult::Event(ConnEvent::OutboundMessage(*msg)),
            Some(Right(action)) => EventResult::Event(ConnEvent::NodeAction(action)),
            None => EventResult::Event(ConnEvent::ClosedChannel),
        }
    }

    fn handle_handshake_msg(&self, msg: Result<HandshakeEvent, HandshakeError>) -> EventResult {
        match msg {
            Ok(event) => EventResult::Event(ConnEvent::HandshakeAction(event)),
            Err(HandshakeError::ChannelClosed) => EventResult::Event(ConnEvent::ClosedChannel),
            _ => EventResult::Continue,
        }
    }

    fn handle_node_controller_msg(&self, msg: Option<NodeEvent>) -> EventResult {
        match msg {
            Some(msg) => EventResult::Event(ConnEvent::NodeAction(msg)),
            None => EventResult::Event(ConnEvent::ClosedChannel),
        }
    }

    fn handle_client_transaction_result(
        &self,
        event_id: Result<(ClientId, Transaction), anyhow::Error>,
        state: &mut EventListenerState,
    ) -> EventResult {
        let Ok((client_id, transaction)) = event_id.map_err(|e| {
            tracing::debug!("Error while receiving client transaction result: {:?}", e);
        }) else {
            return EventResult::Continue;
        };
        state.tx_to_client.insert(transaction, client_id);
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
    peer_connections:
        FuturesUnordered<BoxFuture<'static, Result<PeerConnectionInbound, TransportError>>>,
    pending_from_executor: HashSet<Transaction>,
    tx_to_client: HashMap<Transaction, ClientId>,
    transient_conn: HashMap<Transaction, SocketAddr>,
    awaiting_connection: HashMap<SocketAddr, Box<dyn ConnectResultSender>>,
}

impl EventListenerState {
    fn new() -> Self {
        Self {
            peer_connections: FuturesUnordered::new(),
            pending_from_executor: HashSet::new(),
            tx_to_client: HashMap::new(),
            transient_conn: HashMap::new(),
            awaiting_connection: HashMap::new(),
        }
    }
}

enum EventResult {
    Continue,
    Event(ConnEvent),
}

#[derive(Debug)]
enum ConnEvent {
    InboundMessage(NetMessage),
    OutboundMessage(NetMessage),
    HandshakeAction(HandshakeEvent),
    NodeAction(NodeEvent),
    ClosedChannel,
}

#[allow(dead_code)]
enum ProtocolStatus {
    Unconfirmed,
    Confirmed,
    Reported,
    Failed,
}

struct PeerConnectionInbound {
    conn: PeerConnection,
    /// Receiver for inbound messages for the peer connection
    rx: Receiver<Either<NetMessage, ConnEvent>>,
    msg: NetMessage,
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
                            ConnEvent::NodeAction(NodeEvent::DropConnection(_)) | ConnEvent::ClosedChannel => {
                                break Err(TransportError::ConnectionClosed(conn.remote_addr()));
                            }
                            other => {
                                unreachable!("Unexpected action: {:?}", other);
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

// TODO: add testing for the network loop, now it should be possible to do since we don't depend upon having real connections
