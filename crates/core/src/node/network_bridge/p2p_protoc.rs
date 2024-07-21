use std::net::IpAddr;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dashmap::DashSet;
use either::{Either, Left, Right};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::Instrument;

use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::message::NetMessageV1;

use crate::node::network_bridge::handshake::{
    EstablishConnection, Event, HandshakeError, HandshakeHandler, InboundJoinRequest,
};
use crate::node::PeerId;
use crate::operations::connect::{ConnectMsg, ConnectRequest};
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

type PeerConnChannelSender = Sender<Either<NetMessage, ConnMngrActions>>;
type PeerConnChannelRecv = Receiver<Either<NetMessage, ConnMngrActions>>;

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

    #[tracing::instrument(name = "network_event_listener", fields(peer = ?self.bridge.op_manager.ring.connection_manager.get_peer_key()), skip_all)]
    pub async fn run_event_listener(
        mut self,
        op_manager: Arc<OpManager>,
        mut client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
        mut notification_channel: EventLoopNotificationsReceiver,
        mut executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
        cli_response_sender: ClientResponsesSender,
        mut node_controller: Receiver<NodeEvent>,
    ) -> anyhow::Result<()> {
        use ConnMngrActions::*;

        tracing::info!(%self.listening_port, %self.listening_ip, %self.is_gateway, key = %self.key_pair.public(), "Openning network listener");

        let (outbound_conn_handler, inbound_conn_handler) = create_connection_handler::<UdpSocket>(
            self.key_pair.clone(),
            self.listening_ip,
            self.listening_port,
            self.is_gateway,
        )
        .await?;

        // FIXME: this two containers need to be clean up on transaction time-out
        let mut pending_from_executor = HashSet::new();
        let mut tx_to_client: HashMap<Transaction, ClientId> = HashMap::new();
        let mut transient_conn = HashMap::new();

        // stores the callbacks for connections that are being established
        // should be clean up when the connection is established
        let mut awaiting_connection = HashMap::new();

        // stores the persistent connections after they have been established and accepted
        let mut peer_connections = FuturesUnordered::new();

        let (mut handshake_handler, establish_connection, outbound_message) = HandshakeHandler::new(
            inbound_conn_handler,
            outbound_conn_handler.clone(),
            self.bridge.op_manager.ring.connection_manager.clone(),
            self.bridge.op_manager.ring.router.clone(),
        );

        // TODO: move the code inside the loop to a function
        loop {
            let notification_msg = notification_channel.0.recv().map(|m| match m {
                None => Ok(Right(ClosedChannel)),
                Some(Left(msg)) => Ok(Left(msg)),
                Some(Right(action)) => Ok(Right(NodeAction(action))),
            });

            let conn_event = handshake_handler.wait_for_events();

            let bridge_msg = async {
                while let Some(msg) = self.conn_bridge_rx.recv().await {
                    match msg {
                        Left((peer, msg)) => {
                            tracing::debug!("Message outbound: {:?}", msg);
                            if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                                id,
                                msg: ConnectRequest::StartJoinReq { .. },
                            })) = *msg
                            {
                                if self.connections.contains_key(&peer) {
                                    tracing::warn!(
                                        "Connection already exists with gateway {}",
                                        peer.addr
                                    );
                                    continue;
                                }

                                if let Err(e) = Self::try_establish_connection(
                                    &establish_connection,
                                    peer.clone(),
                                    id,
                                )
                                .await
                                {
                                    tracing::error!(
                                        "Failed to initiate connection process: {:?}",
                                        e
                                    );
                                    return Ok(Right(ClosedChannel));
                                }
                                continue;
                            }
                            return Ok(Left(*msg));
                        }
                        Right(action) => return Ok(Right(NodeAction(action))),
                    }
                }
                Ok(Right(ClosedChannel))
            };

            let msg: Result<_, ConnectionError> = tokio::select! {
                msg = peer_connections.next(), if !peer_connections.is_empty() => {
                    let PeerConnectionInbound { conn, rx, msg } = match msg {
                        Some(Ok(peer_conn)) => peer_conn,
                        Some(Err(err)) => {
                            tracing::error!("Error in peer connection: {err}");
                            if let TransportError::ConnectionClosed(socket_addr) = err {
                                if let Some(peer) = self.connections.keys().find_map(|k| (k.addr == socket_addr).then(|| k.clone())) {
                                    op_manager.ring.prune_connection(peer.clone()).await;
                                    self.connections.remove(&peer);
                                }
                            }
                            continue;
                        }
                        None =>  {
                            tracing::error!("All peer connections closed");
                            continue;
                        }
                    };
                    let task = peer_connection_listener(rx, conn).boxed();
                    peer_connections.push(task);
                    Ok(Left(msg))
                }
                msg = notification_msg => { msg }
                msg = bridge_msg => { msg }
                msg = conn_event => {
                   match msg {
                        Ok(event) => Ok(Right(HandShakeAction(event))),
                        Err(HandshakeError::ChannelClosed) => Ok(Right(ClosedChannel)),
                        _ => continue,
                    }
                }
                msg = node_controller.recv() => {
                    if let Some(msg) = msg {
                        Ok(Right(NodeAction(msg)))
                    } else {
                        Ok(Right(ClosedChannel))
                    }
                }
                event_id = client_wait_for_transaction.relay_transaction_result_to_client() => {
                    let (client_id, transaction) = event_id.map_err(anyhow::Error::msg)?;
                    tx_to_client.insert(transaction, client_id);
                    continue;
                }
                id = executor_listener.transaction_from_executor() => {
                    let id = id.map_err(anyhow::Error::msg)?;
                    pending_from_executor.insert(id);
                    continue;
                }
            };

            match msg {
                // This are either inbound messages or fast tracked by the event notifier
                Ok(Left(msg)) => {
                    let cb = self.bridge.clone();
                    match msg {
                        NetMessage::V1(NetMessageV1::Aborted(tx)) => {
                            handle_aborted_op(
                                tx,
                                op_manager.ring.get_peer_pub_key(),
                                &op_manager,
                                &mut self.bridge,
                                &self.gateways,
                            )
                            .await?;
                            continue;
                        }
                        msg => {
                            if let Some(addr) = transient_conn.get(msg.id()) {
                                // this is a message that should be sent to the handshake handler
                                // so it can be forwarded to a transient joiner
                                outbound_message.send_to(*addr, msg).await?;
                                continue;
                            }
                            let executor_callback = pending_from_executor
                                .remove(msg.id())
                                .then(|| executor_listener.callback());
                            let pending_client_req = tx_to_client.get(msg.id()).copied();
                            let client_req_handler_callback = if pending_client_req.is_some() {
                                Some(cli_response_sender.clone())
                            } else {
                                None
                            };
                            let parent_span = tracing::Span::current();
                            let span = tracing::info_span!(
                                parent: parent_span,
                                "process_network_message",
                                peer = ?self.bridge.op_manager.ring.connection_manager.get_peer_key(),
                                transaction = %msg.id(),
                                tx_type = %msg.id().transaction_type()
                            );
                            GlobalExecutor::spawn(
                                process_message(
                                    msg,
                                    op_manager.clone(),
                                    cb,
                                    self.event_listener.trait_clone(),
                                    executor_callback,
                                    client_req_handler_callback,
                                    pending_client_req,
                                )
                                .instrument(span),
                            );
                        }
                    }
                }
                Ok(Right(NodeAction(NodeEvent::ConnectPeer { peer, callback, tx }))) => {
                    tracing::info!(remote = %peer, this_peer = ?op_manager.ring.connection_manager.get_peer_key().unwrap(), "Connecting to peer");
                    awaiting_connection.insert(peer.addr, callback);
                    establish_connection.establish_conn(peer, tx).await?;
                }
                Ok(Right(HandShakeAction(event))) => {
                    match event {
                        Event::InboundConnection(InboundJoinRequest {
                            conn,
                            id,
                            joiner,
                            joiner_key,
                            hops_to_live,
                            max_hops_to_live,
                            skip_list,
                        }) => {
                            let (tx, rx) = mpsc::channel(1);
                            self.connections
                                .insert(joiner.expect("should be set at this point"), tx);

                            // Spawn a task to handle the connection messages (inbound and outbound)
                            let task = peer_connection_listener(rx, conn).boxed();
                            peer_connections.push(task);
                            todo!("Need to add the operation to the op_manager properly, in the correct state")
                        }
                        Event::TransientForwardTransaction {
                            target,
                            tx,
                            forward_to,
                            msg,
                        } => {
                            if let Some(older_addr) = transient_conn.insert(tx, target) {
                                debug_assert_eq!(older_addr, target);
                                tracing::warn!(
                                    %target,
                                    %forward_to,
                                    "Transaction {} already exists as transient connections",
                                    tx
                                );
                                if older_addr != target {
                                    tracing::error!(
                                        %tx,
                                        "Not same target in new and old transient connections: {} != {}",
                                        older_addr, target
                                    );
                                }
                            }
                            if let Some(peer) = self.connections.get(&forward_to) {
                                peer.send(Left(msg)).await?;
                            } else {
                                tracing::warn!(%forward_to, "No connection to forward the message");
                            }
                        }
                        Event::OutboundConnectionSuccessful {
                            peer_id,
                            connection,
                        } => {
                            if let Some(cb) = awaiting_connection.remove(&peer_id.addr) {
                                let _ = cb.send(Ok(())).await;
                            } else {
                                tracing::warn!(%peer_id, "No callback for connection established");
                            }
                            let (tx, rx) = mpsc::channel(10);
                            self.connections.insert(peer_id.clone(), tx);
                            // Spawn a task to handle the connection messages (inbound and outbound)
                            let task = peer_connection_listener(rx, connection).boxed();
                            peer_connections.push(task);
                        }
                        Event::OutboundGatewayConnectionSuccessful {
                            peer_id,
                            connection,
                        } => {
                            if let Some(cb) = awaiting_connection.remove(&peer_id.addr) {
                                let _ = cb.send(Ok(())).await;
                            } else {
                                tracing::warn!(%peer_id, "No callback for connection established");
                            }
                            let (tx, rx) = mpsc::channel(10);
                            self.connections.insert(peer_id.clone(), tx);
                            // Spawn a task to handle the connection messages (inbound and outbound)
                            let task = peer_connection_listener(rx, connection).boxed();
                            peer_connections.push(task);
                            todo!("Create a new connect op to handle the inbound forward connection attempts")
                        }
                        Event::OutboundConnectionFailed { peer_id, error } => {
                            tracing::info!(%peer_id, "Connection failed: {:?}", error);
                            awaiting_connection.remove(&peer_id.addr);
                        }
                        Event::RemoveTransaction(tx) => {
                            pending_from_executor.remove(&tx);
                            transient_conn.remove(&tx);
                            if let Some(cli) = tx_to_client.remove(&tx) {
                                todo!("probably need to do something with this, signal back that it failed")
                            }
                        }
                        Event::OutboundConnectionRejected { peer_id } => {
                            todo!()
                        }
                    }
                }
                Ok(Right(NodeAction(NodeEvent::Disconnect { cause }))) => {
                    match cause {
                        Some(cause) => tracing::warn!("Shutting down node: {cause}"),
                        None => tracing::warn!("Shutting down node"),
                    }
                    return Ok(());
                }
                Ok(Right(NodeAction(NodeEvent::DropConnection(peer_id)))) => {
                    tracing::info!(remote = %peer_id, this_peer = ?op_manager.ring.connection_manager.get_peer_key().unwrap(), "Dropping connection");
                    // gw_inbound_pending_connections.remove(&peer_id.addr());
                    // TODO: handshake handler should be able to drop the connection just in case
                    op_manager.ring.prune_connection(peer_id.clone()).await;
                    self.connections.remove(&peer_id);
                    tracing::info!("Dropped connection with peer {}", peer_id);
                }
                Ok(Right(ClosedChannel)) => {
                    tracing::info!("Notification channel closed");
                    break;
                }
                Err(err) => {
                    super::super::report_result(
                        None,
                        Err(err.into()),
                        &op_manager,
                        None,
                        None,
                        &mut *self.event_listener as &mut _,
                    )
                    .await;
                }
            }
        }
        Ok(())
    }

    async fn try_establish_connection(
        establish_connection: &EstablishConnection,
        peer: PeerId,
        id: Transaction,
    ) -> Result<(), HandshakeError> {
        match establish_connection.establish_conn(peer.clone(), id).await {
            Ok(()) => {
                tracing::debug!(
                    "Successfully initiated connection process for peer: {:?}",
                    peer
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug)]
enum ConnMngrActions {
    HandShakeAction(Event),
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
    rx: Receiver<Either<NetMessage, ConnMngrActions>>,
    msg: NetMessage,
}

async fn peer_connection_listener(
    mut rx: PeerConnChannelRecv,
    mut conn: PeerConnection,
) -> Result<PeerConnectionInbound, TransportError> {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Received message from channel");
                let Some(msg) = msg else { break Err(TransportError::ConnectionClosed(conn.remote_addr())); };
                match msg {
                    Left(msg) => {
                        tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr() ,"Sending message to peer. Msg: {msg}");
                        conn
                            .send(msg)
                            .await?;
                    }
                    Right(action) => {
                        tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Received action from channel");
                        match action {
                            ConnMngrActions::NodeAction(NodeEvent::DropConnection(_)) | ConnMngrActions::ClosedChannel => {
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
                let Ok(msg) = msg.map_err(|error| {
                    tracing::error!(at=?conn.my_address(), from=%conn.remote_addr(), "Error while receiving message: {error}");
                }) else {
                     break Err(TransportError::ConnectionClosed(conn.remote_addr()));
                };
                let net_message = decode_msg(&msg).unwrap();
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr() ,"Received message from peer. Msg: {net_message}");
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
