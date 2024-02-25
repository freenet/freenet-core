use std::future::Future;
use std::net::SocketAddr;
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
};

use asynchronous_codec::BytesMut;
use dashmap::{DashMap, DashSet};
use either::{Either, Left, Right};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt, TryFutureExt};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::Instrument;

use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::message::ConnectionResult;
use crate::node::network_bridge::p2p_protoc::ConnMngrActions::{
    ClosedChannel, ConnectionEstablished, NodeAction, SendMessage,
};
use crate::node::PeerId;
use crate::operations::connect::{ConnectMsg, ConnectRequest};
use crate::transport::{BytesPerSecond, ConnectionHandler, PeerConnection, TransportKeypair};
use crate::{
    client_events::ClientId,
    config::GlobalExecutor,
    contract::{
        ClientResponsesSender, ContractHandlerChannel, ExecutorToEventLoopChannel,
        NetworkEventListenerHalve, WaitingResolution,
    },
    message::{NetMessage, NodeEvent, Transaction},
    node::{
        handle_aborted_op, process_message, NetEventRegister, NodeConfig, OpManager,
        PeerId as FreenetPeerId,
    },
    ring::PeerKeyLocation,
    tracing::NetEventLog,
};

/// The default maximum size for a varint length-delimited packet.
pub const DEFAULT_MAX_PACKET_SIZE: usize = 16 * 1024;

const CURRENT_AGENT_VER: &str = "/freenet/agent/0.1.0";
const CURRENT_PROTOC_VER: &str = "/freenet/0.1.0";
const CURRENT_PROTOC_VER_STR: &str = "/freenet/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "/id/1.0.0";

type P2pBridgeEvent = Either<(FreenetPeerId, Box<NetMessage>), NodeEvent>;

#[derive(Clone)]
pub(crate) struct P2pBridge {
    active_net_connections: Arc<DashMap<FreenetPeerId, SocketAddr>>,
    accepted_peers: Arc<DashSet<FreenetPeerId>>,
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
            active_net_connections: Arc::new(DashMap::new()),
            accepted_peers: Arc::new(DashSet::new()),
            ev_listener_tx: sender,
            op_manager,
            log_register: Arc::new(event_register),
        }
    }
}

#[async_trait::async_trait]
impl NetworkBridge for P2pBridge {
    async fn try_add_connection(&mut self, peer: FreenetPeerId) -> super::ConnResult<()> {
        if self.active_net_connections.contains_key(&peer) {
            self.accepted_peers.insert(peer);
        }
        // Implement typed channel to sent ok if all fine or error if not
        let (result_sender, mut result_receiver) = mpsc::channel(1);
        self.ev_listener_tx
            .send(Right(NodeEvent::AcceptConnection(
                peer.clone(),
                result_sender,
            )))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        // Retrun Ok or ConnectionError depending if reciver recives a ConnAcion::ConnectionAccepted OR NOT
        match result_receiver.recv().await {
            Some(ConnectionResult::Accepted) => Ok(()),
            _ => Err(ConnectionError::SendNotCompleted),
        }
    }

    async fn drop_connection(&mut self, peer: &FreenetPeerId) -> super::ConnResult<()> {
        self.accepted_peers.remove(peer);
        self.ev_listener_tx
            .send(Right(NodeEvent::DropConnection(peer.clone())))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        self.log_register
            .register_events(Either::Left(NetEventLog::disconnected(
                &self.op_manager.ring,
                peer,
            )))
            .await;
        Ok(())
    }

    async fn send(&self, target: &FreenetPeerId, msg: NetMessage) -> super::ConnResult<()> {
        self.log_register
            .register_events(NetEventLog::from_outbound_msg(&msg, &self.op_manager.ring))
            .await;
        self.op_manager.sending_transaction(target, &msg);
        self.ev_listener_tx
            .send(Left((target.clone(), Box::new(msg))))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        Ok(())
    }
}

type PeerConnChannel = Sender<NetMessage>;

pub(in crate::node) struct P2pConnManager {
    conn_handler: ConnectionHandler,
    pub(in crate::node) gateways: Vec<PeerKeyLocation>,
    pub(in crate::node) bridge: P2pBridge,
    conn_bridge_rx: Receiver<P2pBridgeEvent>,
    /// last valid observed public address
    public_addr: Option<SocketAddr>,
    listening_addr: Option<SocketAddr>,
    event_listener: Box<dyn NetEventRegister>,
    connection: HashMap<PeerId, PeerConnChannel>,
}

impl P2pConnManager {
    pub async fn build(
        config: &NodeConfig,
        op_manager: Arc<OpManager>,
        event_listener: impl NetEventRegister + Clone,
        private_key: TransportKeypair,
    ) -> Result<Self, anyhow::Error> {
        let listen_port = config
            .local_port
            .ok_or_else(|| anyhow::anyhow!("private_addr does not contain a port"))?;

        let private_addr = if let Some(conn) = config.local_ip.zip(config.local_port) {
            let public_addr = SocketAddr::from(conn);
            Some(public_addr)
        } else {
            None
        };

        let conn_handler = ConnectionHandler::new::<UdpSocket>(
            private_key,
            config.listener_ip,
            listen_port,
            config.is_gateway(),
            BytesPerSecond::new(0.1),
        )
        .await?;

        let (tx_bridge_cmd, rx_bridge_cmd) = mpsc::channel(100);
        let bridge = P2pBridge::new(tx_bridge_cmd, op_manager, event_listener.clone());

        let gateways = config.get_gateways()?;
        Ok(P2pConnManager {
            conn_handler,
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            public_addr: None,
            listening_addr: private_addr,
            event_listener: Box::new(event_listener),
            connection: HashMap::new(),
        })
    }

    #[tracing::instrument(name = "network_event_listener", fields(peer = % self.bridge.op_manager.ring.peer_key), skip_all)]
    pub async fn run_event_listener(
        mut self,
        op_manager: Arc<OpManager>,
        mut client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
        mut notification_channel: EventLoopNotificationsReceiver,
        mut executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
        cli_response_sender: ClientResponsesSender,
        mut node_controller: Receiver<NodeEvent>,
    ) -> Result<(), anyhow::Error> {
        use ConnMngrActions::*;

        // FIXME: this two containers need to be clean up on transaction time-out
        let mut pending_from_executor = HashSet::new();
        let mut tx_to_client: HashMap<Transaction, ClientId> = HashMap::new();

        let mut peer_connections = FuturesUnordered::new();

        loop {
            let notification_msg = notification_channel.0.recv().map(|m| match m {
                None => Ok(Right(ClosedChannel)),
                Some(Left(msg)) => Ok(Left(msg)),
                Some(Right(action)) => Ok(Right(NodeAction(action))),
            });

            let bridge_msg = async {
                match self.conn_bridge_rx.recv().await {
                    Some(msg) => match msg {
                        Left((peer, msg)) => {
                            if let NetMessage::Connect(ConnectMsg::Request {
                                id,
                                msg:
                                    ConnectRequest::StartReq {
                                        target: this_peer,
                                        joiner,
                                        ..
                                    },
                            }) = &*msg
                            {
                                match self
                                    .conn_handler
                                    .connect(peer.pub_key, peer.addr, true)
                                    .await
                                {
                                    Ok(peer_conn) => {
                                        tracing::debug!(
                                            "Connection established with peer {}",
                                            peer.addr.clone()
                                        );
                                        Ok(Right(ConnectionEstablished {
                                            peer: peer.clone(),
                                            peer_conn,
                                        }))
                                    }
                                    Err(e) => Ok(Right(ClosedChannel)),
                                }
                            } else {
                                tracing::debug!("Message outbound: {:?}", msg);
                                Ok(Right(SendMessage { peer, msg }))
                            }
                        }
                        Right(action) => Ok(Right(NodeAction(action))),
                    },
                    None => Ok(Right(ClosedChannel)),
                }
            };

            let msg: Result<_, ConnectionError> = tokio::select! {
                msg = peer_connections.next(), if !peer_connections.is_empty() => {
                    match msg {
                        Some(Ok(incoming_msg)) => {
                            let incoming_msg: Vec<u8> = incoming_msg;
                            let netwiork_msg = decode_msg(BytesMut::from(incoming_msg.as_slice())).map_err(|err| anyhow::anyhow!(err))?;
                            Ok(Left(netwiork_msg))
                        },
                        _ => Ok(Right(ClosedChannel))
                    }
                }
                msg = notification_msg => { msg }
                msg = bridge_msg => { msg }
                msg = node_controller.recv() => {
                    if let Some(msg) = msg {
                        Ok(Right(NodeAction(msg)))
                    } else {
                        Ok(Right(ClosedChannel))
                    }
                }
                event_id = client_wait_for_transaction.relay_transaction_result_to_client() => {
                    let (client_id, transaction) = event_id.map_err(|err| anyhow::anyhow!(err))?;
                    tx_to_client.insert(transaction, client_id);
                    continue;
                }
                id = executor_listener.transaction_from_executor() => {
                    let id = id.map_err(|err| anyhow::anyhow!(err))?;
                    pending_from_executor.insert(id);
                    continue;
                }
            };

            match msg {
                Ok(Left(msg)) => {
                    let cb = self.bridge.clone();
                    match msg {
                        NetMessage::Aborted(tx) => {
                            handle_aborted_op(
                                tx,
                                op_manager.ring.peer_key.clone(),
                                &op_manager,
                                &mut self.bridge,
                                &self.gateways,
                            )
                            .await?;
                            continue;
                        }
                        msg => {
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
                                // peer = %this_peer,
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
                Ok(Right(SendMessage { peer, msg })) => {
                    tracing::debug!(
                        "Sending swarm message from {} to {}",
                        op_manager.ring.peer_key,
                        peer
                    );
                    if let Some(tx) = self.connection.get(&peer) {
                        tx.send(*msg).await?;
                    } else {
                        tracing::warn!("No connection to peer {}", peer);
                    }
                }
                Ok(Right(NodeAction(NodeEvent::ShutdownNode))) => {
                    tracing::info!("Shutting down message loop gracefully");
                    break;
                }
                Ok(Right(NodeAction(NodeEvent::Error(err)))) => {
                    tracing::error!("Bridge conn error: {err}");
                }
                Ok(Right(NodeAction(NodeEvent::AcceptConnection(_key, result_sender)))) => {
                    // todo: if we prefilter connections, should only accept ones informed this way
                    //       (except 'join ring' requests)
                    result_sender.send(ConnectionResult::Accepted).await?;
                }
                Ok(Right(NodeAction(NodeEvent::Disconnect { cause }))) => {
                    match cause {
                        Some(cause) => tracing::warn!("Shutting down node: {cause}"),
                        None => tracing::warn!("Shutting down node"),
                    }
                    return Ok(());
                }
                Ok(Right(ConnectionEstablished {
                    peer,
                    mut peer_conn,
                })) => {
                    let addr = peer.addr.clone();
                    tracing::debug!("Established connection with peer @ {}", addr);
                    self.bridge
                        .active_net_connections
                        .insert(peer.clone(), addr);

                    let (tx, mut rx) = mpsc::channel(100);
                    self.connection.insert(peer.clone(), tx);

                    // Spawn a task to handle the connection outbound messages
                    tokio::spawn(async move {
                        loop {
                            tokio::select! {
                                Some(msg) = rx.recv() => {
                                    peer_conn
                                        .send(&msg)
                                        .await
                                        .map_err(|err| ConnectionError::IOError(err.to_string()))?;
                                }
                                else => {
                                    break;
                                }
                            }
                        }
                        Ok::<_, ConnectionError>(())
                    });

                    peer_connections.push(peer_conn.recv());
                }
                Ok(Right(ConnectionClosed { peer: peer_id }))
                | Ok(Right(NodeAction(NodeEvent::DropConnection(peer_id)))) => {
                    self.bridge.active_net_connections.remove(&peer_id);
                    op_manager.ring.prune_connection(peer_id.clone()).await;
                    // todo: notify the handler, read `disconnect_peer_id` doc
                    tracing::info!("Dropped connection with peer {}", peer_id);
                }
                Ok(Right(UpdatePublicAddr(address))) => {
                    self.public_addr = Some(address);
                }
                Ok(Right(IsPrivatePeer(_peer))) => {
                    todo!("this peer is private, attempt hole punching")
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
                Ok(Right(NoAction)) | Ok(Right(NodeAction(NodeEvent::ConfirmedInbound))) => {}
                _ => {}
            }
        }
        Ok(())
    }
}

enum ConnMngrActions {
    /// Received a new connection
    ConnectionEstablished {
        peer: FreenetPeerId,
        peer_conn: PeerConnection,
    },
    /// Closed a connection with the peer
    ConnectionClosed {
        peer: FreenetPeerId,
    },
    /// Outbound message
    SendMessage {
        peer: FreenetPeerId,
        msg: Box<NetMessage>,
    },
    /// Update self own public address, useful when communicating for first time
    UpdatePublicAddr(SocketAddr),
    /// This is private, so when establishing connections hole-punching should be performed
    IsPrivatePeer(SocketAddr),
    NodeAction(NodeEvent),
    ClosedChannel,
    NoAction,
}

type UniqConnId = usize;

#[derive(Debug)]
pub(in crate::node) enum HandlerEvent {
    Inbound(Either<NetMessage, NodeEvent>),
    Outbound(Either<NetMessage, NodeEvent>),
}

#[allow(dead_code)]
enum ProtocolStatus {
    Unconfirmed,
    Confirmed,
    Reported,
    Failed,
}

#[inline(always)]
fn encode_msg(msg: NetMessage) -> Result<Vec<u8>, ConnectionError> {
    bincode::serialize(&msg).map_err(|err| ConnectionError::Serialization(Some(err)))
}

#[inline(always)]
fn decode_msg(buf: BytesMut) -> Result<NetMessage, ConnectionError> {
    let cursor = std::io::Cursor::new(buf);
    bincode::deserialize_from(cursor).map_err(|err| ConnectionError::Serialization(Some(err)))
}
