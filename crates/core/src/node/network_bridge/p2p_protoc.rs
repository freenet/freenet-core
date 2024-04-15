use std::net::{IpAddr, SocketAddr};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dashmap::{DashMap, DashSet};
use either::{Either, Left, Right};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::Instrument;

use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::message::ConnectionResult;

use crate::node::PeerId;
use crate::operations::connect::{ConnectMsg, ConnectRequest, ConnectResponse};
use crate::transport::{
    create_connection_handler, OutboundConnectionHandler, PeerConnection, TransportError,
    TransportKeypair,
};
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

type EncodedNetMessage = Vec<u8>;

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

impl NetworkBridge for P2pBridge {
    async fn try_add_connection(&mut self, peer: FreenetPeerId) -> super::ConnResult<()> {
        if self.active_net_connections.contains_key(&peer) {
            self.accepted_peers.insert(peer.clone());
        }
        let (result_sender, mut result_receiver) = mpsc::channel(1);
        self.ev_listener_tx
            .send(Right(NodeEvent::AcceptConnection(peer, result_sender)))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
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
    // conn_handler: Arc<Mutex<OutbountConnectionHandler>>,
    pub(in crate::node) gateways: Vec<PeerKeyLocation>,
    pub(in crate::node) bridge: P2pBridge,
    conn_bridge_rx: Receiver<P2pBridgeEvent>,
    /// last valid observed public address
    public_addr: Option<SocketAddr>,
    listening_addr: Option<SocketAddr>,
    event_listener: Box<dyn NetEventRegister>,
    connection: HashMap<PeerId, PeerConnChannel>,
    rejected_peers: HashSet<PeerId>,
    private_key: TransportKeypair,
    listener_ip: IpAddr,
    listen_port: u16,
    is_gateway: bool,
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

        let (tx_bridge_cmd, rx_bridge_cmd) = mpsc::channel(100);
        let bridge = P2pBridge::new(tx_bridge_cmd, op_manager, event_listener.clone());

        let gateways = config.get_gateways()?;
        Ok(P2pConnManager {
            // conn_handler: Arc::new(Mutex::new(conn_handler)),
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            public_addr: None,
            listening_addr: private_addr,
            event_listener: Box::new(event_listener),
            connection: HashMap::new(),
            rejected_peers: HashSet::new(),
            private_key,
            listener_ip: config.listener_ip,
            listen_port,
            is_gateway: config.is_gateway(),
        })
    }

    #[tracing::instrument(name = "network_event_listener", fields(peer = ?self.bridge.op_manager.ring.get_peer_key()), skip_all)]
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

        let (mut outbound_conn_handler, mut inbound_conn_handler) =
            create_connection_handler::<UdpSocket>(
                self.private_key.clone(),
                self.listener_ip,
                self.listen_port,
                self.is_gateway,
            )
            .await?;

        // FIXME: this two containers need to be clean up on transaction time-out
        let mut pending_from_executor = HashSet::new();
        let mut tx_to_client: HashMap<Transaction, ClientId> = HashMap::new();

        let mut peer_connections = FuturesUnordered::new();

        let conn_bridge = self.bridge.clone();

        loop {
            let notification_msg = notification_channel.0.recv().map(|m| match m {
                None => Ok(Right(ClosedChannel)),
                Some(Left(msg)) => Ok(Left(msg)),
                Some(Right(action)) => Ok(Right(NodeAction(action))),
            });

            let bridge_msg = async {
                loop {
                    match self.conn_bridge_rx.recv().await {
                        Some(msg) => match msg {
                            Left((peer, msg)) => {
                                if let Right(new_conn) = self
                                    .handle_bridge_connection_message(
                                        peer,
                                        msg,
                                        &mut outbound_conn_handler,
                                    )
                                    .await?
                                {
                                    break Ok::<_, anyhow::Error>(Left(new_conn));
                                }
                            }
                            Right(action) => break Ok(Right(NodeAction(action))),
                        },
                        None => break Ok(Right(ClosedChannel)),
                    }
                }
            };

            let new_incoming_connection = async {
                let peer = conn_bridge.op_manager.ring.get_peer_key().unwrap().clone();
                match inbound_conn_handler.next_connection().await {
                    Some(peer_conn) => Ok(Right(ConnectionEstablished {
                        peer: peer.clone(),
                        peer_conn,
                    })),
                    None => Ok(Right(ClosedChannel)),
                }
            };

            let msg: Result<_, ConnectionError> = tokio::select! {
                msg = peer_connections.next(), if !peer_connections.is_empty() => {
                    let PeerConnectionInbound { conn, rx, msg } = match msg {
                        Some(Ok(peer_conn)) => peer_conn,
                        Some(Err(err)) => {
                            tracing::error!("Error in peer connection: {err}");
                            // FIXME: clean up the remote connection from everywhere
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
                msg = bridge_msg => {
                    match msg? {
                        Left(peer_conn) => {
                            let peer = conn_bridge.op_manager.ring.get_peer_key().expect("Peer key not set");
                            Ok(Right(ConnectionEstablished {
                                peer,
                                peer_conn,
                            }))
                        }
                        Right(action) => Ok(Right(action)),
                    }
                }
                msg = node_controller.recv() => {
                    if let Some(msg) = msg {
                        Ok(Right(NodeAction(msg)))
                    } else {
                        Ok(Right(ClosedChannel))
                    }
                }
                msg = new_incoming_connection => { msg }
                event_id = client_wait_for_transaction.relay_transaction_result_to_client() => {
                    let (client_id, transaction) = event_id.map_err(|err| anyhow::Error::msg(err))?;
                    tx_to_client.insert(transaction, client_id);
                    continue;
                }
                id = executor_listener.transaction_from_executor() => {
                    let id = id.map_err(|err| anyhow::Error::msg(err))?;
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
                                op_manager.ring.get_peer_pub_key(),
                                &op_manager,
                                &mut self.bridge,
                                &self.gateways,
                            )
                            .await?;
                            continue;
                        }
                        NetMessage::Connect(ConnectMsg::Response {
                            msg:
                                ConnectResponse::AcceptedBy {
                                    accepted, joiner, ..
                                },
                            ..
                        }) => {
                            if accepted {
                                tracing::debug!("Connection accepted by target");
                            } else {
                                let peer_id = &joiner.peer;
                                tracing::debug!(remote = %joiner.peer, "Connection rejected by target");
                                self.connection.remove(&peer_id);
                                self.rejected_peers.insert(peer_id.clone());
                                self.bridge.active_net_connections.remove(&peer_id);
                                op_manager.ring.prune_connection(peer_id.clone()).await;
                            }
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
                        "Sending swarm message from {:?} to {}",
                        op_manager.ring.get_peer_key(),
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
                Ok(Right(ConnectionEstablished { peer, peer_conn })) => {
                    let addr = peer.addr.clone();
                    tracing::debug!("Established connection with peer @ {}", addr);
                    self.bridge
                        .active_net_connections
                        .insert(peer.clone(), addr);

                    let (tx, rx) = mpsc::channel(10);
                    self.connection.insert(peer.clone(), tx);

                    // Spawn a task to handle the connection messages (inbound and outbound)
                    let task = peer_connection_listener(rx, peer_conn).boxed();
                    peer_connections.push(task);
                }
                Ok(Right(ConnectionClosed { peer: peer_id }))
                | Ok(Right(NodeAction(NodeEvent::DropConnection(peer_id)))) => {
                    self.bridge.active_net_connections.remove(&peer_id);
                    op_manager.ring.prune_connection(peer_id.clone()).await;
                    self.connection.remove(&peer_id);
                    self.rejected_peers.insert(peer_id.clone());
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

    async fn handle_bridge_connection_message(
        &mut self,
        peer: PeerId,
        net_msg: Box<NetMessage>,
        outbound_conn_handler: &mut OutboundConnectionHandler,
    ) -> Result<Either<(), PeerConnection>, ConnectionError> {
        if let Some(conn) = self.connection.get(&peer) {
            conn.send(*net_msg).await;
            Ok(Left(()))
        } else if let NetMessage::Connect(ConnectMsg::Request { id, msg }) = *net_msg {
            self.handle_connection_request(&peer, id, msg, outbound_conn_handler)
                .await
        } else {
            Err(ConnectionError::UnexpectedReq)
        }
    }

    async fn handle_connection_request(
        &mut self,
        peer: &PeerId,
        id: Transaction,
        msg: ConnectRequest,
        outbound_conn_handler: &mut OutboundConnectionHandler,
    ) -> Result<Either<(), PeerConnection>, ConnectionError> {
        let (joiner_key, hops_to_live, skip_list) = match msg {
            ConnectRequest::StartJoinReq {
                joiner_key,
                hops_to_live,
                skip_list,
                ..
            } => (joiner_key, hops_to_live, skip_list),
            _ => return Err(ConnectionError::UnexpectedReq),
        };

        let mut peer_conn = outbound_conn_handler
            .connect(peer.pub_key.clone(), peer.addr)
            .await
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;

        let my_address = peer_conn.my_address().unwrap();

        if self.bridge.op_manager.ring.get_peer_key().is_none() {
            let own_peer_id = PeerId::new(my_address, joiner_key.clone());
            self.bridge.op_manager.ring.set_peer_key(own_peer_id);
        }

        tracing::debug!("Connection established with peer {}", peer.addr.clone());

        // Create a connection request message
        let net_msg = Box::new(NetMessage::Connect(ConnectMsg::Request {
            id,
            msg: ConnectRequest::StartJoinReq {
                joiner: None,
                joiner_key: joiner_key.clone(),
                assigned_location: None,
                hops_to_live,
                max_hops_to_live: hops_to_live,
                skip_list,
            },
        }));

        peer_conn
            .send(net_msg)
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;

        Ok(Right(peer_conn))
    }
}

enum ConnMngrActions {
    /// Received a new connection
    ConnectionEstablished {
        peer: FreenetPeerId,
        peer_conn: PeerConnection,
    },
    ConnectionAccepted {
        peer: PeerKeyLocation,
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

struct PeerConnectionInbound {
    conn: PeerConnection,
    /// Receiver for inbound messages for the peer connection
    rx: Receiver<NetMessage>,
    msg: NetMessage,
}

async fn peer_connection_listener(
    mut rx: mpsc::Receiver<NetMessage>,
    mut conn: PeerConnection,
) -> Result<PeerConnectionInbound, TransportError> {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break Err(TransportError::ConnectionClosed); };
                conn
                    .send(msg)
                    .await?;
            }
            msg = conn.recv() => {
                let msg = msg.unwrap();
                let net_message = decode_msg(&msg).unwrap();
                break Ok(PeerConnectionInbound { conn, rx, msg: net_message });
            }
        }
    }
}

#[inline(always)]
fn encode_msg(msg: NetMessage) -> Result<Vec<u8>, ConnectionError> {
    bincode::serialize(&msg).map_err(|err| ConnectionError::Serialization(Some(err)))
}

#[inline(always)]
fn decode_msg(data: &[u8]) -> Result<NetMessage, ConnectionError> {
    bincode::deserialize(data).map_err(|err| ConnectionError::Serialization(Some(err)))
}
