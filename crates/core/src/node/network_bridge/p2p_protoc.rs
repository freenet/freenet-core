use std::net::{IpAddr, SocketAddr};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::anyhow;
use dashmap::{DashMap, DashSet};
use either::{Either, Left, Right};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::Instrument;

use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::message::{ConnectionResult, NetMessageV1};

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
    message::{MessageStats, NetMessage, NodeEvent, Transaction},
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
    /// This map only matters for gateways which are forwarding messages
    /// for joiners, we only maintain a connection as long as that peer hasn't
    /// been connected to an other peer, if the gw doesn't want the connection
    /// it will then be dropped.
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
    listening_addr: Option<SocketAddr>,
    event_listener: Box<dyn NetEventRegister>,
    connection: HashMap<PeerId, PeerConnChannel>,
    rejected_peers: HashSet<PeerId>, // TODO: what's the point of this set?
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
        private_key: TransportKeypair,
    ) -> Result<Self, anyhow::Error> {
        let listen_port = config
            .local_port
            .ok_or_else(|| anyhow::anyhow!("network listener port does not contain a port"))?;

        let listener_ip = config
            .local_ip
            .ok_or(anyhow!("network listener IP not set"))?;

        let private_addr = if let Some(conn) = config.local_ip.zip(config.local_port) {
            let addr = SocketAddr::from(conn);
            Some(addr)
        } else {
            None
        };

        let (tx_bridge_cmd, rx_bridge_cmd) = mpsc::channel(100);
        let bridge = P2pBridge::new(tx_bridge_cmd, op_manager, event_listener.clone());

        let gateways = config.get_gateways()?;
        Ok(P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            listening_addr: private_addr,
            event_listener: Box::new(event_listener),
            connection: HashMap::new(),
            rejected_peers: HashSet::new(),
            key_pair: private_key,
            listening_ip: listener_ip,
            listening_port: listen_port,
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
                self.key_pair.clone(),
                self.listening_ip,
                self.listening_port,
                self.is_gateway,
            )
            .await?;

        // FIXME: this two containers need to be clean up on transaction time-out
        let mut pending_from_executor = HashSet::new();
        let mut tx_to_client: HashMap<Transaction, ClientId> = HashMap::new();

        let mut peer_connections = FuturesUnordered::new();
        let mut outbound_conn_handler_2 = outbound_conn_handler.clone();
        let mut pending_outbound_conns = FuturesUnordered::new();
        let mut pending_inbound_gw_conns = HashMap::new();

        loop {
            let notification_msg = notification_channel.0.recv().map(|m| match m {
                None => Ok(Right(ClosedChannel)),
                Some(Left(msg)) => Ok(Left((msg, None))),
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
                // TODO: add a separate code path for gateways for handling connection liveness
                // we must ensure that the peer stays connected long enough so an other peer has forwarded
                // or accepted a connection
                match inbound_conn_handler.next_connection().await {
                    Some(peer_conn) => Ok(Right(GatewayConnection {
                        remote_addr: peer_conn.remote_addr(),
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
                    let remote_addr = conn.remote_addr();
                    let task = peer_connection_listener(rx, conn).boxed();
                    peer_connections.push(task);
                    Ok(Left((msg, Some(remote_addr))))
                }
                msg = pending_outbound_conns.next(), if !pending_outbound_conns.is_empty() => {
                    match msg {
                        Some((peer_conn, peer_id)) => {
                            match peer_conn {
                                Ok(peer_conn) => Ok(Right(ConnectionEstablished {
                                    peer: peer_id,
                                    peer_conn,
                                })),
                                Err(err) => {
                                    tracing::error!(remote = %peer_id, "Error while attempting connection: {err}");
                                    // in this case although theoretically the Connect request would have added a new connection
                                    // it failed, so we must free a spot reserved for this connection
                                    op_manager.ring.prune_connection(peer_id.clone()).await;
                                    continue;
                                }
                            }
                        },
                        None => {
                            tracing::error!("All outbound peer connections closed");
                            continue;
                        }
                    }
                }
                msg = notification_msg => { msg }
                msg = bridge_msg => {
                    match msg? {
                        Left((peer, peer_conn)) => {
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
                // This are either inbound messages or fast tracked by the event notifier
                Ok(Left((msg, maybe_socket))) => {
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
                        mut msg => {
                            if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                                msg:
                                    ConnectResponse::AcceptedBy {
                                        accepted,
                                        acceptor,
                                        joiner,
                                    },
                                ..
                            })) = &msg
                            {
                                // this is the inbound response from the peer we want to connect to
                                // for the other peer returning this response, check handle_bridge_connection_message
                                let this_peer_id = op_manager
                                    .ring
                                    .set_peer_key(joiner.clone())
                                    .unwrap_or_else(|| joiner.clone());
                                if *accepted && &this_peer_id == joiner {
                                    tracing::debug!(remote = %acceptor.peer, "Connection accepted by target, attempting connection");
                                    // we need to start the process of connecting to the other peer
                                    let remote_peer = acceptor.peer.clone();
                                    let conn_fut = outbound_conn_handler_2
                                        .connect(acceptor.peer.pub_key.clone(), acceptor.peer.addr)
                                        .await
                                        .map(|peer_conn| (peer_conn, remote_peer));
                                    pending_outbound_conns.push(conn_fut);
                                }
                            }

                            if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                                msg:
                                    ConnectRequest::StartJoinReq {
                                        joiner,
                                        joiner_key,
                                        hops_to_live,
                                        max_hops_to_live,
                                        ..
                                    },
                                ..
                            })) = &mut msg
                            {
                                // this is a gateway forwarding a connection request
                                // in this case a real connection already exists and we just need to maintain it alive
                                // long enough so the request is forwarded
                                // this should be the first hop:
                                debug_assert_eq!(hops_to_live, max_hops_to_live);
                                // at this point the joiner is probably not yet set, so we need to update it
                                if let Some(remote_addr) = maybe_socket {
                                    if let Some(peer_conn) =
                                        pending_inbound_gw_conns.remove(&remote_addr)
                                    {
                                        let peer_id = PeerId::new(remote_addr, joiner_key.clone());

                                        tracing::debug!(
                                            %remote_addr,
                                            "Established connection with peer",
                                        );
                                        self.bridge
                                            .active_net_connections
                                            .insert(peer_id.clone(), remote_addr);

                                        let (tx, rx) = mpsc::channel(10);
                                        self.connection.insert(peer_id.clone(), tx);

                                        // Spawn a task to handle the connection messages (inbound and outbound)
                                        let task = peer_connection_listener(rx, peer_conn).boxed();
                                        peer_connections.push(task);
                                    } else if joiner.is_none() {
                                        tracing::error!(
                                            "Joiner unexpectedly not set for connection request."
                                        );
                                        continue;
                                    }
                                }
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
                Ok(Right(NodeAction(NodeEvent::ShutdownNode))) => {
                    tracing::info!("Shutting down message loop gracefully");
                    break;
                }
                Ok(Right(NodeAction(NodeEvent::Error(err)))) => {
                    tracing::error!("Bridge conn error: {err}");
                }
                Ok(Right(NodeAction(NodeEvent::Disconnect { cause }))) => {
                    match cause {
                        Some(cause) => tracing::warn!("Shutting down node: {cause}"),
                        None => tracing::warn!("Shutting down node"),
                    }
                    return Ok(());
                }
                Ok(Right(NodeAction(NodeEvent::DropConnection(peer_id)))) => {
                    self.bridge.active_net_connections.remove(&peer_id);
                    op_manager.ring.prune_connection(peer_id.clone()).await;
                    self.connection.remove(&peer_id);
                    self.rejected_peers.insert(peer_id.clone());
                    tracing::info!("Dropped connection with peer {}", peer_id);
                }
                Ok(Right(GatewayConnection {
                    remote_addr,
                    peer_conn,
                })) => {
                    pending_inbound_gw_conns.insert(remote_addr, peer_conn);
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
                Ok(Right(NoAction)) => {}
                Ok(Right(NodeAction(NodeEvent::AcceptConnection(_, _)))) => {
                    todo!("is this even relevant, fixme")
                }
            }
        }
        Ok(())
    }

    /// Outbound message from bridge handler
    async fn handle_bridge_connection_message(
        &mut self,
        peer: PeerId,
        net_msg: Box<NetMessage>,
        outbound_conn_handler: &mut OutboundConnectionHandler,
    ) -> Result<Either<(), (PeerId, PeerConnection)>, ConnectionError> {
        let mut connection = None;
        if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
            msg:
                ConnectResponse::AcceptedBy {
                    accepted: true,
                    acceptor,
                    joiner,
                },
            ..
        })) = &*net_msg
        {
            if acceptor.peer
                == self
                    .bridge
                    .op_manager
                    .ring
                    .get_peer_key()
                    .expect("should be set at this point")
            {
                // In this case we are the acceptor, we need to establish a connection with the joiner
                // this should only happen for the non-first peers in a Connect request, the first one
                // should already be connected at this point, so check just in case
                if !self.connection.contains_key(&acceptor.peer) {
                    let peer_conn = outbound_conn_handler
                        .connect(joiner.pub_key.clone(), joiner.addr)
                        .await
                        .await?;
                    // let my_address = peer_conn.my_address().unwrap();
                    // if self.bridge.op_manager.ring.get_peer_key().is_none() {
                    //     let own_peer_id = PeerId::new(my_address, joiner.pub_key.clone());
                    //     self.bridge.op_manager.ring.set_peer_key(own_peer_id);
                    // }
                    tracing::debug!("Connection established with peer {}", joiner.addr.clone());
                    connection = Some((peer.clone(), peer_conn));
                }
            }
        }

        if let Some(conn) = self.connection.get(&peer) {
            conn.send(*net_msg)
                .await
                .map_err(|_| ConnectionError::SendNotCompleted)?;
            Ok(connection
                .map(|conn| Either::Right(conn))
                .unwrap_or(Either::Left(())))
        } else {
            tracing::debug!("Connection likely dropped");
            Err(ConnectionError::SendNotCompleted)
        }
    }
}

enum ConnMngrActions {
    /// Gateway connection
    GatewayConnection {
        remote_addr: SocketAddr,
        peer_conn: PeerConnection,
    },
    /// Received a new connection
    ConnectionEstablished {
        peer: FreenetPeerId,
        peer_conn: PeerConnection,
    },
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
