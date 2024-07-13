use std::net::{IpAddr, SocketAddr};
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

use crate::node::network_bridge::handshake::HandshakeError::ChannelClosed;
use crate::node::network_bridge::handshake::{
    EstablishConnection, Event, HandshakeError, HandshakeHandler, InboundJoinRequest,
    OutboundMessage,
};
use crate::node::PeerId;
use crate::operations::connect::{self, ConnectMsg, ConnectRequest, ConnectResponse};
use crate::transport::{
    create_connection_handler, OutboundConnectionHandler, PeerConnection, TransportError,
    TransportKeypair, TransportPublicKey,
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
                Some(Left(msg)) => Ok(Left((Some(msg), None::<SocketAddr>))),
                Some(Right(action)) => Ok(Right(NodeAction(action))),
            });

            let conn_event = handshake_handler.wait_for_events();

            let bridge_msg = self.conn_bridge_rx.recv().map(|msg| match msg {
                Some(Left((peer, msg))) => {
                    tracing::debug!("Message outbound: {:?}", msg);
                    Ok(Left((Some(*msg), None)))
                }
                Some(Right(action)) => Ok(Right(NodeAction(action))),
                None => Ok(Right(ClosedChannel)),
            });

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
                    let remote_addr = conn.remote_addr();
                    let task = peer_connection_listener(rx, conn).boxed();
                    peer_connections.push(task);
                    Ok(Left((msg, Some(remote_addr))))
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
                Ok(Left((Some(msg), maybe_socket))) => {
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
                Ok(Right(NodeAction(NodeEvent::ConnectPeer { peer, callback }))) => {
                    //TODO: Habria que usar el estabish_connection para enviar el evento al hanshake
                    tracing::info!(remote = %peer, this_peer = ?op_manager.ring.connection_manager.get_peer_key().unwrap(), "Connecting to peer");
                    let mut ob = outbound_conn_handler.clone();
                    let conn_fut = (async move {
                        let c = ob.connect(peer.pub_key.clone(), peer.addr).await;
                        let pc = c.await;
                        if pc.is_ok() {
                            let _ = callback.send(Ok(())).await;
                        } else {
                            let _ = callback.send(Err(())).await;
                        }
                        (pc, peer)
                    })
                    .boxed();
                    // pending_outbound_conns.push(conn_fut);
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
                // Ok(Right(ConnectionEstablished { peer, peer_conn })) => {
                //     let (tx, rx) = mpsc::channel(10);
                //     self.connections.insert(peer.clone(), tx);
                //     self.print_connected_peers(gw_inbound_pending_connections.iter());

                //     // Spawn a task to handle the connection messages (inbound and outbound)
                //     let task = peer_connection_listener(rx, peer_conn).boxed();
                //     peer_connections.push(task);
                // }
                Ok(Right(ClosedChannel)) => {
                    tracing::info!("Notification channel closed");
                    break;
                }
                Ok(Left((None, _))) => {
                    // the conn to the gw has been accepted, we can dismiss this
                    tracing::debug!("Connection accepted by gateway");
                }
                Ok(Right(AcceptConnection)) => {
                    unreachable!()
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
                            // TODO: Need to add the operation to the op_manager properly, in the correct state
                            let (tx, rx) = mpsc::channel(1);
                            self.connections
                                .insert(joiner.expect("should be set at this point"), tx);

                            // Spawn a task to handle the connection messages (inbound and outbound)
                            let task = peer_connection_listener(rx, conn).boxed();
                            peer_connections.push(task);
                        }
                        Event::TransientForwardTransaction(addr, tx) => {
                            if let Some(older_tx) = transient_conn.insert(tx, addr) {
                                todo!("Handle the case when there is an older transaction");
                            }
                        }
                        _ => {
                            //TODO: Implement the rest of the events
                            todo!()
                        }
                    }
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

    async fn establish_connection(
        outbound_conn_handler: &mut OutboundConnectionHandler,
        peer: &PeerId,
    ) -> Result<PeerConnection, ConnectionError> {
        let peer_conn = outbound_conn_handler
            .connect(peer.pub_key.clone(), peer.addr)
            .await
            .await?;
        tracing::debug!("Connection established with peer {}", peer.addr);
        Ok(peer_conn)
    }

    /// Outbound message from bridge handler
    // async fn handle_bridge_connection_message(
    //     &mut self,
    //     peer: PeerId,
    //     mut net_msg: Box<NetMessage>,
    //     establish_connection: &EstablishConnection,
    //     outbound_message: &OutboundMessage
    // ) -> Result<Either<(), (PeerId, PeerConnection)>, ConnectionError> {
    //     let mut connection = None;
    //     let mut closing_gw_conn = None;
    //
    //     tracing::debug!(target_peer = %peer, %net_msg, "Handling bridge connection message");
    //     match &mut *net_msg {
    //         NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
    //             msg:
    //                 connect::ConnectRequest::StartJoinReq {
    //                     joiner,
    //                     joiner_key,
    //                     skip_list,
    //                     ..
    //                 },
    //             ..
    //         })) => {
    //             if !self.connections.contains_key(&peer) {
    //                 // Establish a new connection with the peer
    //                 let peer_conn = establish_connection.establish_conn(peer.clone(), ne).await?;
    //
    //                 // Set the local peer ID using the connection information
    //                 if self
    //                     .bridge
    //                     .op_manager
    //                     .ring
    //                     .connection_manager
    //                     .get_peer_key()
    //                     .is_none()
    //                 {
    //                     tracing::debug!("Setting peer key for the first time");
    //                     let my_address: SocketAddr = peer_conn.my_address().unwrap();
    //                     let own_peer_id = PeerId::new(my_address, joiner_key.clone());
    //                     self.bridge
    //                         .op_manager
    //                         .ring
    //                         .connection_manager
    //                         .set_peer_key(own_peer_id);
    //                 }
    //                 *joiner = self
    //                     .bridge
    //                     .op_manager
    //                     .ring
    //                     .connection_manager
    //                     .get_peer_key();
    //                 *skip_list = self.connections.keys().cloned().collect();
    //                 connection = Some((peer.clone(), peer_conn));
    //             } else {
    //                 tracing::warn!("Connection already exists with gateway {}", peer.addr);
    //                 return Ok(Either::Left(()));
    //             }
    //         }
    //         NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
    //             msg: connect::ConnectRequest::FindOptimalPeer { skip_list, .. },
    //             ..
    //         })) => {
    //             *skip_list = self.connections.keys().cloned().collect();
    //         }
    //         NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
    //             msg:
    //                 ConnectResponse::AcceptedBy {
    //                     accepted,
    //                     acceptor,
    //                     joiner,
    //                 },
    //             ..
    //         })) => {
    //             if acceptor.peer
    //                 == self
    //                     .bridge
    //                     .op_manager
    //                     .ring
    //                     .connection_manager
    //                     .get_peer_key()
    //                     .expect("should be set at this point")
    //             {
    //                 if *accepted {
    //                     // In this case we are the acceptor, we need to establish a connection with the joiner
    //                     // this should only happen for the non-first peers in a Connect request, the first one
    //                     // should already be connected at this point, so check just in case
    //                     if !self.is_gateway && !self.connections.contains_key(&acceptor.peer) {
    //                         let peer_conn =
    //                             Self::establish_connection(outbound_conn_handler, joiner).await?;
    //                         connection = Some((joiner.clone(), peer_conn));
    //                     }
    //                     tracing::debug!(this = %acceptor.peer, %joiner, "Connection accepted");
    //                     if let Some(tx) = pending_inbound_gw_conns.get(&peer.addr) {
    //                         // if this node is a gateway we need to signal that we accepted the connection
    //                         let _ = tx.send(Right(ConnMngrActions::AcceptConnection)).await.map_err(|_| {
    //                             tracing::debug!(remote = %peer.addr, "Couldn't signal to the connection that is accepted, likely dropped");
    //                         });
    //                     }
    //                     pending_inbound_gw_conns.remove(&peer.addr);
    //                 } else {
    //                     closing_gw_conn = pending_inbound_gw_conns.remove(&joiner.addr());
    //                 }
    //             }
    //         }
    //         _ => {}
    //     }
    //
    //     if let Some(tx) = closing_gw_conn
    //         .as_ref()
    //         .map(Some)
    //         .unwrap_or_else(|| pending_inbound_gw_conns.get(&peer.addr()))
    //     {
    //         tx.send(Either::Left(*net_msg))
    //             .await
    //             .map_err(|_| ConnectionError::SendNotCompleted(peer.clone()))?;
    //         if closing_gw_conn.is_some() {
    //             let _ = tx
    //                 .send(Either::Right(ConnMngrActions::ClosedChannel))
    //                 .await
    //                 .map_err(|_| {
    //                     tracing::warn!("failed to send closed action");
    //                 });
    //         }
    //         return Ok(connection.map(Either::Right).unwrap_or(Either::Left(())));
    //     }
    //
    //     if let Some((_, conn)) = &mut connection {
    //         conn.send(net_msg)
    //             .await
    //             .map_err(|_| ConnectionError::SendNotCompleted(peer))?;
    //     } else if let Some(conn) = self.connections.get(&peer) {
    //         tracing::debug!(target = %peer, "Connection status: {}", if conn.is_closed() { "closed" } else { "open" });
    //         conn.send(Either::Left(*net_msg))
    //             .await
    //             .map_err(|_| ConnectionError::SendNotCompleted(peer))?;
    //     } else {
    //         tracing::error!(target = %peer, "Connection likely dropped");
    //         return Err(ConnectionError::SendNotCompleted(peer));
    //     }
    //
    //     Ok(connection.map(Either::Right).unwrap_or(Either::Left(())))
    // }

    #[inline]
    fn print_connected_peers<'a>(
        &self,
        gw_inbound_pending_connections: impl Iterator<Item = &'a SocketAddr>,
    ) {
        tracing::debug!(
            "This peer {:?} has active connections with peers: {:?}",
            self.bridge
                .op_manager
                .ring
                .connection_manager
                .get_peer_key()
                .as_ref(),
            self.connections.keys().map(|p| &p.addr).collect::<Vec<_>>()
        );
        if self.is_gateway {
            tracing::debug!(
                "This gateway {:?} has pending connections with peers: {:?}",
                self.bridge
                    .op_manager
                    .ring
                    .connection_manager
                    .get_peer_key()
                    .as_ref(),
                gw_inbound_pending_connections.collect::<Vec<_>>()
            );
        }
    }
}

enum ConnMngrActions {
    HandShakeAction(Event),
    /// Accept connection
    AcceptConnection,
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
    msg: Option<NetMessage>,
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
                            ConnMngrActions::AcceptConnection => {
                                return Ok(PeerConnectionInbound { conn, rx, msg: None });
                            }
                            _ => {}
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
                break Ok(PeerConnectionInbound { conn, rx, msg: Some(net_message) });
            }
        }
    }
}

#[inline(always)]
fn decode_msg(data: &[u8]) -> Result<NetMessage, ConnectionError> {
    bincode::deserialize(data).map_err(|err| ConnectionError::Serialization(Some(err)))
}
