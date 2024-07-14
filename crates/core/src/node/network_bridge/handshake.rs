//! Handles initial connection handshake.
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::time::{timeout, Duration};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::mpsc::{self};

use crate::{
    dev_tool::{Location, PeerId, Transaction},
    message::{InnerMessage, NetMessage, NetMessageV1},
    node::NetworkBridge,
    operations::connect::{
        forward_conn, ConnectMsg, ConnectOp, ConnectRequest, ConnectResponse, ConnectState,
    },
    ring::{ConnectionManager, PeerKeyLocation},
    router::Router,
    transport::{
        InboundConnectionHandler, OutboundConnectionHandler, PeerConnection, TransportError,
        TransportPublicKey,
    },
};

type Result<T, E = HandshakeError> = std::result::Result<T, E>;
type OutboundConnResult = Result<InternalEvent, (PeerId, HandshakeError)>;

#[derive(Debug, thiserror::Error)]
pub(super) enum HandshakeError {
    #[error("channel closed")]
    ChannelClosed,
    #[error("connection closed to {0}")]
    ConnectionClosed(SocketAddr),
    #[error(transparent)]
    Serialization(#[from] Box<bincode::ErrorKind>),
    #[error(transparent)]
    TransportError(#[from] TransportError),
}

#[cfg_attr(test, derive(Debug))]
pub(super) enum Event {
    /// An inbound connection to a peer was successfully established at a gateway.
    InboundConnection(InboundJoinRequest),
    /// An outbound connection to a peer was successfully established.
    OutboundConnectionSuccessful {
        peer_id: PeerId,
        connection: PeerConnection,
    },
    /// An outbound connection to a peer failed to be established.
    OutboundConnectionFailed {
        peer_id: PeerId,
        error: HandshakeError,
    },
    /// An outbound connection to a gateway was rejected.
    OutboundConnectionRejected {
        peer_id: PeerId,
        /// The ongoing connection operation state, to keep track of the forwarded connection attempts.
        connection: ConnectOp,
    },
    /// Message relayed by a gateway with a transient connection from a third party.
    OutboundGatewayRelayMessage {
        peer_id: PeerId,
        message: ConnectMsg,
    },
    /// An outbound connection to a gateway was successfully established. It can be managed by the connection manager.
    OutboundGatewayConnectionSuccessful {
        peer_id: PeerId,
        connection: PeerConnection,
        op: ConnectMsg,
    },
    /// Clean up a transaction that was completed or duplicate.
    RemoveTransaction(Transaction),
    /// Wait for replies via an other peer from forwarded connection attempts.
    TransientForwardTransaction(SocketAddr, Transaction),
}

/// Use for sending messages to a peer which has not yet been confirmed at a logical level
/// or is just a transient connection (e.g. in case of gateways just forwarding messages).
pub(super) struct OutboundMessage(mpsc::Sender<(SocketAddr, NetMessage)>);

impl OutboundMessage {
    pub async fn send_to(&self, remote: SocketAddr, msg: NetMessage) -> Result<()> {
        self.0
            .send((remote, msg))
            .await
            .map_err(|_| HandshakeError::ChannelClosed)?;
        Ok(())
    }
}

/// Use for starting a new outboound connection to a peer.
pub(super) struct EstablishConnection(mpsc::Sender<(PeerId, Transaction)>);

impl EstablishConnection {
    pub async fn establish_conn(&self, remote: PeerId, tx: Transaction) -> Result<()> {
        self.0
            .send((remote, tx))
            .await
            .map_err(|_| HandshakeError::ChannelClosed)?;
        Ok(())
    }
}

type OutboundMessageSender = mpsc::Sender<NetMessage>;
type OutboundMessageReceiver = mpsc::Receiver<(SocketAddr, NetMessage)>;
type EstablishConnectionReceiver = mpsc::Receiver<(PeerId, Transaction)>;


/// Manages the handshake process for establishing connections with peers.
/// Handles both inbound and outbound connection attempts, and manages
/// the transition from unconfirmed to confirmed connections.
pub(super) struct HandshakeHandler {
    /// Tracks ongoing connection attempts by their remote socket address
    connecting: HashMap<SocketAddr, Transaction>,

    /// Set of socket addresses for established connections
    connected: HashSet<SocketAddr>,

    /// Handles incoming connections from the network
    inbound_conn_handler: InboundConnectionHandler,

    /// Initiates outgoing connections to remote peers
    outbound_conn_handler: OutboundConnectionHandler,

    /// Queue of ongoing outbound connection attempts
    /// Used for non-gateway peers initiating connections
    ongoing_outbound_connections: FuturesUnordered<BoxFuture<'static, OutboundConnResult>>,

    /// Queue of inbound connections not yet confirmed at the logical level
    /// Used primarily by gateways for handling new peer join requests
    unconfirmed_inbound_connections: FuturesUnordered<
        BoxFuture<'static, Result<(InternalEvent, PeerOutboundMessage), HandshakeError>>,
    >,

    /// Mapping of socket addresses to channels for sending messages to peers
    /// Used for both confirmed and unconfirmed connections
    outbound_messages: HashMap<SocketAddr, OutboundMessageSender>,

    /// Receiver for messages to be sent to peers not yet confirmed
    /// Part of the OutboundMessage public API
    pending_msg_rx: OutboundMessageReceiver,

    /// Queues messages for peers that are not yet connected
    queues: HashMap<SocketAddr, Vec<NetMessage>>,

    /// Receiver for commands to establish new outbound connections
    /// Part of the EstablishConnection public API
    establish_connection_rx: EstablishConnectionReceiver,

    /// Manages the node's connections and topology
    connection_manager: ConnectionManager,

    /// Handles routing decisions within the network
    router: Arc<RwLock<Router>>,
}

impl HandshakeHandler {
    pub fn new(
        inbound_conn_handler: InboundConnectionHandler,
        outbound_conn_handler: OutboundConnectionHandler,
        connection_manager: ConnectionManager,
        router: Arc<RwLock<Router>>,
    ) -> (Self, EstablishConnection, OutboundMessage) {
        let (pending_msg_tx, pending_msg_rx) = tokio::sync::mpsc::channel(100);
        let (establish_connection_tx, establish_connection_rx) = tokio::sync::mpsc::channel(100);
        let connector = HandshakeHandler {
            connecting: HashMap::new(),
            connected: HashSet::new(),
            inbound_conn_handler,
            outbound_conn_handler,
            ongoing_outbound_connections: FuturesUnordered::new(),
            unconfirmed_inbound_connections: FuturesUnordered::new(),
            outbound_messages: HashMap::new(),
            pending_msg_rx,
            queues: HashMap::new(),
            establish_connection_rx,
            connection_manager,
            router,
        };
        (
            connector,
            EstablishConnection(establish_connection_tx),
            OutboundMessage(pending_msg_tx),
        )
    }

    /// Processes events related to connection establishment and management.
    /// This is the main event loop for the HandshakeHandler.
    pub async fn wait_for_events(&mut self) -> Result<Event, HandshakeError> {
        loop {
            tokio::select! {
                // Handle new inbound connections
                new_conn = self.inbound_conn_handler.next_connection() => {
                    let Some(conn) = new_conn else {
                        return Err(HandshakeError::ChannelClosed);
                    };
                    tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "New inbound connection");
                    self.track_inbound_connection(conn);
                }
                // Process outbound connection attempts
                outbound_conn = self.ongoing_outbound_connections.next(), if !self.ongoing_outbound_connections.is_empty() => {
                    let r = match outbound_conn {
                        Some(Ok(InternalEvent::OutboundConnEstablished(id, connection))) => {
                            tracing::debug!(at=?connection.my_address(), from=%connection.remote_addr(), "Outbound connection successful");
                            self.wait_for_gw_confirmation(id, connection).await;
                            continue;
                        }
                        Some(Err((peer_id, error))) => {
                            tracing::debug!(from=%peer_id.addr, "Outbound connection failed: {error}");
                            Ok(Event::OutboundConnectionFailed { peer_id, error: error.into() })
                        }
                        None => Err(HandshakeError::ChannelClosed),
                        _ => unreachable!()
                    };
                    break r;
                }
                // Handle unconfirmed inbound connections (mainly for gateways)
                unconfirmed_inbound_conn = self.unconfirmed_inbound_connections.next(), if !self.unconfirmed_inbound_connections.is_empty() => {
                    let Some(res) = unconfirmed_inbound_conn else {
                        return Err(HandshakeError::ChannelClosed);
                    };
                    let (event, outbound_sender) = res?;
                    match event {
                        InternalEvent::InboundJoinRequest(req) => {
                            let remote = req.conn.remote_addr();
                            let location = Location::from_address(&remote);
                            let peer_id = req.joiner.clone().unwrap_or_else(|| PeerId::new(remote, req.joiner_key.clone()));
                            let should_accept = self.connection_manager.should_accept(location, Some(&peer_id));
                            if should_accept {
                                tracing::debug!(at=?req.conn.my_address(), from=%req.conn.remote_addr(), "Accepting connection");
                                return Ok(Event::InboundConnection(req));
                            } else {
                                let InboundJoinRequest { conn, id, hops_to_live, max_hops_to_live, skip_list, .. } = req;
                                let remote = conn.remote_addr();
                                tracing::debug!(at=?conn.my_address(), from=%remote, "Transient connection");
                                self.unconfirmed_inbound_connections.push(
                                    gw_transient_peer_conn(
                                        conn,
                                        outbound_sender,
                                        TransientConnection {
                                            tx: id,
                                            joiner: peer_id,
                                            max_hops_to_live,
                                            hops_to_live,
                                            skip_list,
                                        },
                                        self.connection_manager.clone(),
                                        self.router.clone()
                                    ).boxed()
                                );
                                return Ok(Event::TransientForwardTransaction(remote, id));
                            }
                        }
                        InternalEvent::DropInboundConnection(addr) => {
                            self.outbound_messages.remove(&addr);
                            self.queues.remove(&addr);
                            self.connecting.remove(&addr);
                            continue;
                        }
                        InternalEvent::OutboundConnEstablished(peer_id, connection) => {
                            tracing::debug!(at=?connection.my_address(), from=%connection.remote_addr(), "Outbound connection successful");
                            return Ok(Event::OutboundConnectionSuccessful { peer_id, connection });
                        }
                    }
                }
                // Process pending messages for unconfirmed connections
                pending_msg = self.pending_msg_rx.recv() => {
                    let Some((addr, msg)) = pending_msg else {
                        return Err(HandshakeError::ChannelClosed);
                    };
                    if let Some(event) = self.outbound(addr, msg).await {
                        break Ok(event);
                    }
                }
                // Handle requests to establish new connections
                establish_connection = self.establish_connection_rx.recv() => {
                    let Some((peer_id, tx)) = establish_connection else {
                        return Err(HandshakeError::ChannelClosed);
                    };
                    self.start_outbound_connection(peer_id, tx).await;
                }
            }
        }
    }

    /// Tracks a new inbound connection and sets up message handling for it.
    fn track_inbound_connection(&mut self, conn: PeerConnection) {
        let (outbound_msg_sender, outbound_msg_recv) = mpsc::channel(1);
        let remote = conn.remote_addr();
        let f = gw_peer_connection_listener(conn, PeerOutboundMessage(outbound_msg_recv)).boxed();
        self.unconfirmed_inbound_connections.push(f);
        self.outbound_messages.insert(remote, outbound_msg_sender);
    }

    /// Handles outbound messages to peers, including queueing for disconnected peers.
    async fn outbound(&mut self, addr: SocketAddr, op: NetMessage) -> Option<Event> {
        if let Some(alive_conn) = self.outbound_messages.get_mut(&addr) {
            match &op {
                NetMessage::V1(NetMessageV1::Connect(op)) => {
                    // TODO: check what the exact message is to track state of the connection and what we should do with it

                    let tx = *op.id();
                    if self
                        .connecting
                        .get(&addr)
                        .filter(|current_tx| *current_tx != &tx)
                        .is_some()
                    {
                        // avoid duplicate connection attempts
                        tracing::warn!("Duplicate connection attempt to {addr}, ignoring");
                        return Some(Event::RemoveTransaction(tx));
                    }
                    self.connecting.insert(addr, tx);
                }
                _ => {}
            }

            if alive_conn.send(op).await.is_err() {
                self.queues.remove(&addr);
                self.outbound_messages.remove(&addr);
                self.connecting.remove(&addr);
            }
            None
        } else {
            let mut send_to_remote = None;
            match &op {
                NetMessage::V1(NetMessageV1::Connect(op)) => {
                    match op {
                        ConnectMsg::Response {
                            msg: ConnectResponse::AcceptedBy { joiner, .. },
                            ..
                        } => {
                            // this may be a reply message from a downstream peer to which it was forwarded previously
                            // for a transient connection, in this case we must send this message to the proper
                            // gw_transient_peer_conn future that is waiting for it
                            send_to_remote = Some(joiner.addr);
                        }
                        _ => {}
                    }
                }
                _ => {}
            }

            if let Some(remote) = send_to_remote {
                if let Some(addr) = self.outbound_messages.get_mut(&remote) {
                    if addr.send(op).await.is_err() {
                        tracing::warn!("Failed to send message to {addr}", addr = remote);
                    }
                } else {
                    // this shouldn't happen really
                    tracing::error!("No outbound message sender for {addr}", addr = remote);
                };
                return None;
            }

            // if is a message to a peer which is not yet connected, just queue it
            tracing::debug!("Queueing message to {addr}", addr = addr);
            self.queues.entry(addr).or_default().push(op);
            None
        }
    }

    /// Starts an outbound connection to the given peer.
    async fn start_outbound_connection(&mut self, remote: PeerId, transaction: Transaction) {
        if self.connected.contains(&remote.addr) {
            tracing::warn!(
                "Already connected to {}, ignore connection attempt",
                remote.addr
            );
            return;
        }
        self.connecting.insert(remote.addr, transaction);
        tracing::debug!("Starting outbound connection to {addr}", addr = remote.addr);
        let f = self
            .outbound_conn_handler
            .connect(remote.pub_key.clone(), remote.addr)
            .await
            .map(move |c| match c {
                Ok(conn) => Ok(InternalEvent::OutboundConnEstablished(remote, conn)),
                Err(e) => Err((remote, e.into())),
            })
            .boxed();
        self.ongoing_outbound_connections.push(f);
    }

    /// Waits for confirmation from a gateway after establishing a connection.
    async fn wait_for_gw_confirmation(&mut self, peer_id: PeerId, conn: PeerConnection) {
        self.ongoing_outbound_connections.push(
            wait_for_gw_confirmation(peer_id, conn, self.connection_manager.max_hops_to_live)
                .boxed(),
        );
    }
}


/// Waits for confirmation from a gateway after initiating a connection.
async fn wait_for_gw_confirmation(
    peer_id: PeerId,
    mut conn: PeerConnection,
    max_hops_to_live: usize,
) -> OutboundConnResult {
    let tx = Transaction::new::<ConnectMsg>();
    let msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
        id: tx,
        msg: ConnectRequest::StartJoinReq {
            joiner: None,
            joiner_key: peer_id.pub_key.clone(),
            hops_to_live: max_hops_to_live,
            max_hops_to_live,
            skip_list: vec![],
        },
    }));
    tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Sending initial connection message to gw");
    conn.send(msg)
        .await
        .map_err(|err| (peer_id.clone(), HandshakeError::TransportError(err)))?;
    tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Waiting for answer from gw");
    let msg = conn
        .recv()
        .await
        .map_err(|err| (peer_id, HandshakeError::TransportError(err)))?;
    tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Received answer from gw");
    todo!()
}

#[cfg_attr(test, derive(Debug))]
pub(super) struct InboundJoinRequest {
    pub conn: PeerConnection,
    pub id: Transaction,
    pub joiner: Option<PeerId>,
    pub joiner_key: TransportPublicKey,
    pub hops_to_live: usize,
    pub max_hops_to_live: usize,
    pub skip_list: Vec<PeerId>,
}

enum InternalEvent {
    InboundJoinRequest(InboundJoinRequest),
    OutboundConnEstablished(PeerId, PeerConnection),
    DropInboundConnection(SocketAddr),
}

#[repr(transparent)]
struct PeerOutboundMessage(mpsc::Receiver<NetMessage>);

/// Handles communication with a potentially transient peer connection.
/// Used primarily by gateways to manage connections in the process of joining the network.
async fn gw_peer_connection_listener(
    mut conn: PeerConnection,
    mut outbound: PeerOutboundMessage,
) -> Result<(InternalEvent, PeerOutboundMessage), HandshakeError> {
    loop {
        tokio::select! {
            msg = outbound.0.recv() => {
                let Some(msg) = msg else { break Err(HandshakeError::ConnectionClosed(conn.remote_addr())); };

                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr() ,"Sending message to peer. Msg: {msg}");
                        conn
                            .send(msg)
                            .await?;
            }
            msg = conn.recv() => {
                let Ok(msg) = msg.map_err(|error| {
                    tracing::error!(at=?conn.my_address(), from=%conn.remote_addr(), "Error while receiving message: {error}");
                }) else {
                     break Err(HandshakeError::ConnectionClosed(conn.remote_addr()));
                };
                let net_message = decode_msg(&msg).unwrap();
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), %net_message, "Received message from peer");
                match net_message {
                    NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                        id,
                        msg: ConnectRequest::StartJoinReq { joiner, joiner_key, hops_to_live, max_hops_to_live, skip_list }
                    })) => {
                        break Ok((
                            InternalEvent::InboundJoinRequest(
                                InboundJoinRequest {
                                    conn, id, joiner, joiner_key, hops_to_live, max_hops_to_live, skip_list
                                }
                            ),
                            outbound
                        ));
                    }
                    other =>  {
                        tracing::warn!(
                            at=?conn.my_address(),
                            from=%conn.remote_addr(),
                            %other,
                            "Unexpected message received from peer, terminating connection"
                        );
                        break Err(HandshakeError::ConnectionClosed(conn.remote_addr()));
                    }
                }
            }
        }
    }
}

/// Manages a transient connection during the joining process.
/// Handles forwarding of connection requests and tracking of responses.
async fn gw_transient_peer_conn(
    mut conn: PeerConnection,
    mut outbound: PeerOutboundMessage,
    transaction: TransientConnection,
    conn_manager: ConnectionManager,
    router: Arc<RwLock<Router>>,
) -> Result<(InternalEvent, PeerOutboundMessage), HandshakeError> {
    // TODO: should be the same timeout as the one used for any other tx
    let timeout_duration = Duration::from_secs(10);

    // Attempt forwarding the connection request to the next hop and wait for answers
    // then return those answers to the transitory peer connection.

    // TODO: this type should be capturing the messages to be forwarded to other peers and communicate
    // that back to the network event loop via Events
    struct ForwardPeerMessage;

    impl NetworkBridge for ForwardPeerMessage {
        async fn send(&self, target: &PeerId, msg: NetMessage) -> super::ConnResult<()> {
            Ok(())
        }

        async fn drop_connection(&mut self, peer: &PeerId) -> super::ConnResult<()> {
            Ok(())
        }
    }

    // TODO: after forwarding track the messagesfrom those peers and send back to joiner

    let mut nw_bridge = ForwardPeerMessage;

    let joiner_loc = Location::from_address(&conn.remote_addr());
    let joiner_pk_loc = PeerKeyLocation {
        peer: transaction.joiner.clone(),
        location: Some(joiner_loc),
    };
    let my_peer_id = conn_manager.own_location();
    let Ok(Some(conn_state)) = forward_conn(
        transaction.tx,
        &conn_manager,
        router.clone(),
        &mut nw_bridge,
        (my_peer_id, joiner_pk_loc),
        transaction.hops_to_live,
        transaction.hops_to_live,
        false,
        vec![transaction.joiner.clone()],
    )
    .await
    else {
        return Err(HandshakeError::ConnectionClosed(conn.remote_addr()));
    };
    let ConnectState::AwaitingConnectivity(mut info) = conn_state else {
        unreachable!()
    };

    loop {
        tokio::select! {
            incoming_result = timeout(timeout_duration, conn.recv()) => {
                match incoming_result {
                    Ok(Ok(msg)) => {
                        let net_msg = decode_msg(&msg).unwrap();
                        if transaction.is_drop_connection_message(&net_msg) {
                            tracing::debug!("Received drop connection message");
                            break Ok((InternalEvent::DropInboundConnection(conn.remote_addr()), outbound));
                        } else {
                            tracing::warn!(
                                at=?conn.my_address(),
                                from=%conn.remote_addr(),
                                %net_msg,
                                "Unexpected message received from peer, terminating connection"
                            );
                            break Err(HandshakeError::ConnectionClosed(conn.remote_addr()));
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Error receiving message: {:?}", e);
                        break Ok((InternalEvent::DropInboundConnection(conn.remote_addr()), outbound));
                    }
                    Err(_) => {
                        tracing::debug!("Transient connection timed out");
                        break Ok((InternalEvent::DropInboundConnection(conn.remote_addr()), outbound));
                    }
                }
            }
            outbound_msg = timeout(timeout_duration, outbound.0.recv()) => {
                match outbound_msg {
                    Ok(Some(msg)) => {
                        if matches!(
                            msg,
                            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response { msg: ConnectResponse::AcceptedBy { .. }, .. }))
                        ) {
                            let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                                id,
                                target,
                                msg: ConnectResponse::AcceptedBy { accepted, acceptor, joiner },
                                ..
                            })) = msg else {
                                unreachable!()
                            };
                            // TODO: in this case it may be a reply of a third party we forwarded to,
                            // and need to send that back to the joiner and count the reply
                            let msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                                id,
                                sender: target,
                                target: acceptor.clone(),
                                msg: ConnectResponse::AcceptedBy {
                                    accepted,
                                    acceptor,
                                    joiner,
                                },
                            }));
                            conn.send(msg).await?;
                            if info.decrement_check() {
                                break Ok((InternalEvent::DropInboundConnection(conn.remote_addr()), outbound));
                            } else {
                                continue;
                            }
                        }
                        conn.send(msg).await?;
                    }
                    Ok(None) => {
                        tracing::debug!("Outbound channel closed");
                        break Ok((InternalEvent::DropInboundConnection(conn.remote_addr()), outbound));
                    }
                    Err(_) => {
                        tracing::debug!("Transient connection timed out");
                        break Ok((InternalEvent::DropInboundConnection(conn.remote_addr()), outbound));
                    }
                }
            }
        }
    }
}

struct TransientConnection {
    tx: Transaction,
    joiner: PeerId,
    max_hops_to_live: usize,
    hops_to_live: usize,
    skip_list: Vec<PeerId>,
}

impl TransientConnection {
    fn is_drop_connection_message(&self, net_message: &NetMessage) -> bool {
        if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
            id,
            msg: ConnectRequest::CleanConnection { joiner },
        })) = net_message
        {
            // this peer should never be receiving messages for other transactions or other peers at this point
            debug_assert_eq!(id, &self.tx);
            debug_assert_eq!(joiner.peer, self.joiner);

            if id != &self.tx || joiner.peer != self.joiner {
                return false;
            }
            return true;
        }
        false
    }
}

#[inline(always)]
fn decode_msg(data: &[u8]) -> Result<NetMessage> {
    bincode::deserialize(data).map_err(|err| HandshakeError::Serialization(err))
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use aes_gcm::{Aes128Gcm, KeyInit};
    use anyhow::{anyhow, bail};
    use serde::Serialize;
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::{
        dev_tool::TransportKeypair,
        operations::connect::{ConnectMsg, ConnectResponse},
        ring::PeerKeyLocation,
        transport::{
            ConnectionEvent, OutboundConnectionHandler, PacketData, RemoteConnection,
            SymmetricMessage, SymmetricMessagePayload, UnknownEncryption,
        },
    };

    struct TransportMock {
        inbound_sender: mpsc::Sender<PeerConnection>,
        outbound_recv: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
        packet_senders:
            HashMap<SocketAddr, (Aes128Gcm, mpsc::Sender<PacketData<UnknownEncryption>>)>,
        packet_receivers: Vec<mpsc::Receiver<(SocketAddr, Arc<[u8]>)>>,
        in_key: Aes128Gcm,
    }

    impl TransportMock {
        async fn new_conn(&mut self, addr: SocketAddr) {
            let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
            let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
            let (conn, packet_sender) =
                PeerConnection::new_test(addr, out_symm_key, in_symm_key.clone());
            self.inbound_sender.send(conn).await.unwrap();
            tracing::debug!("New inbound connection established");
            self.packet_senders
                .insert(addr, (in_symm_key, packet_sender));
        }

        async fn new_outbound_conn(
            &mut self,
            addr: SocketAddr,
            callback: oneshot::Sender<Result<crate::transport::RemoteConnection, TransportError>>,
        ) {
            let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
            let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
            let (conn, packet_sender, packet_recv) =
                PeerConnection::new_remote_test(addr, out_symm_key, in_symm_key.clone());
            callback
                .send(Ok(conn))
                .map_err(|_| "Failed to send connection")
                .unwrap();
            tracing::debug!("New outbound connection established");
            self.packet_senders
                .insert(addr, (in_symm_key, packet_sender));
            self.packet_receivers.push(packet_recv);
        }

        /// This would happen when a new unsolicited connection is established with a gateway or
        /// when after initialising a connection with a peer via `outbound_recv`, a connection
        /// is successfully established.
        async fn establish_inbound_conn(&mut self, addr: SocketAddr) {
            let id = Transaction::new::<ConnectMsg>();
            let joiner_key = TransportKeypair::new();
            let pub_key = joiner_key.public().clone();
            let initial_join_req = ConnectMsg::Request {
                id,
                msg: ConnectRequest::StartJoinReq {
                    joiner: None,
                    joiner_key: pub_key,
                    hops_to_live: 10,
                    max_hops_to_live: 10,
                    skip_list: vec![],
                },
            };
            self.inbound_msg(
                addr,
                NetMessage::V1(NetMessageV1::Connect(initial_join_req)),
            )
            .await
        }

        /// A peer established a connection successfully with a gateway.
        async fn establish_outbound_conn(&mut self, addr: SocketAddr) -> Transaction {
            let id = Transaction::new::<ConnectMsg>();
            let joiner_key = TransportKeypair::new();
            let pub_key = joiner_key.public().clone();
            let initial_join_req = ConnectMsg::Request {
                id,
                msg: ConnectRequest::StartJoinReq {
                    joiner: None,
                    joiner_key: pub_key,
                    hops_to_live: 10,
                    max_hops_to_live: 10,
                    skip_list: vec![],
                },
            };
            tracing::debug!("Sending initial connection message");
            self.inbound_msg(addr, NetMessageV1::Connect(initial_join_req))
                .await;
            id
        }

        async fn inbound_msg(&mut self, addr: SocketAddr, msg: impl Serialize) {
            let msg = bincode::serialize(&msg).unwrap();
            let (out_symm_key, packet_sender) = self.packet_senders.get_mut(&addr).unwrap();
            let sym_msg =
                SymmetricMessage::serialize_msg_to_packet_data(0, msg, &out_symm_key, vec![])
                    .unwrap();
            packet_sender
                .send(sym_msg.as_unknown().into())
                .await
                .unwrap();
        }

        async fn recv_outbound_msg(&mut self) -> anyhow::Result<NetMessage> {
            let (_, msg) = self.packet_receivers[0]
                .recv()
                .await
                .ok_or_else(|| anyhow::Error::msg("Failed to receive packet"))?;
            tracing::info!("Received message");
            let packet: PacketData<UnknownEncryption> = PacketData::from_buf(&*msg);
            let packet = packet
                .try_decrypt_sym(&self.in_key)
                .map_err(|_| anyhow!("Failed to decrypt packet"))?;
            let msg: SymmetricMessage = bincode::deserialize(packet.data()).unwrap();
            let SymmetricMessage {
                payload: SymmetricMessagePayload::ShortMessage { payload },
                ..
            } = msg
            else {
                panic!()
            };
            let msg: NetMessage = bincode::deserialize(&payload).unwrap();
            tracing::debug!("Received message: {:?}", msg);
            Ok(msg)
        }
    }

    struct NodeMock {
        establish_conn: EstablishConnection,
        outbound_msg: OutboundMessage,
    }

    impl NodeMock {
        async fn establish_conn(&self, remote: PeerId, tx: Transaction) {
            self.establish_conn
                .establish_conn(remote, tx)
                .await
                .unwrap();
        }

        async fn outbound_msg(&self, remote: SocketAddr, msg: NetMessage) {
            self.outbound_msg.send_to(remote, msg).await.unwrap();
        }
    }

    struct TestVerifier {
        transport: TransportMock,
        node: NodeMock,
    }

    fn config_handler() -> (HandshakeHandler, TestVerifier) {
        let (outbound_sender, outbound_recv) = mpsc::channel(5);
        let outbound_conn_handler = OutboundConnectionHandler::new(outbound_sender);
        let (inbound_sender, inbound_recv) = mpsc::channel(5);
        let inbound_conn_handler = InboundConnectionHandler::new(inbound_recv);
        let keypair = TransportKeypair::new();
        let peer_id = PeerId::new(([127, 0, 0, 1], 10000).into(), keypair.public().clone());
        let mngr = ConnectionManager::default();
        mngr.set_peer_key(peer_id.clone());
        let router = Router::new(&[]);
        let (handler, establish_conn, outbound_msg) = HandshakeHandler::new(
            inbound_conn_handler,
            outbound_conn_handler,
            mngr,
            Arc::new(RwLock::new(router)),
        );
        (
            handler,
            TestVerifier {
                transport: TransportMock {
                    inbound_sender,
                    outbound_recv,
                    packet_senders: HashMap::new(),
                    packet_receivers: Vec::new(),
                    in_key: Aes128Gcm::new_from_slice(&[0; 16]).unwrap(),
                },
                node: NodeMock {
                    establish_conn,
                    outbound_msg,
                },
            },
        )
    }

    async fn start_conn(
        test: &mut TestVerifier,
        addr: SocketAddr,
        pub_key: TransportPublicKey,
        id: Transaction,
    ) -> oneshot::Sender<Result<RemoteConnection, TransportError>> {
        test.node
            .establish_conn(PeerId::new(addr, pub_key.clone()), id)
            .await;
        let (
            trying_addr,
            ConnectionEvent::ConnectionStart {
                remote_public_key,
                open_connection,
            },
        ) = test
            .transport
            .outbound_recv
            .recv()
            .await
            .ok_or_else(|| anyhow!("failed to get conn start req"))
            .unwrap();
        assert_eq!(trying_addr, addr);
        assert_eq!(remote_public_key, pub_key);
        tracing::debug!("Received connection event");
        open_connection
    }

    #[tokio::test]
    async fn test_gateway_inbound_conn_success() -> anyhow::Result<()> {
        let (mut handler, mut test) = config_handler();
        let test_controller = async {
            let addr = ([127, 0, 0, 1], 10000).into();
            test.transport.new_conn(addr).await;
            test.transport.establish_inbound_conn(addr).await;
            Ok::<_, anyhow::Error>(())
        };

        let gw_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            match event {
                Event::InboundConnection(req) => {
                    let addr: SocketAddr = ([127u8, 0, 0, 1], 10000u16).into();
                    assert_eq!(req.conn.remote_addr(), addr);
                    Ok(())
                }
                other => bail!("Unexpected event: {:?}", other),
            }
        };
        futures::try_join!(test_controller, gw_inbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_gateway_inbound_conn_rejected() -> anyhow::Result<()> {
        let (mut handler, mut test) = config_handler();

        // Configure the handler to reject connections by setting max_connections to 0
        handler.connection_manager.max_connections = 0;
        handler.connection_manager.min_connections = 0;

        let test_controller = async {
            let addr = ([127, 0, 0, 1], 10000).into();
            test.transport.new_conn(addr).await;
            test.transport.establish_inbound_conn(addr).await;
            Ok::<_, anyhow::Error>(())
        };

        let gw_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            match event {
                Event::OutboundConnectionRejected {
                    peer_id,
                    connection,
                } => {
                    let expected_addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
                    assert_eq!(peer_id.addr, expected_addr);
                    assert!(matches!(connection, ConnectOp { .. }));
                    Ok(())
                }
                other => Ok(()),
            }
        };

        futures::try_join!(test_controller, gw_inbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_peer_to_gw_outbound_conn() -> anyhow::Result<()> {
        crate::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG));
        let (mut handler, mut test) = config_handler();

        let joiner_key = TransportKeypair::new();
        let pub_key = joiner_key.public().clone();
        let id = Transaction::new::<ConnectMsg>();

        let remote_addr: SocketAddr = ([127, 0, 0, 1], 10000).into();

        let test_controller = async {
            let addr = ([127, 0, 0, 1], 10000).into();
            let open_connection = start_conn(&mut test, addr, pub_key.clone(), id).await;
            test.transport
                .new_outbound_conn(addr, open_connection)
                .await;
            tracing::debug!("Outbound connection established");
            let msg = test.transport.recv_outbound_msg().await?;
            let msg = match msg {
                NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                    id: inbound_id,
                    msg:
                        ConnectRequest::StartJoinReq {
                            joiner, joiner_key, ..
                        },
                })) => {
                    assert_eq!(id, inbound_id);
                    assert!(joiner.is_none());
                    let sender = PeerKeyLocation {
                        peer: PeerId::new(addr, pub_key.clone()),
                        location: Some(Location::from_address(&remote_addr)),
                    };
                    let joiner_peer_id = PeerId::new(addr, joiner_key.clone());
                    let target = PeerKeyLocation {
                        peer: joiner_peer_id.clone(),
                        location: Some(Location::random()),
                    };
                    NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                        id: inbound_id,
                        sender: sender.clone(),
                        target,
                        msg: ConnectResponse::AcceptedBy {
                            accepted: true,
                            acceptor: sender,
                            joiner: joiner_peer_id,
                        },
                    }))
                }
                other => bail!("Unexpected message: {:?}", other),
            };
            test.transport.inbound_msg(addr, msg).await;
            Ok::<_, anyhow::Error>(())
        };

        let peer_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            match event {
                Event::OutboundConnectionSuccessful { peer_id, .. } => {
                    assert_eq!(peer_id.addr, remote_addr);
                    assert_eq!(peer_id.pub_key, pub_key);
                    Ok(())
                }
                other => bail!("Unexpected event: {:?}", other),
            }
        };
        futures::try_join!(test_controller, peer_inbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_peer_to_gw_outbound_conn_failed() -> anyhow::Result<()> {
        let (mut handler, mut test) = config_handler();

        let joiner_key = TransportKeypair::new();
        let pub_key = joiner_key.public().clone();
        let id = Transaction::new::<ConnectMsg>();

        let test_controller = async {
            let addr = ([127, 0, 0, 1], 10000).into();
            let open_connection = start_conn(&mut test, addr, pub_key.clone(), id).await;
            open_connection
                .send(Err(TransportError::ConnectionEstablishmentFailure {
                    cause: "Connection refused".into(),
                }))
                .map_err(|_| anyhow!("Failed to send connection"))?;
            Ok::<_, anyhow::Error>(())
        };

        let peer_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            match event {
                Event::OutboundConnectionFailed { peer_id, error } => {
                    let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
                    assert_eq!(peer_id.addr, addr);
                    assert_eq!(peer_id.pub_key, pub_key);
                    assert!(matches!(
                        error,
                        HandshakeError::TransportError(
                            TransportError::ConnectionEstablishmentFailure { .. }
                        )
                    ));
                    Ok(())
                }
                other => bail!("Unexpected event: {:?}", other),
            }
        };
        futures::try_join!(test_controller, peer_inbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_peer_to_gw_outbound_conn_rejected() -> anyhow::Result<()> {
        crate::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG));
        let (mut handler, mut test) = config_handler();

        let gw_key = TransportKeypair::new();
        let gw_pub_key = gw_key.public().clone();
        let gw_peer_id = PeerId::new(([127, 0, 0, 1], 10000).into(), gw_pub_key.clone());

        let joiner_key = TransportKeypair::new();
        let joiner_pub_key = joiner_key.public().clone();
        let joiner_peer_id = PeerId::new(([127, 0, 0, 1], 10001).into(), joiner_pub_key.clone());

        let tx = Transaction::new::<ConnectMsg>();

        let test_controller = async {
            let addr = ([127, 0, 0, 1], 10000).into();
            let open_connection = start_conn(&mut test, addr, gw_pub_key.clone(), tx).await;
            test.transport
                .new_outbound_conn(addr, open_connection)
                .await;

            let acceptor = PeerKeyLocation {
                peer: gw_peer_id.clone(),
                location: Some(Location::random()),
            };
            let initial_join_req = ConnectMsg::Response {
                id: tx,
                sender: acceptor.clone(),
                target: PeerKeyLocation {
                    peer: joiner_peer_id.clone(),
                    location: Some(Location::random()),
                },
                msg: ConnectResponse::AcceptedBy {
                    accepted: false,
                    acceptor,
                    joiner: joiner_peer_id.clone(),
                },
            };
            test.transport
                .inbound_msg(
                    addr,
                    NetMessage::V1(NetMessageV1::Connect(initial_join_req)),
                )
                .await;

            Ok::<_, anyhow::Error>(())
        };

        let peer_inbound = async {
            loop {
                let event =
                    tokio::time::timeout(Duration::from_secs(60), handler.wait_for_events())
                        .await??;
                match event {
                    Event::OutboundConnectionSuccessful { peer_id, .. } => {
                        let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
                        assert_eq!(peer_id.addr, addr);
                        assert_eq!(peer_id.pub_key, gw_pub_key);
                        tracing::debug!("Outbound connection successful");
                    }
                    Event::OutboundConnectionRejected {
                        peer_id,
                        connection,
                    } => {
                        let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
                        assert_eq!(peer_id.addr, addr);
                        assert_eq!(peer_id.pub_key, gw_pub_key);
                        break Ok(());
                    }
                    other => bail!("Unexpected event: {:?}", other),
                }
            }
        };
        futures::try_join!(test_controller, peer_inbound)?;
        Ok(())
    }
}
