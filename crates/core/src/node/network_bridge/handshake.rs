//! Handles initial connection handshake.
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::time::{timeout, Duration};
use tracing::{instrument, Instrument};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt, TryFutureExt};
use tokio::sync::mpsc::{self};

use crate::{
    dev_tool::{Location, PeerId, Transaction},
    message::{InnerMessage, NetMessage, NetMessageV1},
    node::NetworkBridge,
    operations::connect::{
        forward_conn, ConnectMsg, ConnectOp, ConnectRequest, ConnectResponse, ConnectState,
        ConnectivityInfo, ForwardParams,
    },
    ring::{ConnectionManager, PeerKeyLocation, Ring},
    router::Router,
    transport::{
        InboundConnectionHandler, OutboundConnectionHandler, PeerConnection, TransportError,
    },
};

type Result<T, E = HandshakeError> = std::result::Result<T, E>;
type OutboundConnResult = Result<InternalEvent, (PeerId, HandshakeError)>;

const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(super) struct ForwardInfo {
    pub target: PeerId,
    pub msg: NetMessage,
}

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
    #[error("receibed an unexpected message at this point: {0}")]
    UnexpectedMessage(Box<NetMessage>),
}

#[derive(Debug)]
pub(super) enum Event {
    // todo: instead of returning InboundJoinReq which is an internal event
    // return a proper well formed ConnectOp and any other types needed (PeerConnection etc.)
    /// An inbound connection to a peer was successfully established at a gateway.
    InboundConnection {
        id: Transaction,
        conn: PeerConnection,
        joiner: PeerId,
        op: Option<Box<ConnectOp>>,
        forward_info: Option<Box<ForwardInfo>>,
    },
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
    OutboundGatewayConnectionRejected { peer_id: PeerId },
    /// An inbound connection in a gateway was rejected.
    InboundConnectionRejected { peer_id: PeerId },
    /// An outbound connection to a gateway was successfully established. It can be managed by the connection manager.
    OutboundGatewayConnectionSuccessful {
        peer_id: PeerId,
        connection: PeerConnection,
        remaining_checks: usize,
    },
    /// Clean up a transaction that was completed or duplicate.
    RemoveTransaction(Transaction),
    /// Wait for replies via an other peer from forwarded connection attempts.
    TransientForwardTransaction {
        target: SocketAddr,
        tx: Transaction,
        forward_to: PeerId,
        msg: Box<NetMessage>,
    },
}

#[allow(clippy::large_enum_variant)]
enum ForwardResult {
    Forward(PeerId, NetMessage, ConnectivityInfo),
    Rejected,
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

pub(super) enum ExternConnection {
    Establish {
        peer: PeerId,
        tx: Transaction,
        is_gw: bool,
    },
}

/// Use for starting a new outboound connection to a peer.
pub(super) struct EstablishConnection(pub(crate) mpsc::Sender<ExternConnection>);

impl EstablishConnection {
    pub async fn establish_conn(&self, remote: PeerId, tx: Transaction, is_gw: bool) -> Result<()> {
        self.0
            .send(ExternConnection::Establish {
                peer: remote,
                tx,
                is_gw,
            })
            .await
            .map_err(|_| HandshakeError::ChannelClosed)?;
        Ok(())
    }
}

type OutboundMessageSender = mpsc::Sender<NetMessage>;
type OutboundMessageReceiver = mpsc::Receiver<(SocketAddr, NetMessage)>;
type EstablishConnectionReceiver = mpsc::Receiver<ExternConnection>;

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
    #[instrument(skip(self))]
    pub async fn wait_for_events(&mut self) -> Result<Event, HandshakeError> {
        loop {
            tokio::select! {
                // Handle new inbound connections
                new_conn = self.inbound_conn_handler.next_connection() => {
                    let Some(conn) = new_conn else {
                        return Err(HandshakeError::ChannelClosed);
                    };
                    tracing::debug!(from=%conn.remote_addr(), "New inbound connection");
                    self.track_inbound_connection(conn);
                }
                // Process outbound connection attempts
                outbound_conn = self.ongoing_outbound_connections.next(), if !self.ongoing_outbound_connections.is_empty() => {
                    let r = match outbound_conn {
                        Some(Ok(InternalEvent::OutboundConnEstablished(peer_id, connection))) => {
                            tracing::debug!(at=?connection.my_address(), from=%connection.remote_addr(), "Outbound connection successful");
                            Ok(Event::OutboundConnectionSuccessful { peer_id, connection })
                        }
                        Some(Ok(InternalEvent::OutboundGwConnEstablished(id, connection))) => {
                            if let Some(addr) = connection.my_address() {
                                tracing::debug!(%addr, "Attempting setting own peer key");
                                self.connection_manager.try_set_peer_key(addr);
                                self.connection_manager.update_location(Some(Location::from_address(&addr)));
                            }
                            tracing::debug!(at=?connection.my_address(), from=%connection.remote_addr(), "Outbound connection to gw successful");
                            self.wait_for_gw_confirmation(id, connection, Ring::DEFAULT_MAX_HOPS_TO_LIVE).await?;
                            continue;
                        }
                        Some(Ok(InternalEvent::FinishedOutboundConnProcess(tracker))) => {
                            self.connecting.remove(&tracker.gw_peer.peer.addr);
                            // at this point we are done checking all the accepts inbound from a transient gw conn
                            tracing::debug!(at=?tracker.gw_conn.my_address(), gw=%tracker.gw_conn.remote_addr(), "Done checking, connection not accepted by gw, dropping connection");
                            Ok(Event::OutboundGatewayConnectionRejected { peer_id: tracker.gw_peer.peer })
                        }
                        Some(Ok(InternalEvent::OutboundGwConnConfirmed(tracker))) => {
                            tracing::debug!(at=?tracker.gw_conn.my_address(), from=%tracker.gw_conn.remote_addr(), "Outbound connection to gw confirmed");
                            self.connected.insert(tracker.gw_conn.remote_addr());
                            self.connecting.remove(&tracker.gw_conn.remote_addr());
                            return Ok(Event::OutboundGatewayConnectionSuccessful {
                                peer_id: tracker.gw_peer.peer,
                                connection: tracker.gw_conn,
                                remaining_checks: tracker.remaining_checks,
                            });
                        }
                        Some(Ok(InternalEvent::NextCheck(tracker))) => {
                            self.ongoing_outbound_connections.push(
                                check_remaining_hops(tracker).boxed()
                            );
                            continue;
                        }
                        Some(Ok(InternalEvent::RemoteConnectionAttempt { remote, tracker })) => {
                             // this shouldn't happen as the tx would exit this module
                             // see: OutboundGwConnConfirmed
                            debug_assert!(!tracker.gw_accepted);
                            tracing::debug!(
                                at=?tracker.gw_conn.my_address(),
                                gw=%tracker.gw_conn.remote_addr(),
                                "Attempting remote connection to {remote}"
                            );
                            self.start_outbound_connection(remote.clone(), tracker.tx, false).await;
                            let current_span = tracing::Span::current();
                            let checking_hops_span = tracing::info_span!(parent: current_span, "checking_hops");
                            self.ongoing_outbound_connections.push(
                                check_remaining_hops(tracker).instrument(checking_hops_span).boxed()
                            );
                            continue;
                        }
                        Some(Ok(InternalEvent::DropInboundConnection(addr))) => {
                            self.connecting.remove(&addr);
                            self.outbound_messages.remove(&addr);
                            continue;
                        }
                        Some(Err((peer_id, error))) => {
                            tracing::debug!(from=%peer_id.addr, "Outbound connection failed: {error}");
                            self.connecting.remove(&peer_id.addr);
                            self.outbound_messages.remove(&peer_id.addr);
                            self.connection_manager.prune_alive_connection(&peer_id);
                            Ok(Event::OutboundConnectionFailed { peer_id, error })
                        }
                        Some(Ok(other)) => {
                            tracing::error!("Unexpected event: {other:?}");
                            continue;
                        }
                        None => Err(HandshakeError::ChannelClosed),
                    };
                    break r;
                }
                // Handle unconfirmed inbound connections (only applies in gateways)
                unconfirmed_inbound_conn = self.unconfirmed_inbound_connections.next(), if !self.unconfirmed_inbound_connections.is_empty() => {
                    let Some(res) = unconfirmed_inbound_conn else {
                        return Err(HandshakeError::ChannelClosed);
                    };
                    let (event, outbound_sender) = res?;
                    match event {
                        InternalEvent::InboundGwJoinRequest(mut req) => {
                            let remote = req.conn.remote_addr();
                            let location = Location::from_address(&remote);
                            let should_accept = self.connection_manager.should_accept(location, &req.joiner);
                            if should_accept {
                                let accepted_msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                                    id: req.id,
                                    sender: self.connection_manager.own_location(),
                                    target: PeerKeyLocation {
                                        peer: req.joiner.clone(),
                                        location: Some(location),
                                    },
                                    msg: ConnectResponse::AcceptedBy {
                                        accepted: true,
                                        acceptor: self.connection_manager.own_location(),
                                        joiner: req.joiner.clone(),
                                    },
                                }));

                                tracing::debug!(at=?req.conn.my_address(), from=%req.conn.remote_addr(), "Accepting connection");

                                if let Err(e) = req.conn.send(accepted_msg).await {
                                    tracing::error!(%e, "Failed to send accepted message from gw, pruning reserved connection");
                                    self.connection_manager.prune_in_transit_connection(&req.joiner);
                                    return Err(e.into());
                                }

                                let InboundGwJoinRequest { conn, id, joiner, hops_to_live, max_hops_to_live, skip_list } = req;

                                let (ok, forward_info) = {
                                    // TODO: refactor this so it happens in the background out of the main handler loop
                                    let mut nw_bridge = ForwardPeerMessage {
                                        msg: parking_lot::Mutex::new(None),
                                    };

                                    let my_peer_id = self.connection_manager.own_location();
                                    let joiner_loc = Location::from_address(&conn.remote_addr());
                                    let joiner_pk_loc = PeerKeyLocation {
                                        peer: joiner.clone(),
                                        location: Some(joiner_loc),
                                    };

                                    let f = forward_conn(
                                        id,
                                        &self.connection_manager,
                                        self.router.clone(),
                                        &mut nw_bridge,
                                        ForwardParams {
                                            left_htl: hops_to_live,
                                            max_htl: max_hops_to_live,
                                            skip_list,
                                            accepted: true,
                                            req_peer: my_peer_id.clone(),
                                            joiner: joiner_pk_loc.clone(),
                                        }
                                    );

                                    match f.await {
                                        Err(err) => {
                                            tracing::error!(%err, "Error forwarding connection");
                                            continue;
                                        }
                                        Ok(ok) => {
                                            if let Some(ok_value) = ok {
                                                let forward_info = nw_bridge.msg.lock().take().map(|(forward_target, msg)| {
                                                    ForwardInfo {
                                                        target: forward_target,
                                                        msg,
                                                    }
                                                });
                                                (Some(ok_value), forward_info)
                                            } else {
                                                (None, None)
                                            }
                                        }
                                    }
                                };

                                return Ok(Event::InboundConnection {
                                    id,
                                    conn,
                                    joiner,
                                    op: ok.map(|ok_value| Box::new(ConnectOp::new(id, Some(ok_value), None, None))),
                                    forward_info: forward_info.map(Box::new),
                                })

                            } else {
                                let InboundGwJoinRequest { mut conn, id, hops_to_live, max_hops_to_live, skip_list, .. } = req;
                                let remote = conn.remote_addr();
                                tracing::debug!(at=?conn.my_address(), from=%remote, "Transient connection");
                                let mut tx = TransientConnection {
                                    tx: id,
                                    joiner: req.joiner.clone(),
                                    max_hops_to_live,
                                    hops_to_live,
                                    skip_list,
                                };
                                match self.forward_transient_connection(&mut conn, &mut tx).await {
                                    Ok(ForwardResult::Forward(forward_target, msg, info)) => {
                                        self.unconfirmed_inbound_connections.push(
                                            gw_transient_peer_conn(
                                                conn,
                                                outbound_sender,
                                                tx,
                                                info,
                                            ).boxed()
                                        );
                                        return Ok(Event::TransientForwardTransaction {
                                            target: remote,
                                            tx: id,
                                            forward_to: forward_target,
                                            msg: Box::new(msg),
                                        });
                                    }
                                    Ok(ForwardResult::Rejected) => {
                                        self.outbound_messages.remove(&remote);
                                        self.connecting.remove(&remote);
                                        return Ok(Event::InboundConnectionRejected { peer_id: req.joiner });
                                    }
                                    Err(e) => {
                                        tracing::error!(from=%remote, "Error forwarding transient connection: {e}");
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        InternalEvent::DropInboundConnection(addr) => {
                            self.outbound_messages.remove(&addr);
                            self.connecting.remove(&addr);
                            continue;
                        }
                        other => {
                            tracing::error!("Unexpected event: {other:?}");
                            continue;
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
                    match establish_connection {
                        Some(ExternConnection::Establish { peer, tx, is_gw }) => {
                            self.start_outbound_connection(peer, tx, is_gw).await;
                        }
                        None => return Err(HandshakeError::ChannelClosed),
                    }
                }
            }
        }
    }

    async fn forward_transient_connection(
        &mut self,
        conn: &mut PeerConnection,
        transaction: &mut TransientConnection,
    ) -> Result<ForwardResult, HandshakeError> {
        let mut nw_bridge = ForwardPeerMessage {
            msg: parking_lot::Mutex::new(None),
        };

        let joiner_loc = Location::from_address(&conn.remote_addr());
        let joiner_pk_loc = PeerKeyLocation {
            peer: transaction.joiner.clone(),
            location: Some(joiner_loc),
        };
        let my_peer_id = self.connection_manager.own_location();
        transaction.skip_list.push(transaction.joiner.clone());
        transaction.skip_list.push(my_peer_id.peer.clone());

        match forward_conn(
            transaction.tx,
            &self.connection_manager,
            self.router.clone(),
            &mut nw_bridge,
            ForwardParams {
                left_htl: transaction.hops_to_live,
                max_htl: transaction.max_hops_to_live,
                skip_list: transaction.skip_list.clone(),
                accepted: false,
                req_peer: my_peer_id.clone(),
                joiner: joiner_pk_loc.clone(),
            },
        )
        .await
        {
            Ok(Some(conn_state)) => {
                let (forward_target, msg) = nw_bridge
                    .msg
                    .into_inner()
                    .expect("target was successfully set");
                let ConnectState::AwaitingConnectivity(info) = conn_state else {
                    unreachable!()
                };
                Ok(ForwardResult::Forward(forward_target, msg, info))
            }
            Ok(None) => {
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Rejecting connection, no peers found to forward");
                // No peer to forward to, reject the connection
                let reject_msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                    id: transaction.tx,
                    sender: my_peer_id.clone(),
                    target: joiner_pk_loc,
                    msg: ConnectResponse::AcceptedBy {
                        accepted: false,
                        acceptor: my_peer_id,
                        joiner: transaction.joiner.clone(),
                    },
                }));
                conn.send(reject_msg).await?;
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Connection rejected");
                Ok(ForwardResult::Rejected)
            }
            Err(_) => Err(HandshakeError::ConnectionClosed(conn.remote_addr())),
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

    /// Handles outbound messages to peers.
    async fn outbound(&mut self, addr: SocketAddr, op: NetMessage) -> Option<Event> {
        if let Some(alive_conn) = self.outbound_messages.get_mut(&addr) {
            if let NetMessage::V1(NetMessageV1::Connect(op)) = &op {
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

            if alive_conn.send(op).await.is_err() {
                self.outbound_messages.remove(&addr);
                self.connecting.remove(&addr);
            }
            None
        } else {
            let mut send_to_remote = None;
            if let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                msg: ConnectResponse::AcceptedBy { joiner, .. },
                ..
            })) = &op
            {
                // this may be a reply message from a downstream peer to which it was forwarded previously
                // for a transient connection, in this case we must send this message to the proper
                // gw_transient_peer_conn future that is waiting for it
                send_to_remote = Some(joiner.addr);
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

            #[cfg(debug_assertions)]
            {
                unreachable!("Can't send messages to a peer without an established connection");
            }
            #[cfg(not(debug_assertions))]
            {
                // we don't want to crash the node in case of a bug here
                tracing::error!("No outbound message sender for {addr}", addr = addr);
                None
            }
        }
    }

    /// Starts an outbound connection to the given peer.
    async fn start_outbound_connection(
        &mut self,
        remote: PeerId,
        transaction: Transaction,
        is_gw: bool,
    ) {
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
                Ok(conn) if is_gw => {
                    tracing::debug!(%remote, "established outbound gw connection");
                    Ok(InternalEvent::OutboundGwConnEstablished(remote, conn))
                }
                Ok(conn) => {
                    tracing::debug!(%remote, "established outbound connection");
                    Ok(InternalEvent::OutboundConnEstablished(remote, conn))
                }
                Err(e) => Err((remote, e.into())),
            })
            .boxed();
        self.ongoing_outbound_connections.push(f);
    }

    /// Waits for confirmation from a gateway after establishing a connection.
    async fn wait_for_gw_confirmation(
        &mut self,
        gw_peer_id: PeerId,
        conn: PeerConnection,
        max_hops_to_live: usize,
    ) -> Result<()> {
        let tx = *self
            .connecting
            .get(&gw_peer_id.addr)
            .ok_or_else(|| HandshakeError::ConnectionClosed(conn.remote_addr()))?;
        let this_peer = self.connection_manager.own_location().peer;
        tracing::debug!(at=?conn.my_address(), %this_peer.addr, from=%conn.remote_addr(), remote_addr = %gw_peer_id, "Waiting for confirmation from gw");
        self.ongoing_outbound_connections.push(
            wait_for_gw_confirmation(
                this_peer,
                AcceptedTracker {
                    gw_peer: gw_peer_id.into(),
                    gw_conn: conn,
                    gw_accepted: false,
                    gw_accepted_processed: false,
                    remaining_checks: max_hops_to_live,
                    accepted: 0,
                    total_checks: max_hops_to_live,
                    tx,
                },
            )
            .boxed(),
        );
        Ok(())
    }
}

// Attempt forwarding the connection request to the next hop and wait for answers
// then return those answers to the transitory peer connection.
struct ForwardPeerMessage {
    msg: parking_lot::Mutex<Option<(PeerId, NetMessage)>>,
}

impl NetworkBridge for ForwardPeerMessage {
    async fn send(&self, target: &PeerId, forward_msg: NetMessage) -> super::ConnResult<()> {
        debug_assert!(matches!(
            forward_msg,
            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                msg: ConnectRequest::CheckConnectivity { .. },
                ..
            }))
        ));
        self.msg
            .try_lock()
            .expect("unique ref")
            .replace((target.clone(), forward_msg));
        Ok(())
    }

    async fn drop_connection(&mut self, _: &PeerId) -> super::ConnResult<()> {
        if cfg!(debug_assertions) {
            unreachable!()
        }
        Ok(())
    }
}

#[derive(Debug)]
struct InboundGwJoinRequest {
    pub conn: PeerConnection,
    pub id: Transaction,
    pub joiner: PeerId,
    pub hops_to_live: usize,
    pub max_hops_to_live: usize,
    pub skip_list: Vec<PeerId>,
}

#[derive(Debug)]
enum InternalEvent {
    InboundGwJoinRequest(InboundGwJoinRequest),
    /// Regular connection established
    OutboundConnEstablished(PeerId, PeerConnection),
    OutboundGwConnEstablished(PeerId, PeerConnection),
    OutboundGwConnConfirmed(AcceptedTracker),
    DropInboundConnection(SocketAddr),
    RemoteConnectionAttempt {
        remote: PeerId,
        tracker: AcceptedTracker,
    },
    NextCheck(AcceptedTracker),
    FinishedOutboundConnProcess(AcceptedTracker),
}

#[repr(transparent)]
struct PeerOutboundMessage(mpsc::Receiver<NetMessage>);

#[derive(Debug)]
struct AcceptedTracker {
    gw_peer: PeerKeyLocation,
    gw_conn: PeerConnection,
    gw_accepted_processed: bool,
    gw_accepted: bool,
    /// Remaining checks to be made, at max total_checks
    remaining_checks: usize,
    /// At max this will be total_checks
    accepted: usize,
    /// Equivalent to max_hops_to_live
    total_checks: usize,
    tx: Transaction,
}

/// Waits for confirmation from a gateway after initiating a connection.
async fn wait_for_gw_confirmation(
    this_peer: PeerId,
    mut tracker: AcceptedTracker,
) -> OutboundConnResult {
    let gw_peer_id = tracker.gw_peer.peer.clone();
    let msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
        id: tracker.tx,
        target: tracker.gw_peer.clone(),
        msg: ConnectRequest::StartJoinReq {
            joiner: Some(this_peer.clone()),
            joiner_key: this_peer.pub_key.clone(),
            hops_to_live: tracker.total_checks,
            max_hops_to_live: tracker.total_checks,
            skip_list: vec![this_peer],
        },
    }));
    tracing::debug!(
        at=?tracker.gw_conn.my_address(),
        from=%tracker.gw_conn.remote_addr(),
        msg = ?msg,
        "Sending initial connection message to gw"
    );
    tracker
        .gw_conn
        .send(msg)
        .await
        .map_err(|err| (gw_peer_id.clone(), HandshakeError::TransportError(err)))?;
    tracing::debug!(
        at=?tracker.gw_conn.my_address(),
        from=%tracker.gw_conn.remote_addr(),
        "Waiting for answer from gw"
    );

    // under this branch we just need to wait long enough for the gateway to reply with all the downstream
    // connection attempts, and then we can drop the connection, so keep listening to it in a loop or timeout
    let remote = tracker.gw_conn.remote_addr();
    tokio::time::timeout(
        TIMEOUT,
        check_remaining_hops(tracker),
    )
    .await
    .map_err(|_| {
        tracing::debug!(from=%gw_peer_id, "Timed out waiting for acknowledgement from downstream requests");
        (
            gw_peer_id,
            HandshakeError::ConnectionClosed(remote),
        )
    })?
}

async fn check_remaining_hops(mut tracker: AcceptedTracker) -> OutboundConnResult {
    let remote_addr = tracker.gw_conn.remote_addr();
    let gw_peer_id = tracker.gw_peer.peer.clone();
    tracing::debug!(
        at=?tracker.gw_conn.my_address(),
        from=%tracker.gw_conn.remote_addr(),
        "Checking for remaining hops, left: {}", tracker.remaining_checks
    );
    while tracker.remaining_checks > 0 {
        let msg = tokio::time::timeout(
            TIMEOUT,
            tracker
                .gw_conn
                .recv()
                .map_err(|err| (gw_peer_id.clone(), HandshakeError::TransportError(err))),
        )
        .map_err(|_| {
            tracing::debug!(from = %gw_peer_id, "Timed out waiting for response from gw");
            (
                gw_peer_id.clone(),
                HandshakeError::ConnectionClosed(remote_addr),
            )
        })
        .await??;
        let msg = decode_msg(&msg).map_err(|e| (gw_peer_id.clone(), e))?;
        match msg {
            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                msg:
                    ConnectResponse::AcceptedBy {
                        accepted, acceptor, ..
                    },
                ..
            })) => {
                tracker.remaining_checks -= 1;
                if acceptor.peer.addr == tracker.gw_conn.remote_addr() {
                    // this is a message from the gw indicating if they accepted or not
                    tracker.gw_accepted_processed = true;
                    if accepted {
                        tracker.gw_accepted = true;
                        tracker.accepted += 1;
                    }
                    tracing::debug!(
                        at = ?tracker.gw_conn.my_address(),
                        from = %tracker.gw_conn.remote_addr(),
                        %accepted,
                        "Received answer from gw"
                    );
                    if accepted {
                        return Ok(InternalEvent::OutboundGwConnConfirmed(tracker));
                    } else {
                        tracing::debug!("Rejected by gateway, waiting for forward replies");
                        return Ok(InternalEvent::NextCheck(tracker));
                    }
                } else if accepted {
                    return Ok(InternalEvent::RemoteConnectionAttempt {
                        remote: acceptor.peer,
                        tracker,
                    });
                } else {
                    continue;
                }
            }
            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                msg: ConnectRequest::FindOptimalPeer { .. },
                ..
            })) => {
                tracing::warn!(from=%tracker.gw_conn.remote_addr(), "Received FindOptimalPeer request, ignoring");
                continue;
            }
            other => {
                return Err((
                    gw_peer_id,
                    HandshakeError::UnexpectedMessage(Box::new(other)),
                ))
            }
        }
    }
    Ok(InternalEvent::FinishedOutboundConnProcess(tracker))
}

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
                        msg: ConnectRequest::StartJoinReq { joiner, joiner_key, hops_to_live, max_hops_to_live, skip_list },
                        ..
                    })) => {
                        let joiner = joiner.unwrap_or_else(|| {
                            tracing::debug!(%joiner_key, "Joiner not provided, using joiner key");
                            PeerId::new(conn.remote_addr(), joiner_key)
                        });
                        break Ok((
                            InternalEvent::InboundGwJoinRequest(
                                InboundGwJoinRequest {
                                    conn, id, joiner, hops_to_live, max_hops_to_live, skip_list
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
    mut info: ConnectivityInfo,
) -> Result<(InternalEvent, PeerOutboundMessage), HandshakeError> {
    // TODO: should be the same timeout as the one used for any other tx
    loop {
        tokio::select! {
            incoming_result = timeout(TIMEOUT, conn.recv()) => {
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
            outbound_msg = timeout(TIMEOUT, outbound.0.recv()) => {
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
                            // in this case it may be a reply of a third party we forwarded to,
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
            ..
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
    bincode::deserialize(data).map_err(HandshakeError::Serialization)
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
        ring::{Connection, PeerKeyLocation, Ring},
        transport::{
            ConnectionEvent, OutboundConnectionHandler, PacketData, RemoteConnection,
            SymmetricMessage, SymmetricMessagePayload, TransportPublicKey, UnknownEncryption,
        },
    };

    struct TransportMock {
        inbound_sender: mpsc::Sender<PeerConnection>,
        outbound_recv: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
        packet_senders:
            HashMap<SocketAddr, (Aes128Gcm, mpsc::Sender<PacketData<UnknownEncryption>>)>,
        packet_id: u32,
        packet_receivers: Vec<mpsc::Receiver<(SocketAddr, Arc<[u8]>)>>,
        in_key: Aes128Gcm,
        my_addr: SocketAddr,
    }

    impl TransportMock {
        async fn new_conn(&mut self, addr: SocketAddr) {
            let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
            let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
            let (conn, packet_sender, packet_recv) =
                PeerConnection::new_test(addr, self.my_addr, out_symm_key, in_symm_key.clone());
            self.inbound_sender.send(conn).await.unwrap();
            tracing::debug!("New inbound connection established");
            self.packet_senders
                .insert(addr, (in_symm_key, packet_sender));
            self.packet_receivers.push(packet_recv);
        }

        async fn new_outbound_conn(
            &mut self,
            addr: SocketAddr,
            callback: oneshot::Sender<Result<crate::transport::RemoteConnection, TransportError>>,
        ) {
            let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
            let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
            let (conn, packet_sender, packet_recv) = PeerConnection::new_remote_test(
                addr,
                self.my_addr,
                out_symm_key,
                in_symm_key.clone(),
            );
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
        async fn establish_inbound_conn(
            &mut self,
            addr: SocketAddr,
            pub_key: TransportPublicKey,
            hops_to_live: Option<usize>,
        ) {
            let id = Transaction::new::<ConnectMsg>();
            let target_peer_id = PeerId::new(addr, pub_key.clone());
            let target_peer = PeerKeyLocation::from(target_peer_id);
            let hops_to_live = hops_to_live.unwrap_or(10);
            // let joiner_key = TransportKeypair::new();
            // let pub_key = joiner_key.public().clone();
            let initial_join_req = ConnectMsg::Request {
                id,
                target: target_peer,
                msg: ConnectRequest::StartJoinReq {
                    joiner: None,
                    joiner_key: pub_key,
                    hops_to_live,
                    max_hops_to_live: hops_to_live,
                    skip_list: vec![],
                },
            };
            self.inbound_msg(
                addr,
                NetMessage::V1(NetMessageV1::Connect(initial_join_req)),
            )
            .await
        }

        async fn inbound_msg(&mut self, addr: SocketAddr, msg: impl Serialize) {
            let msg = bincode::serialize(&msg).unwrap();
            let (out_symm_key, packet_sender) = self.packet_senders.get_mut(&addr).unwrap();
            let sym_msg = SymmetricMessage::serialize_msg_to_packet_data(
                self.packet_id,
                msg,
                out_symm_key,
                vec![],
            )
            .unwrap();
            tracing::trace!(at=?self.my_addr, to=%addr, "Sending message to peer");
            packet_sender.send(sym_msg.into_unknown()).await.unwrap();
            tracing::trace!(at=?self.my_addr, from=%addr, "Message sent");
            self.packet_id += 1;
        }

        async fn recv_outbound_msg(&mut self) -> anyhow::Result<NetMessage> {
            let (_, msg) = self.packet_receivers[0]
                .recv()
                .await
                .ok_or_else(|| anyhow::Error::msg("Failed to receive packet"))?;
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
            Ok(msg)
        }
    }

    struct NodeMock {
        establish_conn: EstablishConnection,
        _outbound_msg: OutboundMessage,
    }

    impl NodeMock {
        /// A request from node internals to establish a connection with a peer.
        async fn establish_conn(&self, remote: PeerId, tx: Transaction, is_gw: bool) {
            self.establish_conn
                .establish_conn(remote, tx, is_gw)
                .await
                .unwrap();
        }
    }

    struct TestVerifier {
        transport: TransportMock,
        node: NodeMock,
    }

    fn config_handler(
        addr: impl Into<SocketAddr>,
        existing_connections: Option<Vec<Connection>>,
    ) -> (HandshakeHandler, TestVerifier) {
        let (outbound_sender, outbound_recv) = mpsc::channel(100);
        let outbound_conn_handler = OutboundConnectionHandler::new(outbound_sender);
        let (inbound_sender, inbound_recv) = mpsc::channel(100);
        let inbound_conn_handler = InboundConnectionHandler::new(inbound_recv);
        let addr = addr.into();
        let keypair = TransportKeypair::new();
        let mngr = ConnectionManager::default_with_key(keypair.public().clone());
        mngr.try_set_peer_key(addr);
        let router = Router::new(&[]);

        if let Some(connections) = existing_connections {
            for conn in connections {
                let location = conn.get_location().location.unwrap();
                let peer_id = conn.get_location().peer.clone();
                mngr.add_connection(location, peer_id, false);
            }
        }

        let (handler, establish_conn, _outbound_msg) = HandshakeHandler::new(
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
                    packet_id: 0,
                    my_addr: addr,
                },
                node: NodeMock {
                    establish_conn,
                    _outbound_msg,
                },
            },
        )
    }

    async fn start_conn(
        test: &mut TestVerifier,
        addr: SocketAddr,
        pub_key: TransportPublicKey,
        id: Transaction,
        is_gw: bool,
    ) -> oneshot::Sender<Result<RemoteConnection, TransportError>> {
        test.node
            .establish_conn(PeerId::new(addr, pub_key.clone()), id, is_gw)
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
        let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
        let (mut handler, mut test) = config_handler(addr, None);

        let remote_addr = ([127, 0, 0, 1], 10001).into();
        let test_controller = async {
            let pub_key = TransportKeypair::new().public().clone();
            test.transport.new_conn(remote_addr).await;
            test.transport
                .establish_inbound_conn(remote_addr, pub_key, None)
                .await;
            Ok::<_, anyhow::Error>(())
        };

        let gw_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            match event {
                Event::InboundConnection { conn, .. } => {
                    assert_eq!(conn.remote_addr(), remote_addr);
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
        let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
        let existing_remote_addr = ([127, 0, 0, 1], 10001).into();
        let remote_peer_loc = PeerKeyLocation {
            peer: PeerId::new(
                existing_remote_addr,
                TransportKeypair::new().public().clone(),
            ),
            location: Some(Location::from_address(&existing_remote_addr)),
        };
        let existing_conn =
            Connection::new(remote_peer_loc.peer, remote_peer_loc.location.unwrap());

        let (mut handler, mut test) = config_handler(addr, Some(vec![existing_conn]));

        // Configure the handler to reject connections by setting max_connections to 1
        handler.connection_manager.max_connections = 1;
        handler.connection_manager.min_connections = 1;

        let remote_addr = ([127, 0, 0, 1], 10002).into();

        let test_controller = async {
            let pub_key = TransportKeypair::new().public().clone();
            test.transport.new_conn(remote_addr).await;
            // Put hops_to_live to 0 to avoid forwarding
            test.transport
                .establish_inbound_conn(remote_addr, pub_key, Some(0))
                .await;
            let msg = test.transport.recv_outbound_msg().await?;
            tracing::debug!("Received outbound message: {:?}", msg);
            assert!(
                matches!(msg, NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                msg: ConnectResponse::AcceptedBy { accepted, .. },
                ..
            })) if !accepted)
            );
            Ok::<_, anyhow::Error>(())
        };

        let gw_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            match event {
                Event::InboundConnectionRejected { peer_id } => {
                    assert_eq!(peer_id.addr, remote_addr);
                    Ok(())
                }
                other => Err(anyhow!("Unexpected event: {:?}", other)),
            }
        };

        futures::try_join!(test_controller, gw_inbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_peer_to_gw_outbound_conn() -> anyhow::Result<()> {
        let addr = ([127, 0, 0, 1], 10000).into();
        let (mut handler, mut test) = config_handler(addr, None);

        let joiner_key = TransportKeypair::new();
        let pub_key = joiner_key.public().clone();
        let id = Transaction::new::<ConnectMsg>();

        let remote_addr: SocketAddr = ([127, 0, 0, 1], 10001).into();
        let test_controller = async {
            let open_connection =
                start_conn(&mut test, remote_addr, pub_key.clone(), id, true).await;
            test.transport
                .new_outbound_conn(remote_addr, open_connection)
                .await;
            tracing::debug!("Outbound connection established");
            let msg = test.transport.recv_outbound_msg().await?;
            let msg = match msg {
                NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                    id: inbound_id,
                    msg: ConnectRequest::StartJoinReq { joiner_key, .. },
                    ..
                })) => {
                    assert_eq!(id, inbound_id);
                    let sender = PeerKeyLocation {
                        peer: PeerId::new(remote_addr, pub_key.clone()),
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
            test.transport.inbound_msg(remote_addr, msg).await;
            Ok::<_, anyhow::Error>(())
        };

        let peer_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            match event {
                Event::OutboundGatewayConnectionSuccessful { peer_id, .. } => {
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
        let addr = ([127, 0, 0, 1], 10000).into();
        let (mut handler, mut test) = config_handler(addr, None);

        let joiner_key = TransportKeypair::new();
        let pub_key = joiner_key.public().clone();
        let id = Transaction::new::<ConnectMsg>();

        let test_controller = async {
            let open_connection = start_conn(&mut test, addr, pub_key.clone(), id, true).await;
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
    async fn test_gw_to_peer_outbound_conn_forwarded() -> anyhow::Result<()> {
        // crate::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG));
        let gw_addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
        let peer_addr: SocketAddr = ([127, 0, 0, 1], 10001).into();
        let joiner_addr: SocketAddr = ([127, 0, 0, 1], 10002).into();

        let (mut gw_handler, mut gw_test) = config_handler(gw_addr, None);

        // the gw only will accept one connection
        gw_handler.connection_manager.max_connections = 1;
        gw_handler.connection_manager.min_connections = 1;

        let peer_key = TransportKeypair::new();
        let joiner_key = TransportKeypair::new();

        let peer_pub_key = peer_key.public().clone();
        let joiner_pub_key = joiner_key.public().clone();

        let peer_peer_id = PeerId::new(peer_addr, peer_pub_key.clone());

        let gw_test_controller = async {
            // the connection to the gw with the third-party peer is established first
            gw_test.transport.new_conn(peer_addr).await;
            gw_test
                .transport
                .establish_inbound_conn(peer_addr, peer_pub_key.clone(), None)
                .await;

            // the joiner attempts to connect to the gw, but since it's out of connections
            // it will just be a transient connection
            gw_test.transport.new_conn(joiner_addr).await;
            gw_test
                .transport
                .establish_inbound_conn(joiner_addr, joiner_pub_key, None)
                .await;

            // TODO: maybe simulate forwarding back all expected responses

            Ok::<_, anyhow::Error>(())
        };

        let peer_and_gw = async {
            let mut third_party = None;
            loop {
                let event =
                    tokio::time::timeout(Duration::from_secs(1), gw_handler.wait_for_events())
                        .await??;
                match event {
                    Event::InboundConnection {
                        conn: first_peer_conn,
                        joiner: third_party_peer,
                        ..
                    } => {
                        tracing::info!("Received join request from joiner");
                        assert_eq!(third_party_peer.pub_key, peer_pub_key);
                        assert_eq!(first_peer_conn.remote_addr(), peer_addr);
                        third_party = Some(third_party_peer);
                        gw_handler.connection_manager.add_connection(
                            Location::from_address(&peer_addr),
                            peer_peer_id.clone(),
                            false,
                        );
                    }
                    Event::TransientForwardTransaction {
                        target,
                        forward_to,
                        msg,
                        ..
                    } => {
                        tracing::info!("Forward join request from joiner to third-party");
                        // transient connection created, and forwarded a request to join to the third-party peer
                        assert_eq!(target, joiner_addr);
                        assert_eq!(forward_to.pub_key, peer_pub_key);
                        assert_eq!(forward_to.addr, peer_peer_id.addr);
                        assert!(matches!(
                            &*msg,
                            NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                                msg: ConnectRequest::CheckConnectivity { .. },
                                ..
                            }))
                        ));
                        break;
                    }
                    other => bail!("Unexpected event: {:?}", other),
                }
            }

            assert!(third_party.is_some());
            Ok(())
        };

        futures::try_join!(gw_test_controller, peer_and_gw)?;
        Ok(())
    }

    #[ignore = "flaky in ci"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_peer_to_gw_outbound_conn_rejected() -> anyhow::Result<()> {
        // crate::config::set_logger(Some(tracing::level_filters::LevelFilter::TRACE), None);
        let joiner_addr = ([127, 0, 0, 1], 10001).into();
        let (mut handler, mut test) = config_handler(joiner_addr, None);

        let gw_key = TransportKeypair::new();
        let gw_pub_key = gw_key.public().clone();
        let gw_addr = ([127, 0, 0, 1], 10000).into();
        let gw_peer_id = PeerId::new(gw_addr, gw_pub_key.clone());
        let gw_pkloc = PeerKeyLocation {
            location: Some(Location::from_address(&gw_peer_id.addr)),
            peer: gw_peer_id.clone(),
        };

        let joiner_key = TransportKeypair::new();
        let joiner_pub_key = joiner_key.public().clone();
        let joiner_peer_id = PeerId::new(joiner_addr, joiner_pub_key.clone());
        let joiner_pkloc = PeerKeyLocation {
            peer: joiner_peer_id.clone(),
            location: Some(Location::from_address(&joiner_peer_id.addr)),
        };

        let tx = Transaction::new::<ConnectMsg>();

        let test_controller = async {
            let open_connection =
                start_conn(&mut test, gw_addr, gw_pub_key.clone(), tx, true).await;
            test.transport
                .new_outbound_conn(gw_addr, open_connection)
                .await;

            let msg = test.transport.recv_outbound_msg().await?;
            tracing::info!("Received connec request: {:?}", msg);
            let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                id,
                msg: ConnectRequest::StartJoinReq { .. },
                ..
            })) = msg
            else {
                panic!("unexpected message");
            };
            assert_eq!(id, tx);

            let initial_join_req = ConnectMsg::Response {
                id: tx,
                sender: gw_pkloc.clone(),
                target: joiner_pkloc.clone(),
                msg: ConnectResponse::AcceptedBy {
                    accepted: false,
                    acceptor: gw_pkloc.clone(),
                    joiner: joiner_peer_id.clone(),
                },
            };
            test.transport
                .inbound_msg(
                    gw_addr,
                    NetMessage::V1(NetMessageV1::Connect(initial_join_req)),
                )
                .await;
            tracing::debug!("Sent initial gw rejected reply");

            for i in 1..Ring::DEFAULT_MAX_HOPS_TO_LIVE {
                let port = i + 10;
                let addr = ([127, 0, port as u8, 1], port as u16).into();
                let acceptor = PeerKeyLocation {
                    location: Some(Location::from_address(&addr)),
                    peer: PeerId::new(addr, TransportKeypair::new().public().clone()),
                };
                tracing::info!(%acceptor, "Sending forward reply number {i} with status `{}`", i > 3);
                let forward_response = ConnectMsg::Response {
                    id: tx,
                    sender: gw_pkloc.clone(),
                    target: joiner_pkloc.clone(),
                    msg: ConnectResponse::AcceptedBy {
                        accepted: i > 3,
                        acceptor: acceptor.clone(),
                        joiner: joiner_peer_id.clone(),
                    },
                };
                test.transport
                    .inbound_msg(
                        gw_addr,
                        NetMessage::V1(NetMessageV1::Connect(forward_response.clone())),
                    )
                    .await;

                if i > 3 {
                    // Create the successful connection
                    async fn establish_conn(
                        test: &mut TestVerifier,
                        i: usize,
                        joiner_addr: SocketAddr,
                    ) -> Result<(), anyhow::Error> {
                        let (remote, ev) = tokio::time::timeout(
                            Duration::from_secs(10),
                            test.transport.outbound_recv.recv(),
                        )
                        .await
                        .inspect_err(|error| {
                            tracing::error!(%error, conn_num = %i, "failed while receiving connection events");
                        })
                        .map_err(|_| anyhow!("time out"))?
                        .ok_or( anyhow!("Failed to receive event"))?;
                        let ConnectionEvent::ConnectionStart {
                            open_connection, ..
                        } = ev;
                        let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
                        let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
                        let (conn, out, inb) = PeerConnection::new_remote_test(
                            remote,
                            joiner_addr,
                            out_symm_key,
                            in_symm_key.clone(),
                        );
                        test.transport
                            .packet_senders
                            .insert(remote, (in_symm_key, out));
                        test.transport.packet_receivers.push(inb);
                        tracing::info!(conn_num = %i, %remote, "Connection established at remote");
                        open_connection
                            .send(Ok(conn))
                            .map_err(|_| anyhow!("failed to open conn"))?;
                        tracing::info!(conn_num = %i, "Returned open conn");
                        Ok(())
                    }

                    establish_conn(&mut test, i, joiner_addr).await?;
                }
            }

            Ok::<_, anyhow::Error>(())
        };

        let peer_inbound = async {
            let mut conn_count = 0;
            let mut gw_rejected = false;
            for conn_num in 3..Ring::DEFAULT_MAX_HOPS_TO_LIVE {
                let conn_num = conn_num + 2;
                let event =
                    tokio::time::timeout(Duration::from_secs(60), handler.wait_for_events())
                        .await
                        .inspect_err(|_| {
                            tracing::error!(%conn_num, "failed while waiting for events");
                        })?
                        .inspect_err(|error| {
                            tracing::error!(%error, %conn_num, "failed while receiving events");
                        })?;
                match event {
                    Event::OutboundConnectionSuccessful { peer_id, .. } => {
                        tracing::info!(%peer_id, %conn_num, "Connection established at peer");
                        conn_count += 1;
                    }
                    Event::OutboundGatewayConnectionRejected { peer_id } => {
                        tracing::info!(%peer_id, "Gateway connection rejected");
                        assert_eq!(peer_id.addr, gw_addr);
                        gw_rejected = true;
                    }
                    other => bail!("Unexpected event: {:?}", other),
                }
            }
            tracing::debug!("Completed all checks, connection count: {conn_count}");
            assert!(gw_rejected);
            assert_eq!(conn_count, 6);
            Ok(())
        };
        futures::try_join!(test_controller, peer_inbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_peer_to_gw_outbound_conn_forwarded() -> anyhow::Result<()> {
        // crate::config::set_logger(Some(tracing::level_filters::LevelFilter::DEBUG), None);
        let joiner_addr = ([127, 0, 0, 1], 10001).into();
        let (mut handler, mut test) = config_handler(joiner_addr, None);

        let gw_key = TransportKeypair::new();
        let gw_pub_key = gw_key.public().clone();
        let gw_addr = ([127, 0, 0, 1], 10000).into();
        let gw_peer_id = PeerId::new(gw_addr, gw_pub_key.clone());
        let gw_pkloc = PeerKeyLocation {
            location: Some(Location::from_address(&gw_peer_id.addr)),
            peer: gw_peer_id.clone(),
        };

        let joiner_key = TransportKeypair::new();
        let joiner_pub_key = joiner_key.public().clone();
        let joiner_peer_id = PeerId::new(joiner_addr, joiner_pub_key.clone());
        let joiner_pkloc = PeerKeyLocation {
            peer: joiner_peer_id.clone(),
            location: Some(Location::from_address(&joiner_peer_id.addr)),
        };

        let tx = Transaction::new::<ConnectMsg>();

        let test_controller = async {
            let open_connection_peer =
                start_conn(&mut test, gw_addr, gw_pub_key.clone(), tx, true).await;
            test.transport
                .new_outbound_conn(gw_addr, open_connection_peer)
                .await;

            let msg = test.transport.recv_outbound_msg().await?;
            tracing::info!("Received connec request: {:?}", msg);
            let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                id,
                msg: ConnectRequest::StartJoinReq { .. },
                ..
            })) = msg
            else {
                panic!("unexpected message");
            };
            assert_eq!(id, tx);

            let initial_join_req = ConnectMsg::Response {
                id: tx,
                sender: gw_pkloc.clone(),
                target: joiner_pkloc.clone(),
                msg: ConnectResponse::AcceptedBy {
                    accepted: true,
                    acceptor: gw_pkloc.clone(),
                    joiner: joiner_peer_id.clone(),
                },
            };
            test.transport
                .inbound_msg(
                    gw_addr,
                    NetMessage::V1(NetMessageV1::Connect(initial_join_req)),
                )
                .await;
            tracing::debug!("Sent initial gw rejected reply");
            Ok::<_, anyhow::Error>(())
        };

        let peer_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            let _conn = match event {
                Event::OutboundGatewayConnectionSuccessful {
                    peer_id,
                    connection,
                    ..
                } => {
                    tracing::info!(%peer_id, "Gateway connection accepted");
                    assert_eq!(peer_id.addr, gw_addr);
                    connection
                }
                other => bail!("Unexpected event: {:?}", other),
            };
            Ok(())
        };

        futures::try_join!(test_controller, peer_inbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_peer_to_peer_outbound_conn_failed() -> anyhow::Result<()> {
        let addr: SocketAddr = ([127, 0, 0, 1], 10001).into();
        let (mut handler, mut test) = config_handler(addr, None);

        let peer_key = TransportKeypair::new();
        let peer_pub_key = peer_key.public().clone();
        let peer_addr = ([127, 0, 0, 2], 10002).into();

        let tx = Transaction::new::<ConnectMsg>();

        let test_controller = async {
            let open_connection =
                start_conn(&mut test, peer_addr, peer_pub_key.clone(), tx, false).await;
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
                    assert_eq!(peer_id.addr, peer_addr);
                    assert_eq!(peer_id.pub_key, peer_pub_key);
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
    async fn test_peer_to_peer_outbound_conn_succeeded() -> anyhow::Result<()> {
        let addr: SocketAddr = ([127, 0, 0, 1], 10001).into();
        let (mut handler, mut test) = config_handler(addr, None);

        let peer_key = TransportKeypair::new();
        let peer_pub_key = peer_key.public().clone();
        let peer_addr = ([127, 0, 0, 2], 10002).into();

        let tx = Transaction::new::<ConnectMsg>();

        let test_controller = async {
            let open_connection =
                start_conn(&mut test, peer_addr, peer_pub_key.clone(), tx, false).await;
            test.transport
                .new_outbound_conn(peer_addr, open_connection)
                .await;

            Ok::<_, anyhow::Error>(())
        };

        let peer_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(1), handler.wait_for_events()).await??;
            match event {
                Event::OutboundConnectionSuccessful {
                    peer_id,
                    connection,
                } => {
                    assert_eq!(peer_id.addr, peer_addr);
                    assert_eq!(peer_id.pub_key, peer_pub_key);
                    drop(connection);
                    Ok(())
                }
                other => bail!("Unexpected event: {:?}", other),
            }
        };

        futures::try_join!(test_controller, peer_inbound)?;
        Ok(())
    }
}
