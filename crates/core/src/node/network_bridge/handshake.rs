//! Handles initial connection handshake.
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::time::{timeout, Duration};
use tracing::Instrument;

use futures::{future::BoxFuture, stream::FuturesUnordered, Future, FutureExt, TryFutureExt};
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

const TIMEOUT: Duration = Duration::from_secs(30);

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
    #[error("connection error: {0}")]
    ConnectionError(#[from] super::ConnectionError),
}

#[derive(Debug)]
pub(super) enum Event {
    /// An inbound connection to a peer was successfully established at a gateway.
    InboundConnection {
        id: Transaction,
        conn: PeerConnection,
        joiner: PeerId,
        op: Option<Box<ConnectOp>>,
        forward_info: Option<Box<ForwardInfo>>,
        /// If true, this is a gateway bootstrap acceptance that should be registered immediately.
        /// See forward_conn() in connect.rs for full explanation.
        is_bootstrap: bool,
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

/// NOTE: This enum is no longer used but kept for reference during transition.
/// The Stream implementation infers the forward result from forward_conn's ConnectState.
#[allow(dead_code, clippy::large_enum_variant)]
enum ForwardResult {
    Forward(PeerId, NetMessage, ConnectivityInfo),
    DirectlyAccepted(ConnectivityInfo),
    /// Gateway bootstrap acceptance - connection should be registered immediately.
    /// See forward_conn() in connect.rs and PR #1871 for context.
    BootstrapAccepted(ConnectivityInfo),
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
    Dropped {
        peer: PeerId,
    },
    #[allow(dead_code)]
    DropConnectionByAddr(SocketAddr),
}

/// Used for communicating with the HandshakeHandler.
pub(super) struct HanshakeHandlerMsg(pub(crate) mpsc::Sender<ExternConnection>);

impl HanshakeHandlerMsg {
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

    pub async fn drop_connection(&self, remote: PeerId) -> Result<()> {
        self.0
            .send(ExternConnection::Dropped { peer: remote })
            .await
            .map_err(|_| HandshakeError::ChannelClosed)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn drop_connection_by_addr(&self, remote_addr: SocketAddr) -> Result<()> {
        self.0
            .send(ExternConnection::DropConnectionByAddr(remote_addr))
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

    /// If set, will sent the location over network messages.
    ///
    /// It will also determine whether to trust the location of peers sent in network messages or derive them from IP.
    ///
    /// This is used for testing deterministically with given location. In production this should always be none
    /// and locations should be derived from IP addresses.
    this_location: Option<Location>,

    /// Whether this node is a gateway
    is_gateway: bool,

    /// Indicates when peer is ready to process client operations (peer_id has been set).
    /// Only used for non-gateway peers - set to Some(flag) for regular peers, None for gateways
    peer_ready: Option<Arc<AtomicBool>>,
}

impl HandshakeHandler {
    pub fn new(
        inbound_conn_handler: InboundConnectionHandler,
        outbound_conn_handler: OutboundConnectionHandler,
        connection_manager: ConnectionManager,
        router: Arc<RwLock<Router>>,
        this_location: Option<Location>,
        is_gateway: bool,
        peer_ready: Option<Arc<AtomicBool>>,
    ) -> (Self, HanshakeHandlerMsg, OutboundMessage) {
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
            this_location,
            is_gateway,
            peer_ready,
        };
        (
            connector,
            HanshakeHandlerMsg(establish_connection_tx),
            OutboundMessage(pending_msg_tx),
        )
    }

    /// Tracks a new inbound connection and sets up message handling for it.
    fn track_inbound_connection(&mut self, conn: PeerConnection) {
        let (outbound_msg_sender, outbound_msg_recv) = mpsc::channel(100);
        let remote = conn.remote_addr();
        tracing::debug!(%remote, "Tracking inbound connection - spawning gw_peer_connection_listener");
        let f = gw_peer_connection_listener(conn, PeerOutboundMessage(outbound_msg_recv)).boxed();
        self.unconfirmed_inbound_connections.push(f);
        self.outbound_messages.insert(remote, outbound_msg_sender);
        tracing::debug!(%remote, "Inbound connection tracked - unconfirmed count: {}", self.unconfirmed_inbound_connections.len());
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
                Err(e) => {
                    tracing::debug!(%remote, "failed to establish outbound connection: {e}");
                    Err((remote, e.into()))
                }
            })
            .boxed();
        self.ongoing_outbound_connections.push(f);
    }
}

/// Stream wrapper that takes ownership of HandshakeHandler and implements Stream properly.
/// This converts the event loop logic from wait_for_events into a proper Stream implementation.
pub(super) struct HandshakeEventStream {
    handler: HandshakeHandler,
}

impl HandshakeEventStream {
    pub fn new(handler: HandshakeHandler) -> Self {
        Self { handler }
    }
}

impl futures::stream::Stream for HandshakeEventStream {
    type Item = Result<Event, HandshakeError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        let handler = &mut self.handler;

        // Main event loop - mirrors the original `loop { tokio::select! {...} }` structure
        // We loop internally to handle "continue" cases without returning to the executor
        loop {
            tracing::trace!(
                "HandshakeEventStream::poll_next iteration - unconfirmed: {}, ongoing_outbound: {}",
                handler.unconfirmed_inbound_connections.len(),
                handler.ongoing_outbound_connections.len()
            );

            // Priority 1: Handle new inbound connections
            // Poll the future and extract the result, then drop it before using handler again
            let inbound_result = {
                let inbound_fut = handler.inbound_conn_handler.next_connection();
                tokio::pin!(inbound_fut);
                inbound_fut.poll(cx)
            }; // inbound_fut dropped here

            match inbound_result {
                Poll::Ready(Some(conn)) => {
                    tracing::debug!(from=%conn.remote_addr(), "New inbound connection");
                    handler.track_inbound_connection(conn);
                    // This was a `continue` in the loop - loop again to re-poll all priorities
                    continue;
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Some(Err(HandshakeError::ChannelClosed)));
                }
                Poll::Pending => {}
            }

            // Priority 2: Process outbound connection attempts
            if !handler.ongoing_outbound_connections.is_empty() {
                match std::pin::Pin::new(&mut handler.ongoing_outbound_connections).poll_next(cx) {
                    Poll::Ready(Some(outbound_result)) => {
                        // Handle the result - may return event or continue
                        let result = handle_outbound_result(handler, outbound_result, cx);
                        if let Some(event) = result {
                            return Poll::Ready(Some(event));
                        } else {
                            // Was a continue case - loop again to re-poll all priorities
                            continue;
                        }
                    }
                    Poll::Ready(None) => {
                        // FuturesUnordered is now empty - this is normal, just continue to next channel
                    }
                    Poll::Pending => {}
                }
            }

            // Priority 3: Handle unconfirmed inbound connections (for gateways)
            if !handler.unconfirmed_inbound_connections.is_empty() {
                match std::pin::Pin::new(&mut handler.unconfirmed_inbound_connections).poll_next(cx)
                {
                    Poll::Ready(Some(res)) => {
                        tracing::debug!("Processing unconfirmed inbound connection");
                        let (event, outbound_sender) = match res {
                            Ok(v) => v,
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        };
                        tracing::debug!("Unconfirmed connection event: {:?}", event);
                        let result =
                            handle_unconfirmed_inbound(handler, event, outbound_sender, cx);
                        if let Some(event) = result {
                            return Poll::Ready(Some(event));
                        } else {
                            // Was a continue case - loop again to re-poll all priorities
                            continue;
                        }
                    }
                    Poll::Ready(None) => {
                        // FuturesUnordered is now empty - this is normal, just continue to next channel
                    }
                    Poll::Pending => {}
                }
            }

            // Priority 4: Handle outbound message requests
            match handler.pending_msg_rx.poll_recv(cx) {
                Poll::Ready(Some((addr, msg))) => {
                    // Call handler.outbound() - this returns Option<Event>
                    // Scope to drop the future borrow immediately
                    let result = {
                        let outbound_fut = handler.outbound(addr, msg);
                        tokio::pin!(outbound_fut);
                        outbound_fut.poll(cx)
                    };
                    match result {
                        Poll::Ready(Some(event)) => {
                            return Poll::Ready(Some(Ok(event)));
                        }
                        Poll::Ready(None) => {
                            // outbound() returned None - continue to re-poll all priorities
                            continue;
                        }
                        Poll::Pending => {
                            // The outbound future is pending - continue to next priority
                        }
                    }
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Some(Err(HandshakeError::ChannelClosed)));
                }
                Poll::Pending => {}
            }

            // Priority 5: Handle connection establishment requests
            match handler.establish_connection_rx.poll_recv(cx) {
                Poll::Ready(Some(ExternConnection::Establish { peer, tx, is_gw })) => {
                    // Start outbound connection - call the async method
                    // Scope to drop the future borrow immediately
                    let _ = {
                        let start_fut = handler.start_outbound_connection(peer, tx, is_gw);
                        tokio::pin!(start_fut);
                        start_fut.poll(cx)
                    };
                    // Poll it immediately - it will push futures to ongoing_outbound_connections
                    // Then loop again to re-poll all priorities (ongoing_outbound_connections might have work)
                    continue;
                }
                Poll::Ready(Some(ExternConnection::Dropped { peer })) => {
                    handler.connected.remove(&peer.addr);
                    handler.outbound_messages.remove(&peer.addr);
                    handler.connecting.remove(&peer.addr);
                    // Continue to re-poll all priorities
                    continue;
                }
                Poll::Ready(Some(ExternConnection::DropConnectionByAddr(addr))) => {
                    handler.connected.remove(&addr);
                    handler.outbound_messages.remove(&addr);
                    handler.connecting.remove(&addr);
                    // Continue to re-poll all priorities
                    continue;
                }
                Poll::Ready(None) => {
                    return Poll::Ready(Some(Err(HandshakeError::ChannelClosed)));
                }
                Poll::Pending => {}
            }

            // All channels are pending - return Pending and wait to be woken
            return Poll::Pending;
        } // end of loop
    }
}

// Helper to handle outbound connection results
// Returns Some(event) if should return an event, None if should continue
fn handle_outbound_result(
    handler: &mut HandshakeHandler,
    result: OutboundConnResult,
    cx: &mut std::task::Context<'_>,
) -> Option<Result<Event, HandshakeError>> {
    match result {
        Ok(InternalEvent::OutboundConnEstablished(peer_id, connection)) => {
            tracing::info!(at=?connection.my_address(), from=%connection.remote_addr(), "Outbound connection successful");
            Some(Ok(Event::OutboundConnectionSuccessful {
                peer_id,
                connection,
            }))
        }
        Ok(InternalEvent::OutboundGwConnEstablished(id, connection)) => {
            tracing::info!(at=?connection.my_address(), from=%connection.remote_addr(), "Outbound gateway connection successful");
            if let Some(addr) = connection.my_address() {
                tracing::debug!(%addr, "Attempting setting own peer key");
                handler.connection_manager.try_set_peer_key(addr);

                if let Some(ref peer_ready) = handler.peer_ready {
                    peer_ready.store(true, std::sync::atomic::Ordering::SeqCst);
                    tracing::info!("Peer initialization complete: peer_ready set to true, client operations now enabled");
                }

                if handler.this_location.is_none() {
                    handler
                        .connection_manager
                        .update_location(Some(Location::from_address(&addr)));
                }
            }
            tracing::debug!(at=?connection.my_address(), from=%connection.remote_addr(), "Outbound connection to gw successful");

            // Call wait_for_gw_confirmation - it pushes a future to ongoing_outbound_connections
            let tx = match handler.connecting.get(&id.addr) {
                Some(t) => *t,
                None => {
                    tracing::error!("Transaction not found for gateway connection");
                    return Some(Err(HandshakeError::ConnectionClosed(
                        connection.remote_addr(),
                    )));
                }
            };
            let this_peer = handler.connection_manager.own_location().peer;
            tracing::debug!(at=?connection.my_address(), %this_peer.addr, from=%connection.remote_addr(), remote_addr = %id, "Waiting for confirmation from gw");
            handler.ongoing_outbound_connections.push(
                wait_for_gw_confirmation(
                    (this_peer, handler.this_location),
                    AcceptedTracker {
                        gw_peer: id.into(),
                        gw_conn: connection,
                        gw_accepted: false,
                        gw_accepted_processed: false,
                        remaining_checks: Ring::DEFAULT_MAX_HOPS_TO_LIVE,
                        accepted: 0,
                        total_checks: Ring::DEFAULT_MAX_HOPS_TO_LIVE,
                        tx,
                    },
                )
                .boxed(),
            );
            None // Continue
        }
        Ok(InternalEvent::FinishedOutboundConnProcess(tracker)) => {
            handler.connecting.remove(&tracker.gw_peer.peer.addr);
            tracing::debug!(at=?tracker.gw_conn.my_address(), gw=%tracker.gw_conn.remote_addr(), "Done checking, connection not accepted by gw, dropping connection");
            Some(Ok(Event::OutboundGatewayConnectionRejected {
                peer_id: tracker.gw_peer.peer,
            }))
        }
        Ok(InternalEvent::OutboundGwConnConfirmed(tracker)) => {
            tracing::debug!(at=?tracker.gw_conn.my_address(), from=%tracker.gw_conn.remote_addr(), "Outbound connection to gw confirmed");
            handler.connected.insert(tracker.gw_conn.remote_addr());
            handler.connecting.remove(&tracker.gw_conn.remote_addr());
            Some(Ok(Event::OutboundGatewayConnectionSuccessful {
                peer_id: tracker.gw_peer.peer,
                connection: tracker.gw_conn,
                remaining_checks: tracker.remaining_checks,
            }))
        }
        Ok(InternalEvent::NextCheck(tracker)) => {
            handler
                .ongoing_outbound_connections
                .push(check_remaining_hops(tracker).boxed());
            None // Continue
        }
        Ok(InternalEvent::RemoteConnectionAttempt { remote, tracker }) => {
            debug_assert!(!tracker.gw_accepted);
            tracing::debug!(
                at=?tracker.gw_conn.my_address(),
                gw=%tracker.gw_conn.remote_addr(),
                "Attempting remote connection to {remote}"
            );

            // Start outbound connection - poll it immediately to start the work
            let _result = {
                let start_fut =
                    handler.start_outbound_connection(remote.clone(), tracker.tx, false);
                tokio::pin!(start_fut);
                start_fut.poll(cx)
            };

            // Whether it completes or pends, push check_remaining_hops
            let current_span = tracing::Span::current();
            let checking_hops_span = tracing::info_span!(parent: current_span, "checking_hops");
            handler.ongoing_outbound_connections.push(
                check_remaining_hops(tracker)
                    .instrument(checking_hops_span)
                    .boxed(),
            );
            None // Continue
        }
        Ok(InternalEvent::DropInboundConnection(addr)) => {
            handler.connecting.remove(&addr);
            handler.outbound_messages.remove(&addr);
            None // Continue
        }
        Err((peer_id, error)) => {
            tracing::debug!(from=%peer_id.addr, "Outbound connection failed: {error}");
            handler.connecting.remove(&peer_id.addr);
            handler.outbound_messages.remove(&peer_id.addr);
            handler.connection_manager.prune_alive_connection(&peer_id);
            Some(Ok(Event::OutboundConnectionFailed { peer_id, error }))
        }
        Ok(other) => {
            tracing::error!("Unexpected event: {other:?}");
            None // Continue
        }
    }
}

// Helper to handle unconfirmed inbound events
// Returns Some(event) if should return, None if should continue
fn handle_unconfirmed_inbound(
    handler: &mut HandshakeHandler,
    event: InternalEvent,
    outbound_sender: PeerOutboundMessage,
    _cx: &mut std::task::Context<'_>,
) -> Option<Result<Event, HandshakeError>> {
    match event {
        InternalEvent::InboundGwJoinRequest(req) => {
            // This requires async work - spawn it as a future
            let conn_manager = handler.connection_manager.clone();
            let router = handler.router.clone();
            let this_location = handler.this_location;
            let is_gateway = handler.is_gateway;

            // Spawn the async handling
            let fut = handle_inbound_gw_join_request(
                req,
                conn_manager,
                router,
                this_location,
                is_gateway,
                outbound_sender,
            );

            handler.unconfirmed_inbound_connections.push(fut.boxed());
            None
        }
        InternalEvent::InboundConnectionAccepted {
            id,
            conn,
            joiner,
            op,
            forward_info,
            is_bootstrap,
        } => {
            tracing::debug!(%joiner, "Inbound connection accepted");
            // The outbound sender was already stored in outbound_messages by track_inbound_connection
            // We just need to return the event
            Some(Ok(Event::InboundConnection {
                id,
                conn,
                joiner,
                op,
                forward_info,
                is_bootstrap,
            }))
        }
        InternalEvent::InboundConnectionRejected { peer_id, remote } => {
            tracing::debug!(%peer_id, %remote, "Inbound connection rejected");
            handler.outbound_messages.remove(&remote);
            handler.connecting.remove(&remote);
            Some(Ok(Event::InboundConnectionRejected { peer_id }))
        }
        InternalEvent::TransientForward {
            conn,
            tx,
            info,
            target,
            forward_to,
            msg,
        } => {
            tracing::debug!(%target, %forward_to, "Transient forward");
            // Save transaction ID before moving tx
            let transaction_id = tx.tx;
            // Push gw_transient_peer_conn future to monitor this connection
            handler
                .unconfirmed_inbound_connections
                .push(gw_transient_peer_conn(conn, outbound_sender, tx, info).boxed());
            Some(Ok(Event::TransientForwardTransaction {
                target,
                tx: transaction_id,
                forward_to,
                msg,
            }))
        }
        InternalEvent::DropInboundConnection(addr) => {
            tracing::debug!(%addr, "Dropping inbound connection");
            handler.outbound_messages.remove(&addr);
            None
        }
        _ => {
            tracing::warn!("Unhandled unconfirmed inbound event: {:?}", event);
            None
        }
    }
}

// Async function to handle InboundGwJoinRequest
async fn handle_inbound_gw_join_request(
    mut req: InboundGwJoinRequest,
    conn_manager: ConnectionManager,
    router: Arc<RwLock<Router>>,
    this_location: Option<Location>,
    is_gateway: bool,
    outbound_sender: PeerOutboundMessage,
) -> Result<(InternalEvent, PeerOutboundMessage), HandshakeError> {
    let location = if let Some((_, other)) = this_location.zip(req.location) {
        other
    } else {
        Location::from_address(&req.conn.remote_addr())
    };

    let should_accept = conn_manager.should_accept(location, &req.joiner);
    let can_accept = should_accept && (is_gateway || conn_manager.num_connections() > 0);

    if can_accept {
        // Accepted connection path: Send acceptance message, then forward
        let accepted_msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
            id: req.id,
            sender: conn_manager.own_location(),
            target: PeerKeyLocation {
                peer: req.joiner.clone(),
                location: Some(location),
            },
            msg: ConnectResponse::AcceptedBy {
                accepted: true,
                acceptor: conn_manager.own_location(),
                joiner: req.joiner.clone(),
            },
        }));

        tracing::debug!(at=?req.conn.my_address(), from=%req.conn.remote_addr(), "Accepting connection");

        if let Err(e) = req.conn.send(accepted_msg).await {
            tracing::error!(%e, "Failed to send accepted message from gw, pruning reserved connection");
            conn_manager.prune_in_transit_connection(&req.joiner);
            return Err(e.into());
        }

        let InboundGwJoinRequest {
            conn,
            id,
            hops_to_live,
            max_hops_to_live,
            skip_connections,
            skip_forwards,
            joiner,
            ..
        } = req;

        // Forward the connection
        let mut nw_bridge = ForwardPeerMessage {
            msg: parking_lot::Mutex::new(None),
        };

        let my_peer_id = conn_manager.own_location();
        let joiner_pk_loc = PeerKeyLocation {
            peer: joiner.clone(),
            location: Some(location),
        };

        let mut skip_connections = skip_connections.clone();
        let mut skip_forwards = skip_forwards.clone();
        skip_connections.insert(my_peer_id.peer.clone());
        skip_forwards.insert(my_peer_id.peer.clone());

        let forward_info = ForwardParams {
            left_htl: hops_to_live,
            max_htl: max_hops_to_live,
            accepted: true,
            skip_connections,
            skip_forwards,
            req_peer: my_peer_id.clone(),
            joiner: joiner_pk_loc.clone(),
            is_gateway,
        };

        match forward_conn(
            id,
            &conn_manager,
            router.clone(),
            &mut nw_bridge,
            forward_info,
        )
        .await
        {
            Err(err) => {
                tracing::error!(%err, "Error forwarding connection");
                // Continue by returning DropInboundConnection
                Ok((
                    InternalEvent::DropInboundConnection(conn.remote_addr()),
                    outbound_sender,
                ))
            }
            Ok(Some(conn_state)) => {
                let ConnectState::AwaitingConnectivity(info) = conn_state else {
                    unreachable!("forward_conn should return AwaitingConnectivity if successful")
                };

                tracing::info!(%id, %joiner, "Creating InboundConnection event");

                // Check if we have a forward message (forwarding) or not (direct acceptance)
                let (op, forward_info_opt, is_bootstrap) =
                    if let Some((forward_target, msg)) = nw_bridge.msg.into_inner() {
                        (
                            Some(Box::new(ConnectOp::new(
                                id,
                                Some(ConnectState::AwaitingConnectivity(info)),
                                None,
                                None,
                            ))),
                            Some(Box::new(ForwardInfo {
                                target: forward_target,
                                msg,
                            })),
                            false,
                        )
                    } else if info.is_bootstrap_acceptance {
                        // Gateway bootstrap case: connection should be registered immediately
                        (
                            Some(Box::new(ConnectOp::new(
                                id,
                                Some(ConnectState::AwaitingConnectivity(info)),
                                None,
                                None,
                            ))),
                            None,
                            true,
                        )
                    } else {
                        // Normal direct acceptance - will wait for CheckConnectivity
                        (
                            Some(Box::new(ConnectOp::new(
                                id,
                                Some(ConnectState::AwaitingConnectivity(info)),
                                None,
                                None,
                            ))),
                            None,
                            false,
                        )
                    };

                Ok((
                    InternalEvent::InboundConnectionAccepted {
                        id,
                        conn,
                        joiner,
                        op,
                        forward_info: forward_info_opt,
                        is_bootstrap,
                    },
                    outbound_sender,
                ))
            }
            Ok(None) => {
                // No forwarding target found - return event with op: None to signal rejection
                // This matches original behavior where forward_result (None, _) returns Event with op: None
                Ok((
                    InternalEvent::InboundConnectionAccepted {
                        id,
                        conn,
                        joiner,
                        op: None, // Signals rejection/no forwarding possible
                        forward_info: None,
                        is_bootstrap: false,
                    },
                    outbound_sender,
                ))
            }
        }
    } else {
        // Transient connection path: Try to forward without accepting
        // If should_accept was true but we can't actually accept (non-gateway with 0 connections),
        // we need to clean up the reserved connection
        if should_accept && !can_accept {
            conn_manager.prune_in_transit_connection(&req.joiner);
            tracing::debug!(
                "Non-gateway with 0 connections cannot accept connection from {:?}",
                req.joiner
            );
        }

        let InboundGwJoinRequest {
            mut conn,
            id,
            hops_to_live,
            max_hops_to_live,
            skip_connections,
            skip_forwards,
            joiner,
            ..
        } = req;

        let remote = conn.remote_addr();
        tracing::debug!(at=?conn.my_address(), from=%remote, "Transient connection");

        // Try to forward the connection without accepting it
        let joiner_loc = this_location.unwrap_or_else(|| Location::from_address(&remote));
        let joiner_pk_loc = PeerKeyLocation {
            peer: joiner.clone(),
            location: Some(joiner_loc),
        };
        let my_peer_id = conn_manager.own_location();

        let mut skip_connections_updated = skip_connections.clone();
        let mut skip_forwards_updated = skip_forwards.clone();
        skip_connections_updated.insert(joiner.clone());
        skip_forwards_updated.insert(joiner.clone());
        skip_connections_updated.insert(my_peer_id.peer.clone());
        skip_forwards_updated.insert(my_peer_id.peer.clone());

        let forward_info = ForwardParams {
            left_htl: hops_to_live,
            max_htl: max_hops_to_live,
            accepted: true,
            skip_connections: skip_connections_updated,
            skip_forwards: skip_forwards_updated,
            req_peer: my_peer_id.clone(),
            joiner: joiner_pk_loc.clone(),
            is_gateway,
        };

        let mut nw_bridge = ForwardPeerMessage {
            msg: parking_lot::Mutex::new(None),
        };

        match forward_conn(
            id,
            &conn_manager,
            router.clone(),
            &mut nw_bridge,
            forward_info,
        )
        .await
        {
            Ok(Some(conn_state)) => {
                let ConnectState::AwaitingConnectivity(info) = conn_state else {
                    unreachable!("forward_conn should return AwaitingConnectivity if successful")
                };

                // Check the forwarding result
                if let Some((forward_target, msg)) = nw_bridge.msg.into_inner() {
                    // Successfully forwarding to another peer
                    // Create a TransientConnection to track this
                    let tx = TransientConnection {
                        tx: id,
                        joiner: joiner.clone(),
                    };

                    // Push gw_transient_peer_conn future to monitor this connection
                    Ok((
                        InternalEvent::TransientForward {
                            conn,
                            tx,
                            info,
                            target: remote,
                            forward_to: forward_target,
                            msg: Box::new(msg),
                        },
                        outbound_sender,
                    ))
                } else if info.is_bootstrap_acceptance {
                    // Bootstrap acceptance - accept it directly even though we didn't send acceptance yet
                    Ok((
                        InternalEvent::InboundConnectionAccepted {
                            id,
                            conn,
                            joiner,
                            op: Some(Box::new(ConnectOp::new(
                                id,
                                Some(ConnectState::AwaitingConnectivity(info)),
                                None,
                                None,
                            ))),
                            forward_info: None,
                            is_bootstrap: true,
                        },
                        outbound_sender,
                    ))
                } else {
                    // Direct acceptance without forwarding - shouldn't happen for transient
                    // Clean up and reject
                    conn_manager.prune_in_transit_connection(&joiner);
                    Ok((
                        InternalEvent::InboundConnectionRejected {
                            peer_id: joiner,
                            remote,
                        },
                        outbound_sender,
                    ))
                }
            }
            Ok(None) => {
                // No peer to forward to - send rejection message
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Rejecting connection, no peers found to forward");
                let reject_msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                    id,
                    sender: my_peer_id.clone(),
                    target: joiner_pk_loc,
                    msg: ConnectResponse::AcceptedBy {
                        accepted: false,
                        acceptor: my_peer_id,
                        joiner: joiner.clone(),
                    },
                }));

                if let Err(e) = conn.send(reject_msg).await {
                    tracing::error!(%e, "Failed to send rejection message");
                    return Err(e.into());
                }

                // Clean up and reject
                conn_manager.prune_in_transit_connection(&joiner);
                Ok((
                    InternalEvent::InboundConnectionRejected {
                        peer_id: joiner,
                        remote,
                    },
                    outbound_sender,
                ))
            }
            Err(e) => {
                tracing::error!(from=%remote, "Error forwarding transient connection: {e}");
                // Drop the connection and clean up
                conn_manager.prune_in_transit_connection(&joiner);
                Ok((
                    InternalEvent::DropInboundConnection(remote),
                    outbound_sender,
                ))
            }
        }
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
            unreachable!("drop_connection should not be called on ForwardPeerMessage")
        }
        Ok(())
    }
}

#[derive(Debug)]
struct InboundGwJoinRequest {
    conn: PeerConnection,
    id: Transaction,
    joiner: PeerId,
    location: Option<Location>,
    hops_to_live: usize,
    max_hops_to_live: usize,
    skip_connections: HashSet<PeerId>,
    skip_forwards: HashSet<PeerId>,
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
    // New variants for forwarding results
    InboundConnectionAccepted {
        id: Transaction,
        conn: PeerConnection,
        joiner: PeerId,
        op: Option<Box<ConnectOp>>,
        forward_info: Option<Box<ForwardInfo>>,
        is_bootstrap: bool,
    },
    InboundConnectionRejected {
        peer_id: PeerId,
        remote: SocketAddr,
    },
    TransientForward {
        conn: PeerConnection,
        tx: TransientConnection,
        info: ConnectivityInfo,
        target: SocketAddr,
        forward_to: PeerId,
        msg: Box<NetMessage>,
    },
}

#[repr(transparent)]
#[derive(Debug)]
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
    (this_peer, this_location): (PeerId, Option<Location>),
    mut tracker: AcceptedTracker,
) -> OutboundConnResult {
    let gw_peer_id = tracker.gw_peer.peer.clone();
    let msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
        id: tracker.tx,
        target: tracker.gw_peer.clone(),
        msg: ConnectRequest::StartJoinReq {
            joiner: Some(this_peer.clone()),
            joiner_key: this_peer.pub_key.clone(),
            joiner_location: this_location,
            hops_to_live: tracker.total_checks,
            max_hops_to_live: tracker.total_checks,
            skip_connections: HashSet::from([this_peer.clone()]),
            skip_forwards: HashSet::from([this_peer.clone()]),
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
    tracing::debug!(from=%conn.remote_addr(), "Starting gw_peer_connection_listener");
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
                let net_message = match decode_msg(&msg) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::error!(
                            at=?conn.my_address(),
                            from=%conn.remote_addr(),
                            error=%e,
                            "Failed to decode message - closing connection"
                        );
                        break Err(HandshakeError::ConnectionClosed(conn.remote_addr()));
                    }
                };
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), %net_message, "Received message from peer");
                match net_message {
                    NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                        id,
                        msg: ConnectRequest::StartJoinReq {
                            joiner,
                            joiner_key,
                            hops_to_live,
                            max_hops_to_live,
                            skip_connections,
                            skip_forwards,
                            joiner_location
                        },
                        ..
                    })) => {
                        let joiner = joiner.unwrap_or_else(|| {
                            tracing::debug!(%joiner_key, "Joiner not provided, using joiner key");
                            PeerId::new(conn.remote_addr(), joiner_key)
                        });
                        break Ok((
                            InternalEvent::InboundGwJoinRequest(InboundGwJoinRequest {
                                conn,
                                id,
                                joiner,
                                location: joiner_location,
                                hops_to_live,
                                max_hops_to_live,
                                skip_connections,
                                skip_forwards,
                            }),
                            outbound,
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
                        let net_msg = match decode_msg(&msg) {
                            Ok(msg) => msg,
                            Err(e) => {
                                tracing::error!(
                                    at=?conn.my_address(),
                                    from=%conn.remote_addr(),
                                    error=%e,
                                    "Failed to decode message from transient peer - closing connection"
                                );
                                break Err(HandshakeError::ConnectionClosed(conn.remote_addr()));
                            }
                        };
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
                                unreachable!("Expected ConnectResponse::AcceptedBy after matches! guard")
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
                            if info.decrement_check() { // this means all checks have been performed
                                break Ok((InternalEvent::DropInboundConnection(conn.remote_addr()), outbound));
                            } else { // still waiting for more checks
                                continue;
                            }
                        }
                        // other messages are just forwarded
                        conn.send(msg).await?;
                    }
                    Ok(None) => {
                        tracing::debug!("Outbound channel closed for transient connection");
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

/// Tracks a transient connection that is being forwarded through this gateway.
/// This struct is only used by `gw_transient_peer_conn` to identify and validate
/// drop connection messages from the joiner.
///
/// Note: In the original implementation, this struct also contained `max_hops_to_live`,
/// `hops_to_live`, `skip_connections`, and `skip_forwards` fields that were used by
/// the `forward_transient_connection` method. In the stream-based refactoring, these
/// values are used directly from the `InboundGwJoinRequest` when calling `forward_conn`,
/// so they don't need to be stored in this struct.
#[derive(Debug)]
struct TransientConnection {
    tx: Transaction,
    joiner: PeerId,
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
    use core::panic;
    use std::{fmt::Display, sync::Arc, time::Duration};

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
        /// Outbount messages to peers
        packet_senders:
            HashMap<SocketAddr, (Aes128Gcm, mpsc::Sender<PacketData<UnknownEncryption>>)>,
        /// Next packet id to use
        packet_id: u32,
        /// Inbound messages from peers
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
            let initial_join_req = ConnectMsg::Request {
                id,
                target: target_peer,
                msg: ConnectRequest::StartJoinReq {
                    joiner: None,
                    joiner_key: pub_key,
                    joiner_location: None,
                    hops_to_live,
                    max_hops_to_live: hops_to_live,
                    skip_connections: HashSet::new(),
                    skip_forwards: HashSet::new(),
                },
            };
            self.inbound_msg(
                addr,
                NetMessage::V1(NetMessageV1::Connect(initial_join_req)),
            )
            .await
        }

        async fn inbound_msg(&mut self, addr: SocketAddr, msg: impl Serialize + Display) {
            tracing::debug!(at=?self.my_addr, to=%addr, "Sending message from peer");
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
            tracing::trace!(at=?self.my_addr, to=%addr, "Message sent");
            self.packet_id += 1;
        }

        async fn recv_outbound_msg(&mut self) -> anyhow::Result<NetMessage> {
            let receiver = &mut self.packet_receivers[0];
            let (_, msg) = receiver
                .recv()
                .await
                .ok_or_else(|| anyhow::Error::msg("Failed to receive packet"))?;
            let packet: PacketData<UnknownEncryption> = PacketData::from_buf(&*msg);
            let packet = packet
                .try_decrypt_sym(&self.in_key)
                .map_err(|_| anyhow!("Failed to decrypt packet"))?;
            let msg: SymmetricMessage = bincode::deserialize(packet.data()).unwrap();
            let payload = match msg {
                SymmetricMessage {
                    payload: SymmetricMessagePayload::ShortMessage { payload },
                    ..
                } => payload,
                SymmetricMessage {
                    payload:
                        SymmetricMessagePayload::StreamFragment {
                            total_length_bytes,
                            mut payload,
                            ..
                        },
                    ..
                } => {
                    let mut remaining = total_length_bytes as usize - payload.len();
                    while remaining > 0 {
                        let (_, msg) = receiver
                            .recv()
                            .await
                            .ok_or_else(|| anyhow::Error::msg("Failed to receive packet"))?;
                        let packet: PacketData<UnknownEncryption> = PacketData::from_buf(&*msg);
                        let packet = packet
                            .try_decrypt_sym(&self.in_key)
                            .map_err(|_| anyhow!("Failed to decrypt packet"))?;
                        let msg: SymmetricMessage = bincode::deserialize(packet.data()).unwrap();
                        match msg {
                            SymmetricMessage {
                                payload:
                                    SymmetricMessagePayload::StreamFragment { payload: new, .. },
                                ..
                            } => {
                                payload.extend_from_slice(&new);
                                remaining -= new.len();
                            }
                            _ => panic!("Unexpected message type"),
                        }
                    }
                    payload
                }
                _ => panic!("Unexpected message type"),
            };
            let msg: NetMessage = bincode::deserialize(&payload).unwrap();
            Ok(msg)
        }
    }

    struct NodeMock {
        establish_conn: HanshakeHandlerMsg,
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
        is_gateway: bool,
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
            None,
            is_gateway,
            None, // test code doesn't need peer_ready
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

    // ============================================================================
    // Stream-based tests for HandshakeEventStream
    // ============================================================================

    /// Helper to get the next event from a HandshakeEventStream
    async fn next_stream_event(stream: &mut HandshakeEventStream) -> Result<Event, HandshakeError> {
        use futures::StreamExt;
        stream.next().await.ok_or(HandshakeError::ChannelClosed)?
    }

    #[tokio::test]
    async fn test_stream_gateway_inbound_conn_success() -> anyhow::Result<()> {
        let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
        let (handler, mut test) = config_handler(addr, None, true);
        let mut stream = HandshakeEventStream::new(handler);

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
                tokio::time::timeout(Duration::from_secs(15), next_stream_event(&mut stream))
                    .await??;
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
    async fn test_stream_gateway_inbound_conn_rejected() -> anyhow::Result<()> {
        let addr: SocketAddr = ([127, 0, 0, 1], 10000).into();
        let (handler, mut test) = config_handler(addr, None, true);
        let mut stream = HandshakeEventStream::new(handler);

        let remote_addr = ([127, 0, 0, 1], 10001).into();
        let remote_pub_key = TransportKeypair::new().public().clone();
        let test_controller = async {
            test.transport.new_conn(remote_addr).await;
            test.transport
                .establish_inbound_conn(remote_addr, remote_pub_key.clone(), None)
                .await;

            // Reject the connection
            let sender_key = TransportKeypair::new().public().clone();
            let acceptor_key = TransportKeypair::new().public().clone();
            let joiner_key = TransportKeypair::new().public().clone();
            let response = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response {
                id: Transaction::new::<ConnectMsg>(),
                sender: PeerKeyLocation {
                    peer: PeerId::new(addr, sender_key),
                    location: Some(Location::random()),
                },
                target: PeerKeyLocation {
                    peer: PeerId::new(remote_addr, remote_pub_key),
                    location: Some(Location::random()),
                },
                msg: ConnectResponse::AcceptedBy {
                    accepted: false,
                    acceptor: PeerKeyLocation {
                        peer: PeerId::new(addr, acceptor_key),
                        location: Some(Location::random()),
                    },
                    joiner: PeerId::new(remote_addr, joiner_key),
                },
            }));

            test.transport.inbound_msg(remote_addr, response).await;
            Ok::<_, anyhow::Error>(())
        };

        let gw_inbound = async {
            // First event: InboundConnection (may be accepted or rejected depending on routing)
            let event =
                tokio::time::timeout(Duration::from_secs(15), next_stream_event(&mut stream))
                    .await??;
            tracing::info!("Received event: {:?}", event);
            Ok(())
        };
        futures::try_join!(test_controller, gw_inbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_peer_to_gw_outbound_conn() -> anyhow::Result<()> {
        let addr: SocketAddr = ([127, 0, 0, 1], 10001).into();
        let (handler, mut test) = config_handler(addr, None, false);
        let mut stream = HandshakeEventStream::new(handler);

        let joiner_key = TransportKeypair::new();
        let pub_key = joiner_key.public().clone();
        let id = Transaction::new::<ConnectMsg>();
        let remote_addr: SocketAddr = ([127, 0, 0, 2], 10002).into();

        let test_controller = async {
            let open_connection =
                start_conn(&mut test, remote_addr, pub_key.clone(), id, true).await;
            test.transport
                .new_outbound_conn(remote_addr, open_connection)
                .await;
            tracing::debug!("Outbound connection established");

            // Wait for and respond to StartJoinReq
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

        let peer_outbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(15), next_stream_event(&mut stream))
                    .await??;
            match event {
                Event::OutboundGatewayConnectionSuccessful {
                    peer_id,
                    connection,
                    ..
                } => {
                    assert_eq!(peer_id.addr, remote_addr);
                    assert_eq!(peer_id.pub_key, pub_key);
                    drop(connection);
                    Ok(())
                }
                other => bail!("Unexpected event: {:?}", other),
            }
        };

        futures::try_join!(test_controller, peer_outbound)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_stream_peer_to_peer_outbound_conn_succeeded() -> anyhow::Result<()> {
        let addr: SocketAddr = ([127, 0, 0, 1], 10001).into();
        let (handler, mut test) = config_handler(addr, None, false);
        let mut stream = HandshakeEventStream::new(handler);

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
                tokio::time::timeout(Duration::from_secs(15), next_stream_event(&mut stream))
                    .await??;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_stream_peer_to_gw_outbound_conn_rejected() -> anyhow::Result<()> {
        let joiner_addr = ([127, 0, 0, 1], 10001).into();
        let (handler, mut test) = config_handler(joiner_addr, None, false);
        let mut stream = HandshakeEventStream::new(handler);

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
            tracing::info!("Received connect request: {:?}", msg);
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
                    tokio::time::timeout(Duration::from_secs(60), next_stream_event(&mut stream))
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
}
