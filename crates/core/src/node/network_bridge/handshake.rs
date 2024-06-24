//! Handles initial connection handshake.
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::mpsc::{self};

use crate::{
    dev_tool::{Location, PeerId, Transaction},
    message::{InnerMessage, NetMessage, NetMessageV1},
    operations::connect::{self, ConnectMsg, ConnectRequest},
    ring::ConnectionManager,
    transport::{
        InboundConnectionHandler, OutboundConnectionHandler, PeerConnection, TransportError,
        TransportPublicKey,
    },
};

type Result<T, E = HandshakeHandlerError> = std::result::Result<T, E>;
type OutboundConnResult = (PeerId, Result<PeerConnection, TransportError>);

#[derive(Debug, thiserror::Error)]
pub(super) enum HandshakeHandlerError {
    #[error("channel closed")]
    ChannelClosed,
    #[error(transparent)]
    Serialization(#[from] Box<bincode::ErrorKind>),
}

pub(super) enum Event {
    /// An inbound connection to a peer was successfully established at a gateway.
    InboundConnection(PeerConnection),
    /// An outbound connection to a peer was successfully established.
    OutboundConnectionSuccessful {
        peer_id: PeerId,
        connection: PeerConnection,
    },
    /// An outbound connection to a peer failed to be established.
    OutboundConnectionFailed {
        peer_id: PeerId,
        error: TransportError,
    },
    /// An outbound connection to a gateway was rejected.
    OutboundConnectionRejected {
        peer_id: PeerId,
        error: HandshakeHandlerError,
    },
    /// Clean up a transaction that was completed.
    RemoveTransaction(Transaction),
}

/// Use for sending messages to a peer which has not yet been confirmed at a logical level
/// or is just a transient connection (e.g. in case of gateways just forwarding messages).
pub(super) struct OutboundMessage(mpsc::Sender<(SocketAddr, NetMessage)>);

impl OutboundMessage {
    pub async fn send_to(&self, remote: SocketAddr, msg: NetMessage) -> Result<()> {
        self.0
            .send((remote, msg))
            .await
            .map_err(|_| HandshakeHandlerError::ChannelClosed)?;
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
            .map_err(|_| HandshakeHandlerError::ChannelClosed)?;
        Ok(())
    }
}

/// Handles initial connection handshake.
pub(super) struct HandshakeHandler {
    is_gateway: bool,
    connecting: HashMap<SocketAddr, Transaction>,
    connected: HashSet<SocketAddr>,
    inbound_conn_handler: InboundConnectionHandler,
    outbound_conn_handler: OutboundConnectionHandler,
    /// On-going outbound connection attempts.
    ongoing_outbound_connections: FuturesUnordered<BoxFuture<'static, OutboundConnResult>>,
    /// This is for connections that are not yet confirmed at a logical level in gateways
    unconfirmed_inbound_connections: FuturesUnordered<
        BoxFuture<'static, Result<(InternalEvent, PeerOutboundMessage), TransportError>>,
    >,
    outbound_messages: HashMap<SocketAddr, mpsc::Sender<NetMessage>>,
    /// The other end of `OutboundMessage`, receives messages to be sent to a yet not confirmed peer.
    pending_msg_rx: mpsc::Receiver<(SocketAddr, NetMessage)>,
    queues: HashMap<SocketAddr, Vec<NetMessage>>,
    /// The other end of `EstablishConnection`, receives commands to establish a new outbound connection.
    establish_connection_rx: mpsc::Receiver<(PeerId, Transaction)>,
    connection_manager: ConnectionManager,
}

impl HandshakeHandler {
    pub fn new(
        inbound_conn_handler: InboundConnectionHandler,
        outbound_conn_handler: OutboundConnectionHandler,
        connection_manager: ConnectionManager,
    ) -> (Self, EstablishConnection, OutboundMessage) {
        let (pending_msg_tx, pending_msg_rx) = tokio::sync::mpsc::channel(100);
        let (establish_connection_tx, establish_connection_rx) = tokio::sync::mpsc::channel(100);
        let connector = HandshakeHandler {
            is_gateway: false,
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
        };
        (
            connector,
            EstablishConnection(establish_connection_tx),
            OutboundMessage(pending_msg_tx),
        )
    }

    /// Listens for either new inbound connections or messages from pending outbound/inbound connections.
    pub async fn wait_for_events(&mut self) -> Result<Event, TransportError> {
        loop {
            tokio::select! {
                new_conn = self.inbound_conn_handler.next_connection() => {
                    let Some(conn) = new_conn else {
                        return Err(TransportError::ChannelClosed);
                    };
                    self.track_inbound_connection(conn);
                }
                outbound_conn = self.ongoing_outbound_connections.next(), if !self.ongoing_outbound_connections.is_empty() => {
                    let r = match outbound_conn {
                        Some((peer_id, Ok(connection))) => {
                            Ok(Event::OutboundConnectionSuccessful { peer_id, connection })
                        }
                        Some((peer_id, Err(error))) => {
                            Ok(Event::OutboundConnectionFailed { peer_id, error })
                        }
                        None => Err(TransportError::ChannelClosed),
                    };
                    break r;
                }
                unconfirmed_inbound_conn = self.unconfirmed_inbound_connections.next(), if !self.unconfirmed_inbound_connections.is_empty() => {
                    let Some(res) = unconfirmed_inbound_conn else {
                        return Err(TransportError::ChannelClosed);
                    };
                    let (event, outbound_sender) = res?;
                    match event {
                        InternalEvent::NewConnection(conn) => break Ok(Event::InboundConnection(conn)),
                        InternalEvent::InboundJoinRequest {
                            conn, id, joiner, joiner_key, hops_to_live, max_hops_to_live, skip_list
                        } => {
                            let remote = conn.remote_addr();
                            let location = Location::from_address(&remote);
                            let peer_id = joiner.unwrap_or_else(|| PeerId::new(remote, joiner_key));
                            let should_accept = self.connection_manager.should_accept(location, Some(&peer_id));
                            if should_accept {
                                todo!();
                            } else {
                                self.unconfirmed_inbound_connections.push(gw_peer_connection_listener(conn, outbound_sender).boxed());
                            }
                        }
                        InternalEvent::DropInboundConnection(conn) => {
                            let addr = conn.remote_addr();
                            self.outbound_messages.remove(&addr);
                            self.queues.remove(&addr);
                            self.connecting.remove(&addr);
                            continue;
                        }
                    }
                }
                pending_msg = self.pending_msg_rx.recv() => {
                    let Some((addr, msg)) = pending_msg else {
                        return Err(TransportError::ChannelClosed);
                    };
                    if let Some(event) = self.outbound(addr, msg).await {
                        break Ok(event);
                    }
                }
                establish_connection = self.establish_connection_rx.recv() => {
                    let Some((peer_id, tx)) = establish_connection else {
                        return Err(TransportError::ChannelClosed);
                    };
                    self.start_outbound_connection(peer_id, tx).await;
                }

            }
        }
    }

    fn track_inbound_connection(&mut self, conn: PeerConnection) {
        let (outbound_msg_sender, outbound_msg_recv) = mpsc::channel(1);
        let remote = conn.remote_addr();
        let f = gw_peer_connection_listener(conn, PeerOutboundMessage(outbound_msg_recv)).boxed();
        self.unconfirmed_inbound_connections.push(f);
        self.outbound_messages.insert(remote, outbound_msg_sender);
    }

    /// Messages sent to a pending outbound connection.
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
            // if is a message to a peer which is not yet connected, just queue it
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
        let f = self
            .outbound_conn_handler
            .connect(remote.pub_key.clone(), remote.addr)
            .await
            .map(move |c| (remote, c))
            .boxed();
        // TODO: if it's a gateway, maybe it will be eventually rejected, and is just forwarding, so take that in mind
        self.ongoing_outbound_connections.push(f);
    }
}

enum InternalEvent {
    NewConnection(PeerConnection),
    InboundJoinRequest {
        conn: PeerConnection,
        id: Transaction,
        joiner: Option<PeerId>,
        joiner_key: TransportPublicKey,
        hops_to_live: usize,
        max_hops_to_live: usize,
        skip_list: Vec<PeerId>,
    },
    DropInboundConnection(PeerConnection),
}

#[repr(transparent)]
struct PeerOutboundMessage(mpsc::Receiver<NetMessage>);

/// Handles the communication with a potentially transient peer connection.
async fn gw_peer_connection_listener(
    mut conn: PeerConnection,
    mut outbound: PeerOutboundMessage,
) -> Result<(InternalEvent, PeerOutboundMessage), TransportError> {
    loop {
        tokio::select! {
            msg = outbound.0.recv() => {
                let Some(msg) = msg else { break Err(TransportError::ConnectionClosed(conn.remote_addr())); };
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr() ,"Sending message to peer. Msg: {msg}");
                        conn
                            .send(msg)
                            .await?;
            }
            msg = conn.recv() => {
                let Ok(msg) = msg.map_err(|error| {
                    tracing::error!(at=?conn.my_address(), from=%conn.remote_addr(), "Error while receiving message: {error}");
                }) else {
                     break Err(TransportError::ConnectionClosed(conn.remote_addr()));
                };
                let net_message = decode_msg(&msg).unwrap();
                tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), %net_message, "Received message from peer");
                match net_message {
                    NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
                        id,
                        msg: ConnectRequest::StartJoinReq { joiner, joiner_key, hops_to_live, max_hops_to_live, skip_list }
                    })) => {
                        break Ok((InternalEvent::InboundJoinRequest { conn, id, joiner, joiner_key, hops_to_live, max_hops_to_live, skip_list }, outbound));
                    }
                    _ => todo!(),
                }
            }
        }
    }
}

#[inline(always)]
fn decode_msg(data: &[u8]) -> Result<NetMessage> {
    bincode::deserialize(data).map_err(|err| HandshakeHandlerError::Serialization(err))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aes_gcm::{Aes128Gcm, KeyInit};
    use anyhow::bail;
    use tokio::sync::mpsc;
    use tracing::level_filters::LevelFilter;

    use super::*;
    use crate::{
        config,
        dev_tool::TransportKeypair,
        operations::connect::ConnectMsg,
        transport::{ConnectionEvent, OutboundConnectionHandler, SymmetricMessage},
    };

    struct TransportMock {
        inbound_sender: mpsc::Sender<PeerConnection>,
        outbound_recv: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
    }

    impl TransportMock {
        /// This would happen when a new unsolicited connection is established with a gateway or
        /// when after initialising a connection with a peer via `outbound_recv`, a connection
        /// is successfully established.
        async fn new_inbound_conn(&mut self, addr: SocketAddr) {
            let out_symm_key = Aes128Gcm::new_from_slice(&[0; 16]).unwrap();
            let in_symm_key = Aes128Gcm::new_from_slice(&[1; 16]).unwrap();
            let (conn, packet_sender) =
                PeerConnection::new_test(addr, out_symm_key, in_symm_key.clone());
            // First a new connection is established
            self.inbound_sender.send(conn).await.unwrap();
            tracing::debug!("New inbound connection established");

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

            let msg = NetMessage::V1(NetMessageV1::Connect(initial_join_req));
            let msg = bincode::serialize(&msg).unwrap();
            let sym_msg =
                SymmetricMessage::serialize_msg_to_packet_data(0, msg, &in_symm_key, vec![])
                    .unwrap();
            tracing::debug!("Sending initial connection message");
            packet_sender
                .send(sym_msg.as_unknown().into())
                .await
                .unwrap();
        }
    }

    struct NodeMock {
        establish_conn: EstablishConnection,
        outbound_msg: OutboundMessage,
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
        let (handler, establish_conn, out) = HandshakeHandler::new(
            inbound_conn_handler,
            outbound_conn_handler,
            ConnectionManager::default(),
        );
        (
            handler,
            TestVerifier {
                transport: TransportMock {
                    inbound_sender,
                    outbound_recv,
                },
                node: NodeMock {
                    establish_conn,
                    outbound_msg: out,
                },
            },
        )
    }

    #[tokio::test]
    async fn test_gateway_inbound_conn() -> anyhow::Result<()> {
        config::set_logger(Some(LevelFilter::DEBUG));
        let (mut handler, mut test) = config_handler();
        let test_controller = async {
            let addr = ([127, 0, 0, 1], 10000).into();
            test.transport.new_inbound_conn(addr).await;
            Ok::<_, anyhow::Error>(())
        };

        let gw_inbound = async {
            let event =
                tokio::time::timeout(Duration::from_secs(60), handler.wait_for_events()).await??;
            match event {
                Event::InboundConnection(conn) => {
                    let addr: SocketAddr = ([127u8, 0, 0, 1], 10000u16).into();
                    assert_eq!(conn.remote_addr(), addr);
                    Ok(())
                }
                _ => bail!("Unexpected event"),
            }
        };
        futures::try_join!(test_controller, gw_inbound)?;
        Ok(())
    }
}
