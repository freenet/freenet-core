//! Handles initial connection handshake.
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use either::Either;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::mpsc::{self};

use crate::{
    dev_tool::{PeerId, Transaction},
    message::{InnerMessage, NetMessage, NetMessageV1},
    transport::{
        InboundConnectionHandler, OutboundConnectionHandler, PeerConnection, TransportError,
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
        error: TransportError,
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
    unconfirmed_inbound_connections:
        FuturesUnordered<BoxFuture<'static, Result<Either<PeerConnection, ()>, TransportError>>>,
    outbound_messages: HashMap<SocketAddr, mpsc::Sender<NetMessage>>,
    /// The other end of `OutboundMessage`, receives messages to be sent to a yet not confirmed peer.
    pending_msg_rx: mpsc::Receiver<(SocketAddr, NetMessage)>,
    /// The other end of `EstablishConnection`, receives commands to establisha new outbound connection.
    establish_connection_rx: mpsc::Receiver<(PeerId, Transaction)>,
}

impl HandshakeHandler {
    pub fn new(
        inbound_conn_handler: InboundConnectionHandler,
        outbound_conn_handler: OutboundConnectionHandler,
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
            establish_connection_rx,
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
                    match unconfirmed_inbound_conn {
                        Some(Ok(Either::Left(connection))) => {
                            return Ok(Event::InboundConnection(connection));
                        }
                        Some(Ok(Either::Right(()))) => {
                            continue;
                        }
                        Some(Err(error)) => {
                            return Err(error);
                        }
                        None => return Err(TransportError::ChannelClosed),
                    }
                }
                pending_msg = self.pending_msg_rx.recv() => {
                    let Some((addr, msg)) = pending_msg else {
                        return Err(TransportError::ChannelClosed);
                    };
                    if let Some(event) = self.outbound(addr, msg) {
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
    fn outbound(&mut self, addr: SocketAddr, op: NetMessage) -> Option<Event> {
        match op {
            NetMessage::V1(NetMessageV1::Connect(op)) => {
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

                // TODO: check what the exact message is to track state of the connection and what we should do with it

                None
            }
            _ => None,
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

#[repr(transparent)]
struct PeerOutboundMessage(mpsc::Receiver<NetMessage>);

/// Handles the communication with a potentially transient peer connection.
async fn gw_peer_connection_listener(
    mut conn: PeerConnection,
    mut outbound: PeerOutboundMessage,
) -> Result<Either<PeerConnection, ()>, TransportError> {
    loop {
        tracing::debug!(at=?conn.my_address(), from=%conn.remote_addr(), "Waiting for message from peer");
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
        transport::{ConnectionEvent, OutboundConnectionHandler, PacketData},
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
                msg: crate::operations::connect::ConnectRequest::StartJoinReq {
                    joiner: None,
                    joiner_key: pub_key,
                    hops_to_live: 10,
                    max_hops_to_live: 10,
                    skip_list: vec![],
                },
            };

            let msg = NetMessage::V1(NetMessageV1::Connect(initial_join_req));
            let msg = bincode::serialize(&msg).unwrap();
            let encrypted = PacketData::from_buf_plain(msg).encrypt_symmetric(&in_symm_key);
            tracing::debug!("Sending initial connection message");
            packet_sender
                .send(encrypted.as_unknown().into())
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
        let (handler, establish_conn, out) =
            HandshakeHandler::new(inbound_conn_handler, outbound_conn_handler);
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
