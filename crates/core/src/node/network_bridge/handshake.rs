//! Handles initial connection handshake.
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use either::Either;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    dev_tool::{PeerId, Transaction},
    message::{InnerMessage, NetMessage, NetMessageV1},
    transport::{
        InboundConnectionHandler, OutboundConnectionHandler, PeerConnection, TransportError,
    },
};

type OutboundConnResult = (PeerId, Result<PeerConnection, TransportError>);

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
pub(super) struct OutboundMessage(Sender<(SocketAddr, NetMessage)>);

/// Use for starting a new outboound connection to a peer.
pub(super) struct EstablishConnection(Sender<(PeerId, Transaction)>);

impl OutboundMessage {
    pub async fn send(&self, addr: SocketAddr, msg: NetMessage) {
        self.0.send((addr, msg)).await.unwrap();
    }
}

/// Handles initial connection handshake.
pub(super) struct HandshakeHandler {
    is_gateway: bool,
    connecting: HashMap<SocketAddr, Transaction>,
    connected: HashSet<SocketAddr>,
    inbound_conn_handler: InboundConnectionHandler,
    outbound_conn_handler: OutboundConnectionHandler,
    ongoing_outbound_connections: FuturesUnordered<BoxFuture<'static, OutboundConnResult>>,
    unconfirmed_inbound_connections:
        FuturesUnordered<BoxFuture<'static, Either<PeerConnection, ()>>>,
    pending_msg_rx: Receiver<(SocketAddr, NetMessage)>,
    establish_connection_rx: Receiver<(PeerId, Transaction)>,
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
        let f = async move {
            // TODO: in case of a new inbound connection, we are a gateway
            // maybe handle the corresponding part of the initial forwarding etc. here to simplify the code
            // and whether we accept or not the connection, wait for the handshake to be completed before making
            // the connection available for other ops; just remember to reserve a spot in case we accept the new connection
            todo!()
        }
        .boxed();
        self.unconfirmed_inbound_connections.push(f);
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_connector() {}
}
