use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    dev_tool::{PeerId, Transaction},
    message::{InnerMessage, NetMessage, NetMessageV1},
    transport::{
        InboundConnectionHandler, OutboundConnectionHandler, PeerConnection, TransportError,
    },
};

pub(super) enum Action {
    None,
    RemoveTransaction(Transaction),
}

type OutboundConnResult = (PeerId, Result<PeerConnection, TransportError>);

pub(super) enum Event {
    InboundConnection(PeerConnection),
    OutboundConnectionSuccessful {
        peer_id: PeerId,
        connection: PeerConnection,
    },
    OutboundConnectionFailed {
        peer_id: PeerId,
        error: TransportError,
    },
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
pub(super) struct Connector {
    is_gateway: bool,
    connecting: HashMap<SocketAddr, Transaction>,
    connected: HashSet<SocketAddr>,
    inbound_conn_handler: InboundConnectionHandler,
    outbound_conn_handler: OutboundConnectionHandler,
    ongoing_connections: FuturesUnordered<BoxFuture<'static, OutboundConnResult>>,
    pending_msg_rx: Receiver<(SocketAddr, NetMessage)>,
    establish_connection_rx: Receiver<(PeerId, Transaction)>,
}

impl Connector {
    pub fn new(
        inbound_conn_handler: InboundConnectionHandler,
        outbound_conn_handler: OutboundConnectionHandler,
    ) -> (Self, EstablishConnection, OutboundMessage) {
        let (pending_msg_tx, pending_msg_rx) = tokio::sync::mpsc::channel(100);
        let (establish_connection_tx, establish_connection_rx) = tokio::sync::mpsc::channel(100);
        let connector = Connector {
            is_gateway: false,
            connecting: HashMap::new(),
            connected: HashSet::new(),
            inbound_conn_handler,
            outbound_conn_handler,
            ongoing_connections: FuturesUnordered::new(),
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
                conn = self.inbound_conn_handler.next_connection() => {
                    let Some(conn) = conn else {
                        return Err(TransportError::ChannelClosed);
                    };
                    break Ok(Event::InboundConnection(conn));
                }
                res = self.ongoing_connections.next(), if !self.ongoing_connections.is_empty() => {
                    let r = match res {
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
                    self.outbound(addr, msg);
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

    /// Messages sent to a pending confirmation outbound connection.
    fn outbound(&mut self, addr: SocketAddr, op: NetMessage) -> Action {
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
                    return Action::RemoveTransaction(tx);
                }
                self.connecting.insert(addr, tx);
                Action::None
            }
            _ => Action::None,
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
        self.ongoing_connections.push(f);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_connector() {}
}
