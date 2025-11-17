//! Minimal handshake driver for the streamlined connect pipeline.
//!
//! The legacy handshake logic orchestrated the multi-stage `Connect` operation. With the
//! simplified state machine we only need a lightweight adapter that wires transport
//! connection attempts to/from the event loop. Higher-level routing decisions now live inside
//! `ConnectOp`.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::dev_tool::{Location, PeerId, Transaction};
use crate::node::network_bridge::ConnectionError;
use crate::ring::ConnectionManager;
use crate::router::Router;
use std::collections::HashMap;

use crate::transport::{InboundConnectionHandler, OutboundConnectionHandler, PeerConnection};

/// Events emitted by the handshake driver.
#[derive(Debug)]
pub(crate) enum Event {
    /// A remote peer initiated or completed a connection to us.
    InboundConnection {
        transaction: Option<Transaction>,
        peer: Option<PeerId>,
        connection: PeerConnection,
        courtesy: bool,
    },
    /// An outbound connection attempt succeeded.
    OutboundEstablished {
        transaction: Transaction,
        peer: PeerId,
        connection: PeerConnection,
        courtesy: bool,
    },
    /// An outbound connection attempt failed.
    OutboundFailed {
        transaction: Transaction,
        peer: PeerId,
        error: ConnectionError,
        courtesy: bool,
    },
}

/// Commands delivered from the event loop into the handshake driver.
#[derive(Debug)]
pub(crate) enum Command {
    /// Initiate a transport connection to `peer`.
    Connect {
        peer: PeerId,
        transaction: Transaction,
        courtesy: bool,
    },
    /// Register expectation for an inbound connection from `peer`.
    ExpectInbound {
        peer: PeerId,
        transaction: Option<Transaction>,
        courtesy: bool,
    },
    /// Remove state associated with `peer`.
    DropConnection { peer: PeerId },
}

#[derive(Clone)]
pub(crate) struct CommandSender(mpsc::Sender<Command>);

impl CommandSender {
    pub async fn send(&self, cmd: Command) -> Result<(), mpsc::error::SendError<Command>> {
        tracing::info!(?cmd, "handshake: sending command");
        self.0.send(cmd).await
    }
}

/// Stream wrapper around the asynchronous handshake driver.
pub(crate) struct HandshakeHandler {
    events_rx: mpsc::Receiver<Event>,
}

impl HandshakeHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inbound: InboundConnectionHandler,
        outbound: OutboundConnectionHandler,
        _connection_manager: ConnectionManager,
        _router: Arc<RwLock<Router>>,
        _this_location: Option<Location>,
        _is_gateway: bool,
        peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> (Self, CommandSender) {
        let (cmd_tx, cmd_rx) = mpsc::channel(128);
        let (event_tx, event_rx) = mpsc::channel(128);

        tokio::spawn(async move {
            run_driver(inbound, outbound, cmd_rx, event_tx, peer_ready).await;
        });

        (
            HandshakeHandler {
                events_rx: event_rx,
            },
            CommandSender(cmd_tx),
        )
    }
}

impl Stream for HandshakeHandler {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.events_rx).poll_recv(cx)
    }
}

#[derive(Debug)]
struct ExpectedInbound {
    peer: PeerId,
    transaction: Option<Transaction>,
    courtesy: bool,
}

#[derive(Default)]
struct ExpectedInboundTracker {
    entries: HashMap<SocketAddr, ExpectedInbound>,
}

impl ExpectedInboundTracker {
    fn register(&mut self, peer: PeerId, transaction: Option<Transaction>, courtesy: bool) {
        self.entries.insert(
            peer.addr,
            ExpectedInbound {
                peer,
                transaction,
                courtesy,
            },
        );
    }

    fn drop_peer(&mut self, peer: &PeerId) {
        self.entries.remove(&peer.addr);
    }

    fn consume(&mut self, addr: SocketAddr) -> Option<ExpectedInbound> {
        self.entries.remove(&addr)
    }

    #[cfg(test)]
    fn contains(&self, addr: SocketAddr) -> bool {
        self.entries.contains_key(&addr)
    }
}

async fn run_driver(
    mut inbound: InboundConnectionHandler,
    outbound: OutboundConnectionHandler,
    mut commands_rx: mpsc::Receiver<Command>,
    events_tx: mpsc::Sender<Event>,
    peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
) {
    use tokio::select;

    let mut expected_inbound = ExpectedInboundTracker::default();

    loop {
        select! {
            command = commands_rx.recv() => match command {
                Some(Command::Connect { peer, transaction, courtesy }) => {
                    spawn_outbound(outbound.clone(), events_tx.clone(), peer, transaction, courtesy, peer_ready.clone());
                }
                Some(Command::ExpectInbound { peer, transaction, courtesy }) => {
                    expected_inbound.register(peer, transaction, courtesy);
                }
                Some(Command::DropConnection { peer }) => {
                    expected_inbound.drop_peer(&peer);
                }
                None => break,
            },
            inbound_conn = inbound.next_connection() => {
                match inbound_conn {
                    Some(conn) => {
                        if let Some(flag) = &peer_ready {
                            flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        }

                        let remote_addr = conn.remote_addr();
                        let entry = expected_inbound.consume(remote_addr);
                        let (peer, transaction, courtesy) = if let Some(entry) = entry {
                            (Some(entry.peer), entry.transaction, entry.courtesy)
                        } else {
                            (None, None, false)
                        };

                        if events_tx.send(Event::InboundConnection {
                            transaction,
                            peer,
                            connection: conn,
                            courtesy,
                        }).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

fn spawn_outbound(
    outbound: OutboundConnectionHandler,
    events_tx: mpsc::Sender<Event>,
    peer: PeerId,
    transaction: Transaction,
    courtesy: bool,
    peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
) {
    tokio::spawn(async move {
        let peer_for_connect = peer.clone();
        let mut handler = outbound;
        let connect_future = handler
            .connect(peer_for_connect.pub_key.clone(), peer_for_connect.addr)
            .await;
        let result: Result<PeerConnection, ConnectionError> =
            match tokio::time::timeout(Duration::from_secs(10), connect_future).await {
                Ok(res) => res.map_err(|err| err.into()),
                Err(_) => Err(ConnectionError::Timeout),
            };

        if let Some(flag) = &peer_ready {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        let event = match result {
            Ok(connection) => Event::OutboundEstablished {
                transaction,
                peer: peer.clone(),
                connection,
                courtesy,
            },
            Err(error) => Event::OutboundFailed {
                transaction,
                peer: peer.clone(),
                error,
                courtesy,
            },
        };

        let _ = events_tx.send(event).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::connect::ConnectMsg;
    use crate::transport::TransportKeypair;
    use std::net::{IpAddr, Ipv4Addr};

    fn make_peer(port: u16) -> PeerId {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let keypair = TransportKeypair::new();
        PeerId::new(addr, keypair.public().clone())
    }

    #[test]
    fn tracker_returns_registered_entry_exactly_once() {
        let mut tracker = ExpectedInboundTracker::default();
        let peer = make_peer(4100);
        let tx = Transaction::new::<ConnectMsg>();
        tracker.register(peer.clone(), Some(tx), true);

        let entry = tracker
            .consume(peer.addr)
            .expect("expected registered inbound entry");
        assert_eq!(entry.peer, peer);
        assert_eq!(entry.transaction, Some(tx));
        assert!(entry.courtesy);
        assert!(tracker.consume(peer.addr).is_none());
    }

    #[test]
    fn tracker_drop_removes_entry() {
        let mut tracker = ExpectedInboundTracker::default();
        let peer = make_peer(4200);
        tracker.register(peer.clone(), None, false);
        assert!(tracker.contains(peer.addr));

        tracker.drop_peer(&peer);
        assert!(!tracker.contains(peer.addr));
        assert!(tracker.consume(peer.addr).is_none());
    }

    #[test]
    fn tracker_overwrites_existing_expectation() {
        let mut tracker = ExpectedInboundTracker::default();
        let peer = make_peer(4300);
        tracker.register(peer.clone(), None, false);
        let new_tx = Transaction::new::<ConnectMsg>();
        tracker.register(peer.clone(), Some(new_tx), true);

        let entry = tracker
            .consume(peer.addr)
            .expect("entry should be present after overwrite");
        assert_eq!(entry.transaction, Some(new_tx));
        assert!(entry.courtesy);
    }
}
