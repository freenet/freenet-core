//! Minimal handshake driver for the streamlined connect pipeline.
//!
//! The legacy handshake logic orchestrated the multi-stage `Connect` operation. With the
//! simplified state machine we only need a lightweight adapter that wires transport
//! connection attempts to/from the event loop. Higher-level routing decisions now live inside
//! `ConnectOp`.

use std::collections::HashMap;
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

async fn run_driver(
    mut inbound: InboundConnectionHandler,
    outbound: OutboundConnectionHandler,
    mut commands_rx: mpsc::Receiver<Command>,
    events_tx: mpsc::Sender<Event>,
    peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
) {
    use tokio::select;

    let mut expected_inbound: HashMap<SocketAddr, ExpectedInbound> = HashMap::new();

    loop {
        select! {
            command = commands_rx.recv() => match command {
                Some(Command::Connect { peer, transaction, courtesy }) => {
                    spawn_outbound(outbound.clone(), events_tx.clone(), peer, transaction, courtesy, peer_ready.clone());
                }
                Some(Command::ExpectInbound { peer, transaction, courtesy }) => {
                    expected_inbound.insert(peer.addr, ExpectedInbound { peer, transaction, courtesy });
                }
                Some(Command::DropConnection { peer }) => {
                    expected_inbound.remove(&peer.addr);
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
                        let entry = expected_inbound.remove(&remote_addr);
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
