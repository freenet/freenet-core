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

use crate::config::GlobalExecutor;
use crate::dev_tool::{Location, Transaction};
use crate::node::network_bridge::ConnectionError;
use crate::ring::{ConnectionManager, PeerKeyLocation};
use crate::router::Router;
use std::collections::HashMap;

use crate::transport::{
    InboundConnectionHandler, OutboundConnectionHandler, PeerConnection, PeerConnectionApi,
};

/// Events emitted by the handshake driver.
///
/// The connection field uses `Box<dyn PeerConnectionApi>` to type-erase the socket type,
/// allowing the same event loop code to work with both production (UdpSocket) and
/// testing (InMemorySocket) transports.
pub(crate) enum Event {
    /// A remote peer initiated or completed a connection to us.
    InboundConnection {
        transaction: Option<Transaction>,
        peer: Option<PeerKeyLocation>,
        connection: Box<dyn PeerConnectionApi>,
        transient: bool,
    },
    /// An outbound connection attempt succeeded.
    OutboundEstablished {
        transaction: Transaction,
        peer: PeerKeyLocation,
        connection: Box<dyn PeerConnectionApi>,
        transient: bool,
    },
    /// An outbound connection attempt failed.
    OutboundFailed {
        transaction: Transaction,
        peer: PeerKeyLocation,
        error: ConnectionError,
        transient: bool,
    },
}

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::InboundConnection {
                transaction,
                peer,
                connection,
                transient,
            } => f
                .debug_struct("InboundConnection")
                .field("transaction", transaction)
                .field("peer", peer)
                .field("remote_addr", &connection.remote_addr())
                .field("transient", transient)
                .finish(),
            Event::OutboundEstablished {
                transaction,
                peer,
                connection,
                transient,
            } => f
                .debug_struct("OutboundEstablished")
                .field("transaction", transaction)
                .field("peer", peer)
                .field("remote_addr", &connection.remote_addr())
                .field("transient", transient)
                .finish(),
            Event::OutboundFailed {
                transaction,
                peer,
                error,
                transient,
            } => f
                .debug_struct("OutboundFailed")
                .field("transaction", transaction)
                .field("peer", peer)
                .field("error", error)
                .field("transient", transient)
                .finish(),
        }
    }
}

/// Commands delivered from the event loop into the handshake driver.
#[derive(Debug)]
pub(crate) enum Command {
    /// Initiate a transport connection to `peer`.
    Connect {
        peer: PeerKeyLocation,
        transaction: Transaction,
        transient: bool,
    },
    /// Register expectation for an inbound connection from `peer`.
    ExpectInbound {
        peer: PeerKeyLocation,
        transaction: Option<Transaction>,
        transient: bool,
    },
    /// Remove state associated with `peer`.
    DropConnection { peer: PeerKeyLocation },
}

#[derive(Clone)]
pub(crate) struct CommandSender(mpsc::Sender<Command>);

impl CommandSender {
    /// Non-blocking send that returns false if the channel is full or closed.
    ///
    /// This is the ONLY way to enqueue a handshake command. Every caller runs
    /// on the network event-loop task, which also drives the handshake driver
    /// that drains this channel; a blocking `.send().await` here would risk
    /// self-stalling the loop under back-pressure (#4145). The blocking `send`
    /// method was removed for that reason — callers must drop/log on a full
    /// channel (each handshake command has a recoverable fallback: redundant
    /// teardown, provisional inbound accept, or op-level retry).
    pub fn try_send(&self, cmd: Command) -> bool {
        tracing::debug!(?cmd, "handshake: try_send command");
        match self.0.try_send(cmd) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => false,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

/// Stream wrapper around the asynchronous handshake driver.
pub(crate) struct HandshakeHandler {
    events_rx: mpsc::Receiver<Event>,
}

impl HandshakeHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new<S: crate::transport::Socket>(
        inbound: InboundConnectionHandler<S>,
        outbound: OutboundConnectionHandler<S>,
        _connection_manager: ConnectionManager,
        _router: Arc<RwLock<Router>>,
        _this_location: Option<Location>,
        _is_gateway: bool,
        peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> (Self, CommandSender) {
        let (cmd_tx, cmd_rx) = mpsc::channel(128);
        let (event_tx, event_rx) = mpsc::channel(128);

        GlobalExecutor::spawn(async move {
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
    peer: PeerKeyLocation,
    transaction: Option<Transaction>,
    transient: bool, // TODO: rename to transient in protocol once we migrate terminology
}

#[derive(Default)]
struct ExpectedInboundTracker {
    entries: HashMap<SocketAddr, ExpectedInbound>,
}

impl ExpectedInboundTracker {
    fn register(
        &mut self,
        peer: PeerKeyLocation,
        transaction: Option<Transaction>,
        transient: bool,
    ) {
        let addr = peer
            .socket_addr()
            .expect("ExpectInbound requires known peer address");
        tracing::debug!(
            remote = %addr,
            transient,
            tx = ?transaction,
            "ExpectInbound: registering expectation"
        );
        // Replace any existing expectation for the same socket to ensure the newest registration wins.
        self.entries.insert(
            addr,
            ExpectedInbound {
                peer,
                transaction,
                transient,
            },
        );
    }

    fn drop_peer(&mut self, peer: &PeerKeyLocation) {
        if let Some(addr) = peer.socket_addr() {
            self.entries.remove(&addr);
        }
    }

    fn consume(&mut self, addr: SocketAddr) -> Option<ExpectedInbound> {
        let entry = self.entries.remove(&addr);
        if let Some(entry) = &entry {
            tracing::debug!(
                remote = %addr,
                peer = %entry.peer,
                transient = entry.transient,
                tx = ?entry.transaction,
                "ExpectInbound: matched by socket address"
            );
        }
        entry
    }

    #[cfg(test)]
    fn contains(&self, addr: SocketAddr) -> bool {
        self.entries.contains_key(&addr)
    }

    #[cfg(test)]
    fn transactions_for(&self, addr: SocketAddr) -> Option<Vec<Option<Transaction>>> {
        self.entries.get(&addr).map(|entry| vec![entry.transaction])
    }
}

async fn run_driver<S: crate::transport::Socket>(
    mut inbound: InboundConnectionHandler<S>,
    outbound: OutboundConnectionHandler<S>,
    mut commands_rx: mpsc::Receiver<Command>,
    events_tx: mpsc::Sender<Event>,
    peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
) {
    use tokio::select;

    let mut expected_inbound = ExpectedInboundTracker::default();

    loop {
        select! {
            command = commands_rx.recv() => match command {
                Some(Command::Connect { peer, transaction, transient }) => {
                    spawn_outbound(outbound.clone(), events_tx.clone(), peer, transaction, transient, peer_ready.clone());
                }
            Some(Command::ExpectInbound { peer, transaction, transient }) => {
                    expected_inbound.register(peer, transaction, transient /* transient */);
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
                        let (peer, transaction, transient) = if let Some(entry) = entry {
                            (Some(entry.peer), entry.transaction, entry.transient)
                        } else {
                            tracing::debug!(
                                remote = %remote_addr,
                                "Received unexpected inbound connection (no matching expectation)"
                            );
                            (None, None, false)
                        };

                        // Non-blocking (#4145): events_tx (cap 128) is drained by
                        // the network event-loop task. A blocking `.send().await`
                        // here would stall the handshake driver — and therefore stop
                        // it draining commands_rx — whenever the event loop is
                        // momentarily behind under fan-out load. On a FULL channel we
                        // drop THIS inbound connection and keep accepting: the peer's
                        // connect op retries, so a transiently-full channel must not
                        // tear down the acceptor. Only a CLOSED channel (event loop
                        // gone) is terminal — then we break to shut the driver down.
                        match events_tx.try_send(Event::InboundConnection {
                            transaction,
                            peer,
                            connection: Box::new(conn),
                            transient,
                        }) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    remote = %remote_addr,
                                    "Event channel full; dropping inbound connection \
                                     (peer's connect op will retry). Acceptor continues."
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                tracing::debug!(
                                    remote = %remote_addr,
                                    "Event channel closed; shutting down handshake driver"
                                );
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
        }
    }
}

fn spawn_outbound<S: crate::transport::Socket>(
    outbound: OutboundConnectionHandler<S>,
    events_tx: mpsc::Sender<Event>,
    peer: PeerKeyLocation,
    transaction: Transaction,
    transient: bool,
    peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
) {
    GlobalExecutor::spawn(async move {
        let peer_for_connect = peer.clone();
        let mut handler = outbound;
        let addr = peer_for_connect
            .socket_addr()
            .expect("Connect requires known peer address");
        let connect_future = handler
            .connect(peer_for_connect.pub_key.clone(), addr)
            .await;
        let result: Result<PeerConnection<S>, ConnectionError> =
            match tokio::time::timeout(Duration::from_secs(10), connect_future).await {
                Ok(res) => res.map_err(|err| err.into()),
                Err(_) => Err(ConnectionError::Timeout),
            };

        // Only mark peer as ready after a successful connection
        if result.is_ok() {
            if let Some(flag) = &peer_ready {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let event = match result {
            Ok(connection) => Event::OutboundEstablished {
                transaction,
                peer: peer.clone(),
                connection: Box::new(connection),
                transient,
            },
            Err(error) => Event::OutboundFailed {
                transaction,
                peer: peer.clone(),
                error,
                transient,
            },
        };

        // channel-safety: ok — this runs in a detached per-connection task
        // (GlobalExecutor::spawn), NOT on the event loop or the run_driver
        // acceptor, so blocking here cannot stall the loop or stop commands
        // draining. Matches the "spawned task where blocking is acceptable"
        // exception in .claude/rules/channel-safety.md (#4145).
        if let Err(e) = events_tx.send(event).await {
            tracing::warn!(error = %e, "failed to send handshake event");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::connect::ConnectMsg;
    use crate::transport::TransportKeypair;
    use std::net::{IpAddr, Ipv4Addr};

    fn make_peer(port: u16) -> PeerKeyLocation {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let keypair = TransportKeypair::new();
        PeerKeyLocation::new(keypair.public().clone(), addr)
    }

    #[test]
    fn tracker_returns_registered_entry_exactly_once() {
        let mut tracker = ExpectedInboundTracker::default();
        let peer = make_peer(4100);
        let addr = peer.socket_addr().unwrap();
        let tx = Transaction::new::<ConnectMsg>();
        tracker.register(peer.clone(), Some(tx), true);

        let entry = tracker
            .consume(addr)
            .expect("expected registered inbound entry");
        assert_eq!(entry.peer, peer);
        assert_eq!(entry.transaction, Some(tx));
        assert!(entry.transient);
        assert!(tracker.consume(addr).is_none());
    }

    #[test]
    fn tracker_drop_removes_entry() {
        let mut tracker = ExpectedInboundTracker::default();
        let peer = make_peer(4200);
        let addr = peer.socket_addr().unwrap();
        tracker.register(peer.clone(), None, false);
        assert!(tracker.contains(addr));

        tracker.drop_peer(&peer);
        assert!(!tracker.contains(addr));
        assert!(tracker.consume(addr).is_none());
    }

    #[test]
    fn tracker_overwrites_existing_expectation() {
        let mut tracker = ExpectedInboundTracker::default();
        let peer = make_peer(4300);
        let addr = peer.socket_addr().unwrap();
        tracker.register(peer.clone(), None, false);
        let new_tx = Transaction::new::<ConnectMsg>();
        tracker.register(peer.clone(), Some(new_tx), true);
        let transactions = tracker.transactions_for(addr).expect("entry should exist");
        assert_eq!(transactions, vec![Some(new_tx)]);

        let entry = tracker
            .consume(addr)
            .expect("entry should be present after overwrite");
        assert_eq!(entry.transaction, Some(new_tx));
        assert!(entry.transient);
    }

    #[test]
    fn tracker_keeps_peers_separate_on_same_ip() {
        let mut tracker = ExpectedInboundTracker::default();
        let peer_a = make_peer(4400);
        let peer_b = make_peer(4401);
        let addr_a = peer_a.socket_addr().unwrap();
        let addr_b = peer_b.socket_addr().unwrap();

        tracker.register(peer_a.clone(), None, false);
        tracker.register(peer_b.clone(), None, true);

        let first = tracker
            .consume(addr_a)
            .expect("first peer should be matched by exact socket");
        assert_eq!(first.peer, peer_a);
        assert!(!first.transient);

        let second = tracker
            .consume(addr_b)
            .expect("second peer should still be tracked independently");
        assert_eq!(second.peer, peer_b);
        assert!(second.transient);
    }

    #[test]
    fn command_sender_try_send_returns_false_when_full() {
        // Channel capacity 1 — fill it, then verify try_send returns false
        let (tx, _rx) = mpsc::channel(1);
        let sender = CommandSender(tx);
        let peer = make_peer(4500);

        // First send should succeed
        assert!(sender.try_send(Command::DropConnection { peer: peer.clone() }));
        // Channel is now full — second send should return false, not block
        assert!(!sender.try_send(Command::DropConnection { peer }));
    }

    #[test]
    fn command_sender_try_send_returns_false_when_closed() {
        let (tx, rx) = mpsc::channel(16);
        let sender = CommandSender(tx);
        let peer = make_peer(4600);
        drop(rx); // Close the channel

        assert!(!sender.try_send(Command::DropConnection { peer }));
    }

    /// SITE 8 source-scrape pin (#4145): the run_driver inbound-accept arm
    /// must forward via the non-blocking `events_tx.try_send(` and must NOT
    /// tear the acceptor down on a transiently-full channel. A blocking
    /// `events_tx.send(...).await` here would stall the handshake driver
    /// (and thus stop it draining commands_rx) whenever the event loop is
    /// momentarily behind under fan-out load. The per-connection spawned
    /// task in `spawn_outbound` keeps its blocking `events_tx.send(...).await`
    /// (it cannot stall the loop), so we assert specifically that the accept
    /// arm uses try_send and that Full continues while Closed breaks.
    #[test]
    fn run_driver_inbound_accept_is_non_blocking() {
        let full = include_str!("handshake.rs");
        // Scan production only (everything before the `mod tests {` block), so
        // this pin does not match the marker strings in its own assertions.
        let cut = full
            .find("\nmod tests {")
            .expect("handshake.rs must have a `mod tests {` block");
        let src = &full[..cut];

        // The accept arm builds an Event::InboundConnection and forwards it.
        let marker = "events_tx.try_send(Event::InboundConnection {";
        let from = src.find(marker).expect(
            "#4145 SITE 8: run_driver's inbound-accept arm must use \
             events_tx.try_send(Event::InboundConnection ...), not a blocking send.",
        );
        // Window large enough to cover the full match (the Closed arm is the
        // last of four arms, ~25 lines down).
        let end = (from + 1200).min(src.len());
        let arm = &src[from..end];
        assert!(
            arm.contains("TrySendError::Full") && arm.contains("TrySendError::Closed"),
            "#4145 SITE 8: the accept arm must explicitly distinguish Full \
             (drop + continue) from Closed (break). Slice:\n{arm}"
        );
        // And the blocking form must be gone from the accept arm.
        assert!(
            !arm.contains("events_tx.send(Event::InboundConnection"),
            "#4145 SITE 8: a blocking events_tx.send(Event::InboundConnection ...).await \
             remains in the accept arm — must be try_send."
        );
    }

    /// SITE 8 behavioral pin: a FULL events channel must drop the inbound
    /// connection and let the acceptor keep going (try_send returns Full,
    /// not a block); a CLOSED channel is the only case that shuts the driver
    /// down. This mirrors the match in run_driver without standing up a real
    /// transport.
    #[tokio::test]
    async fn full_events_channel_does_not_break_acceptor() {
        let (tx, mut rx) = mpsc::channel::<u32>(1);
        tx.try_send(1).expect("fill the single slot");

        // Full -> non-blocking error, acceptor would `continue`.
        let mut should_break = false;
        match tx.try_send(2) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => { /* drop + continue */ }
            Err(mpsc::error::TrySendError::Closed(_)) => should_break = true,
        }
        assert!(
            !should_break,
            "a transiently-full events channel must NOT break the acceptor"
        );

        // Drain + close -> Closed -> break.
        let _ = rx.recv().await;
        drop(rx);
        match tx.try_send(3) {
            Err(mpsc::error::TrySendError::Closed(_)) => should_break = true,
            other => panic!("expected Closed after receiver dropped, got {other:?}"),
        }
        assert!(
            should_break,
            "a closed events channel (event loop gone) must break the acceptor"
        );
    }
}
