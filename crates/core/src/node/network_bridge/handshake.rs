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

use crate::transport::{InboundAcceptor, OutboundConnector, PeerConnectionApi, TransportPublicKey};
use crate::util::time_source::{DynTimeSource, TimeSource};

/// Events emitted by the handshake driver.
///
/// The connection field uses `Box<dyn PeerConnectionApi>` to type-erase the socket type,
/// allowing the same event loop code to work with both production (UdpTransportSocket) and
/// testing (InMemorySocket) transports.
pub(crate) enum Event {
    /// A remote peer initiated or completed a connection to us. `matched`
    /// says whether it matched an expected-inbound registration and, if so,
    /// the key the connect op dialled.
    InboundConnection {
        transaction: Option<Transaction>,
        matched: InboundMatch,
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
                matched,
                connection,
                transient,
            } => f
                .debug_struct("InboundConnection")
                .field("transaction", transaction)
                .field("matched", matched)
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

/// Whether an inbound connection matched an expected-inbound registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InboundMatch {
    /// No registration for its address.
    Unexpected,
    /// A registration for its address, with the key the connect op is
    /// dialling at that address, when the registration carried one.
    Expected(Option<TransportPublicKey>),
}

/// Commands delivered from the event loop into the handshake driver.
#[derive(Debug)]
pub(crate) enum Command {
    /// Initiate a transport connection to `peer`.
    Connect {
        peer: PeerKeyLocation,
        transaction: Transaction,
        transient: bool,
        /// Why this dial is made (telemetry only).
        origin: crate::transport::NatAttemptOrigin,
    },
    /// Register expectation for an inbound connection from `addr`, which the
    /// connect op is dialling under `peer_key`. A matched connection is
    /// recorded under that key, as a successful dial to it would be.
    ExpectInbound {
        addr: SocketAddr,
        peer_key: Option<TransportPublicKey>,
        transaction: Option<Transaction>,
        transient: bool,
    },
    /// Remove state associated with the connection at `addr`.
    DropConnection { addr: SocketAddr },
    /// Test only: answered once every command sent before it is processed.
    #[cfg(test)]
    Barrier(tokio::sync::oneshot::Sender<()>),
}

#[derive(Clone)]
pub(crate) struct CommandSender(mpsc::Sender<Command>);

impl CommandSender {
    /// A sender onto `tx`, for tests that drive the event-loop handlers
    /// without a handshake driver.
    #[cfg(test)]
    pub(crate) fn for_test(tx: mpsc::Sender<Command>) -> Self {
        Self(tx)
    }

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
    pub fn new(
        inbound: Box<dyn InboundAcceptor>,
        outbound: Arc<dyn OutboundConnector>,
        _connection_manager: ConnectionManager,
        _router: Arc<RwLock<Router>>,
        _this_location: Option<Location>,
        _is_gateway: bool,
        peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
        clock: DynTimeSource,
    ) -> (Self, CommandSender) {
        let (cmd_tx, cmd_rx) = mpsc::channel(128);
        let (event_tx, event_rx) = mpsc::channel(128);

        GlobalExecutor::spawn(async move {
            run_driver(inbound, outbound, cmd_rx, event_tx, peer_ready, clock).await;
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

/// How long after its registration an expected-inbound registration's key
/// is honoured. An acceptor registers when it accepts a joiner; the joiner
/// may still be collecting acceptors for up to `OPERATION_TTL` before it
/// dials them, keeps finishing those dials for up to `DIAL_DRAIN_LIMIT` after
/// it stops collecting, and each dial's handshake may take up to
/// `OUTBOUND_CONNECT_TIMEOUT`. A connection from that address after this
/// window is not bound to the registration's key (the address may since
/// belong to someone else).
///
/// No fixed window covers every honest case: the first stage of a dial
/// (handing it to the transport) is not bounded (#5947). An inbound
/// connection that arrives after the window starts without a key, as one
/// did before matched connections were bound: a later ConnectRequest or
/// ConnectPeer names it, or the unpromoted-connection sweep removes it.
pub(crate) const EXPECTED_INBOUND_WINDOW: Duration = Duration::from_secs(100);

// The window covers the joiner's collection, dial drain and one handshake.
// Compared in milliseconds so a sub-second part is not truncated away.
const _: () = assert!(
    EXPECTED_INBOUND_WINDOW.as_millis()
        >= crate::config::OPERATION_TTL.as_millis()
            + crate::operations::connect::op_ctx_task::DIAL_DRAIN_LIMIT.as_millis()
            + OUTBOUND_CONNECT_TIMEOUT.as_millis()
);

/// How often the handshake driver sweeps expired expectations when no
/// registration or match does it.
const EXPECTED_INBOUND_SWEEP_INTERVAL: Duration = Duration::from_secs(50);
const _: () =
    assert!(EXPECTED_INBOUND_SWEEP_INTERVAL.as_millis() * 2 <= EXPECTED_INBOUND_WINDOW.as_millis());

#[derive(Debug)]
struct ExpectedInbound {
    peer_key: Option<TransportPublicKey>,
    /// When it was registered, for [`EXPECTED_INBOUND_WINDOW`].
    registered_at: tokio::time::Instant,
    transaction: Option<Transaction>,
    transient: bool, // TODO: rename to transient in protocol once we migrate terminology
}

/// Matches accepted connections to the connect operations that expect them,
/// by socket address. Node-side bookkeeping, distinct from the transport's
/// expected-IP set (`InboundExpectations`).
///
/// Bounded by time: a registration older than [`EXPECTED_INBOUND_WINDOW`]
/// is swept on the next registration or match, and at the latest by the
/// driver's periodic sweep ([`EXPECTED_INBOUND_SWEEP_INTERVAL`]), so no
/// entry outlives the window by more than one interval. A match against one
/// that has expired but not yet been swept is reported without its key.
struct InboundMatcher {
    entries: HashMap<SocketAddr, ExpectedInbound>,
    clock: DynTimeSource,
}

impl InboundMatcher {
    fn new(clock: DynTimeSource) -> Self {
        Self {
            entries: HashMap::new(),
            clock,
        }
    }

    fn expired(&self, entry: &ExpectedInbound, now: tokio::time::Instant) -> bool {
        now.saturating_duration_since(entry.registered_at) > EXPECTED_INBOUND_WINDOW
    }

    /// Remove every registration older than the window.
    fn sweep(&mut self) {
        let now = self.clock.now();
        let before = self.entries.len();
        self.entries.retain(|_, entry| {
            now.saturating_duration_since(entry.registered_at) <= EXPECTED_INBOUND_WINDOW
        });
        let swept = before - self.entries.len();
        if swept > 0 {
            tracing::debug!(swept, "ExpectInbound: swept expired expectations");
        }
    }

    fn register(
        &mut self,
        addr: SocketAddr,
        peer_key: Option<TransportPublicKey>,
        transaction: Option<Transaction>,
        transient: bool,
    ) {
        self.sweep();
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
                peer_key,
                registered_at: self.clock.now(),
                transaction,
                transient,
            },
        );
    }

    fn drop_addr(&mut self, addr: SocketAddr) {
        self.entries.remove(&addr);
    }

    /// Take the registration for `addr`. One older than the window is
    /// returned without its key: the connection is not the one it asked for.
    fn consume(&mut self, addr: SocketAddr) -> Option<ExpectedInbound> {
        let mut entry = self.entries.remove(&addr);
        let now = self.clock.now();
        if let Some(found) = entry.as_mut() {
            if found.peer_key.is_some() && self.expired(found, now) {
                tracing::debug!(
                    remote = %addr,
                    "ExpectInbound: expectation outlived its window; not binding its key"
                );
                found.peer_key = None;
            }
        }
        self.sweep();
        if let Some(entry) = &entry {
            tracing::debug!(
                remote = %addr,
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

async fn run_driver(
    mut inbound: Box<dyn InboundAcceptor>,
    outbound: Arc<dyn OutboundConnector>,
    mut commands_rx: mpsc::Receiver<Command>,
    events_tx: mpsc::Sender<Event>,
    peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
    clock: DynTimeSource,
) {
    use tokio::select;

    let mut expected_inbound = InboundMatcher::new(clock);
    // Guarantees expiry when no registration or match sweeps. Paced by
    // tokio time; what is expired is decided by the injected clock. The
    // first tick is one interval out: `interval` would fire at once, and a
    // sweep then has nothing to remove.
    let mut sweep_timer = tokio::time::interval_at(
        tokio::time::Instant::now() + EXPECTED_INBOUND_SWEEP_INTERVAL,
        EXPECTED_INBOUND_SWEEP_INTERVAL,
    );
    sweep_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        select! {
            _ = sweep_timer.tick() => expected_inbound.sweep(),
            command = commands_rx.recv() => match command {
                Some(Command::Connect { peer, transaction, transient, origin }) => {
                    spawn_outbound(outbound.clone(), events_tx.clone(), peer, transaction, transient, origin, peer_ready.clone());
                }
                Some(Command::ExpectInbound { addr, peer_key, transaction, transient }) => {
                    expected_inbound.register(addr, peer_key, transaction, transient);
                }
                Some(Command::DropConnection { addr }) => {
                    expected_inbound.drop_addr(addr);
                }
                #[cfg(test)]
                Some(Command::Barrier(done)) => {
                    // The test may have stopped waiting.
                    #[allow(clippy::let_underscore_must_use)]
                    let _ = done.send(());
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
                        let (matched, transaction, transient) = if let Some(entry) = entry {
                            (InboundMatch::Expected(entry.peer_key), entry.transaction, entry.transient)
                        } else {
                            tracing::debug!(
                                remote = %remote_addr,
                                "Received unexpected inbound connection (no matching expectation)"
                            );
                            (InboundMatch::Unexpected, None, false)
                        };

                        // Bounded send (#4145), NOT an unconditional drop.
                        //
                        // events_tx (cap 128) is drained by the network event-loop
                        // task. The original blocking `.send().await` here could
                        // stall this handshake driver indefinitely — and therefore
                        // stop it draining commands_rx — whenever the event loop
                        // fell behind under fan-out load.
                        //
                        // Why send_timeout rather than try_send-drop: dropping an
                        // InboundConnection is not always free. An inbound
                        // connection that satisfies a locally-initiated connect
                        // clears the event loop's `awaiting_connection` entry and
                        // its callbacks; while a dropped one is ultimately recovered
                        // (the dialing side's `spawn_outbound` resolves to
                        // OutboundEstablished/OutboundFailed within 10s, and
                        // OutboundFailed clears `awaiting_connection`), and a
                        // duplicate inbound is harmless, a bounded wait lets us
                        // PRESERVE the connection across a transient backlog instead
                        // of forcing a slower reconnect. Crucially, run_driver is
                        // NOT the event-loop task, so a bounded block here cannot
                        // reintroduce the #4145 event-loop self-stall — it only
                        // briefly pauses this accept loop.
                        //
                        // The block is on the `inbound_conn` select arm, so while it
                        // waits (up to 5s) the sibling `commands_rx.recv()` arm is
                        // also paused — the driver stops draining handshake commands
                        // for that window. That is safe: every command sender is the
                        // event loop's non-blocking `try_send` (the converted sites
                        // 2-7), which drops + logs on a full channel rather than
                        // blocking, so a paused `commands_rx` cannot wedge the event
                        // loop back. The only cost is a brief delay in applying
                        // Connect/ExpectInbound/DropConnection under heavy backlog,
                        // which is recoverable (the senders' drop paths already
                        // tolerate loss).
                        //
                        // Elapsed (event loop wedged longer than the bound): warn +
                        // drop as a last resort, keep accepting. Closed (event loop
                        // gone): break to shut the driver down.
                        const INBOUND_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
                        match events_tx
                            .send_timeout(
                                Event::InboundConnection {
                                    transaction,
                                    matched,
                                    connection: conn,
                                    transient,
                                },
                                INBOUND_FORWARD_TIMEOUT,
                            )
                            .await
                        {
                            Ok(()) => {}
                            Err(mpsc::error::SendTimeoutError::Timeout(_)) => {
                                tracing::warn!(
                                    remote = %remote_addr,
                                    timeout_secs = INBOUND_FORWARD_TIMEOUT.as_secs(),
                                    "Event channel full for the full forward timeout; \
                                     dropping inbound connection as a last resort \
                                     (dialer's connect op recovers via Outbound* / TTL). \
                                     Acceptor continues."
                                );
                            }
                            Err(mpsc::error::SendTimeoutError::Closed(_)) => {
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

fn spawn_outbound(
    outbound: Arc<dyn OutboundConnector>,
    events_tx: mpsc::Sender<Event>,
    peer: PeerKeyLocation,
    transaction: Transaction,
    transient: bool,
    origin: crate::transport::NatAttemptOrigin,
    peer_ready: Option<Arc<std::sync::atomic::AtomicBool>>,
) {
    GlobalExecutor::spawn(async move {
        let event = connect_outbound(
            &*outbound,
            peer,
            transaction,
            transient,
            origin,
            peer_ready.as_deref(),
        )
        .await;

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

/// How long the handshake stage of an outbound connect may take. It does not
/// cover handing the attempt to the transport, as before (see
/// [`OutboundConnector`]); that unbounded stage is #5947.
const OUTBOUND_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Run one outbound connect to `peer` and report it as the event the driver
/// emits. Sets `peer_ready` only on success.
async fn connect_outbound(
    outbound: &dyn OutboundConnector,
    peer: PeerKeyLocation,
    transaction: Transaction,
    transient: bool,
    origin: crate::transport::NatAttemptOrigin,
    peer_ready: Option<&std::sync::atomic::AtomicBool>,
) -> Event {
    let addr = peer
        .socket_addr()
        .expect("Connect requires known peer address");
    let connect_future = outbound.connect(peer.pub_key.clone(), addr, origin).await;
    let result: Result<Box<dyn PeerConnectionApi>, ConnectionError> =
        match tokio::time::timeout(OUTBOUND_CONNECT_TIMEOUT, connect_future).await {
            Ok(res) => res.map_err(|err| err.into()),
            Err(_) => Err(ConnectionError::Timeout),
        };

    // Only mark peer as ready after a successful connection
    if result.is_ok() {
        if let Some(flag) = peer_ready {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    match result {
        Ok(connection) => Event::OutboundEstablished {
            transaction,
            peer,
            connection,
            transient,
        },
        Err(error) => Event::OutboundFailed {
            transaction,
            peer,
            error,
            transient,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::connect::ConnectMsg;
    use crate::transport::{TransportError, TransportKeypair};
    use std::net::{IpAddr, Ipv4Addr};

    fn matcher() -> InboundMatcher {
        InboundMatcher::new(Arc::new(crate::util::time_source::InstantTimeSrc::new()))
    }

    fn make_peer(port: u16) -> PeerKeyLocation {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let keypair = TransportKeypair::new();
        PeerKeyLocation::new(keypair.public().clone(), addr)
    }

    /// A connection that does nothing; only its identity as a
    /// `Box<dyn PeerConnectionApi>` matters to these tests.
    struct NullConnection(SocketAddr);

    impl PeerConnectionApi for NullConnection {
        fn remote_addr(&self) -> SocketAddr {
            self.0
        }

        fn remote_version(&self) -> Option<(u8, u8, u16)> {
            None
        }

        fn send_bytes(
            &mut self,
            _data: Vec<u8>,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send + '_>>
        {
            Box::pin(async { Ok(()) })
        }

        fn recv(
            &mut self,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, TransportError>> + Send + '_>>
        {
            Box::pin(std::future::pending())
        }

        fn set_inbound_stream_registry(
            &mut self,
            _registry: Arc<dyn crate::transport::InboundStreamRegistry>,
        ) {
        }

        fn send_stream_data(
            &mut self,
            _stream_id: crate::transport::StreamId,
            _data: bytes::Bytes,
            _metadata: Option<bytes::Bytes>,
            _completion_tx: Option<
                tokio::sync::oneshot::Sender<crate::transport::StreamSendOutcome>,
            >,
            _progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send + '_>>
        {
            unreachable!("not used by these tests")
        }

        fn pipe_stream_data(
            &mut self,
            _outbound_stream_id: crate::transport::StreamId,
            _inbound_handle: crate::transport::IncomingStream,
            _metadata: Option<bytes::Bytes>,
            _progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send + '_>>
        {
            unreachable!("not used by these tests")
        }
    }

    /// An `InboundAcceptor` that yields whatever the test sends it, and
    /// `None` once the test drops the sender.
    struct ChannelAcceptor(mpsc::Receiver<Box<dyn PeerConnectionApi>>);

    impl InboundAcceptor for ChannelAcceptor {
        fn next_connection(&mut self) -> crate::transport::NextConnection<'_> {
            Box::pin(self.0.recv())
        }
    }

    /// A `Connect` command reaches the connector with its origin.
    #[tokio::test(start_paused = true)]
    async fn run_driver_passes_the_connect_commands_origin_to_the_connector() {
        let (_conn_tx, conn_rx) = mpsc::channel(4);
        let (cmd_tx, cmd_rx) = mpsc::channel(4);
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let connector = Arc::new(ScriptedConnector {
            enqueue: Duration::ZERO,
            handshake: Handshake::Succeeds,
            origins: Default::default(),
        });
        let driver = GlobalExecutor::spawn(run_driver(
            Box::new(ChannelAcceptor(conn_rx)),
            connector.clone(),
            cmd_rx,
            event_tx,
            None,
            Arc::new(crate::util::time_source::InstantTimeSrc::new()),
        ));
        cmd_tx
            .send(Command::Connect {
                peer: make_peer(4310),
                transaction: Transaction::new::<ConnectMsg>(),
                transient: false,
                origin: crate::transport::NatAttemptOrigin::Gateway,
            })
            .await
            .unwrap();
        let Some(Event::OutboundEstablished { .. }) = event_rx.recv().await else {
            panic!("expected OutboundEstablished");
        };
        assert_eq!(
            *connector.origins.lock(),
            vec![crate::transport::NatAttemptOrigin::Gateway]
        );
        driver.abort();
    }

    /// The accept loop, driven through `Box<dyn InboundAcceptor>`: an
    /// expected connection is reported as matched, with the key, transaction
    /// and transient flag its registration carried; an unexpected one with
    /// none of them; each accept marks
    /// the peer ready; and the driver stops when the acceptor yields `None`.
    #[tokio::test(start_paused = true)]
    async fn run_driver_reports_accepted_connections_and_stops_with_the_acceptor() {
        let (conn_tx, conn_rx) = mpsc::channel(4);
        let (cmd_tx, cmd_rx) = mpsc::channel(4);
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let driver = GlobalExecutor::spawn(run_driver(
            Box::new(ChannelAcceptor(conn_rx)),
            Arc::new(ScriptedConnector {
                enqueue: Duration::ZERO,
                handshake: Handshake::Hangs,
                origins: Default::default(),
            }),
            cmd_rx,
            event_tx,
            Some(ready.clone()),
            Arc::new(crate::util::time_source::InstantTimeSrc::new()),
        ));

        let expected_addr: SocketAddr = "127.0.0.1:4300".parse().unwrap();
        let expected_key = TransportKeypair::new().public().clone();
        let tx = Transaction::new::<ConnectMsg>();
        cmd_tx
            .send(Command::ExpectInbound {
                addr: expected_addr,
                peer_key: Some(expected_key.clone()),
                transaction: Some(tx),
                transient: true,
            })
            .await
            .unwrap();
        // Let the driver register the expectation before anything arrives.
        tokio::time::sleep(Duration::from_millis(1)).await;

        conn_tx
            .send(Box::new(NullConnection(expected_addr)))
            .await
            .ok()
            .unwrap();
        let Some(Event::InboundConnection {
            transaction,
            matched,
            connection,
            transient,
        }) = event_rx.recv().await
        else {
            panic!("expected an InboundConnection");
        };
        assert_eq!(matched, InboundMatch::Expected(Some(expected_key)));
        assert_eq!(transaction, Some(tx));
        assert!(transient);
        assert_eq!(connection.remote_addr(), expected_addr);
        assert!(ready.load(std::sync::atomic::Ordering::SeqCst));

        let stranger: SocketAddr = "192.0.2.9:4301".parse().unwrap();
        conn_tx
            .send(Box::new(NullConnection(stranger)))
            .await
            .ok()
            .unwrap();
        let Some(Event::InboundConnection {
            transaction,
            matched,
            connection,
            transient,
        }) = event_rx.recv().await
        else {
            panic!("expected an InboundConnection");
        };
        assert_eq!(matched, InboundMatch::Unexpected);
        assert_eq!(transaction, None);
        assert!(!transient);
        assert_eq!(connection.remote_addr(), stranger);
        ready.store(false, std::sync::atomic::Ordering::SeqCst);
        // Read only after the event, which the driver sends after setting it.
        conn_tx
            .send(Box::new(NullConnection(stranger)))
            .await
            .ok()
            .unwrap();
        assert!(event_rx.recv().await.is_some());
        assert!(
            ready.load(std::sync::atomic::Ordering::SeqCst),
            "every accept marks the peer ready, not only the first"
        );

        drop(conn_tx);
        driver.await.expect("the driver ends cleanly");
        assert!(event_rx.recv().await.is_none(), "the driver has stopped");
    }

    /// What a scripted connector's handshake stage does.
    #[derive(Clone, Copy)]
    enum Handshake {
        Succeeds,
        Fails,
        Hangs,
    }

    /// An `OutboundConnector` whose two stages are scripted: the first
    /// takes `enqueue` (virtual time), the second does `handshake`.
    struct ScriptedConnector {
        enqueue: Duration,
        handshake: Handshake,
        /// The origin each `connect` was called with.
        origins: parking_lot::Mutex<Vec<crate::transport::NatAttemptOrigin>>,
    }

    impl OutboundConnector for ScriptedConnector {
        // Two stages by design: see `OutboundConnector::connect`.
        #[allow(clippy::async_yields_async)]
        fn connect(
            &self,
            _remote_public_key: crate::transport::TransportPublicKey,
            remote_addr: SocketAddr,
            origin: crate::transport::NatAttemptOrigin,
        ) -> Pin<
            Box<dyn std::future::Future<Output = crate::transport::PendingConnection> + Send + '_>,
        > {
            self.origins.lock().push(origin);
            let (enqueue, handshake) = (self.enqueue, self.handshake);
            Box::pin(async move {
                tokio::time::sleep(enqueue).await;
                let pending: crate::transport::PendingConnection = match handshake {
                    Handshake::Succeeds => Box::pin(async move {
                        Ok(Box::new(NullConnection(remote_addr)) as Box<dyn PeerConnectionApi>)
                    }),
                    Handshake::Fails => Box::pin(async {
                        Err(TransportError::ConnectionEstablishmentFailure {
                            cause: "refused".into(),
                        })
                    }),
                    Handshake::Hangs => Box::pin(std::future::pending()),
                };
                pending
            })
        }
    }

    struct ConnectRun {
        event: Event,
        peer: PeerKeyLocation,
        transaction: Transaction,
        ready: bool,
        elapsed: Duration,
        /// The origins the connector was called with.
        origins: Vec<crate::transport::NatAttemptOrigin>,
    }

    /// The origin `run_connect` dials with; any variant would do, as long as
    /// the connector sees exactly it.
    const RUN_ORIGIN: crate::transport::NatAttemptOrigin =
        crate::transport::NatAttemptOrigin::RingPeerNoTransport;

    /// Run `connect_outbound` against a scripted connector.
    async fn run_connect(enqueue: Duration, handshake: Handshake) -> ConnectRun {
        let connector = ScriptedConnector {
            enqueue,
            handshake,
            origins: Default::default(),
        };
        let ready = std::sync::atomic::AtomicBool::new(false);
        let peer = make_peer(4200);
        let transaction = Transaction::new::<ConnectMsg>();
        let start = tokio::time::Instant::now();
        let event = connect_outbound(
            &connector,
            peer.clone(),
            transaction,
            true,
            RUN_ORIGIN,
            Some(&ready),
        )
        .await;
        ConnectRun {
            event,
            peer,
            transaction,
            ready: ready.load(std::sync::atomic::Ordering::SeqCst),
            elapsed: start.elapsed(),
            origins: connector.origins.into_inner(),
        }
    }

    /// A completed handshake becomes `OutboundEstablished` carrying the peer,
    /// transaction and transient flag, and marks the peer ready.
    #[tokio::test(start_paused = true)]
    async fn outbound_connect_success_is_established_and_marks_ready() {
        let run = run_connect(Duration::ZERO, Handshake::Succeeds).await;
        let Event::OutboundEstablished {
            transaction,
            peer,
            connection,
            transient,
        } = run.event
        else {
            panic!("expected OutboundEstablished, got {:?}", run.event);
        };
        assert_eq!(peer, run.peer);
        assert_eq!(transaction, run.transaction);
        assert_eq!(connection.remote_addr(), run.peer.socket_addr().unwrap());
        assert!(transient);
        assert!(run.ready, "a successful connect marks the peer ready");
        assert_eq!(
            run.origins,
            vec![RUN_ORIGIN],
            "the connector gets the dial's origin, once"
        );
    }

    /// A failed handshake becomes `OutboundFailed` with the transport error
    /// converted as before, and does not mark the peer ready.
    #[tokio::test(start_paused = true)]
    async fn outbound_connect_failure_is_failed_and_leaves_ready_unset() {
        let run = run_connect(Duration::ZERO, Handshake::Fails).await;
        let Event::OutboundFailed {
            transaction,
            peer,
            error,
            transient,
        } = run.event
        else {
            panic!("expected OutboundFailed, got {:?}", run.event);
        };
        assert_eq!(peer, run.peer);
        assert_eq!(transaction, run.transaction);
        assert!(transient);
        let expected = ConnectionError::from(TransportError::ConnectionEstablishmentFailure {
            cause: "refused".into(),
        });
        assert!(
            matches!(error, ConnectionError::TransportError(_)),
            "{error:?}"
        );
        assert_eq!(error.to_string(), expected.to_string());
        assert!(!run.ready);
    }

    /// A handshake that never finishes fails as `Timeout` after exactly the
    /// connect timeout.
    #[tokio::test(start_paused = true)]
    async fn outbound_connect_that_hangs_times_out_after_ten_seconds() {
        let run = run_connect(Duration::ZERO, Handshake::Hangs).await;
        let Event::OutboundFailed { error, .. } = run.event else {
            panic!("expected OutboundFailed, got {:?}", run.event);
        };
        assert!(matches!(error, ConnectionError::Timeout), "{error:?}");
        assert_eq!(run.elapsed, Duration::from_secs(10));
        assert!(!run.ready);
    }

    /// Pins CURRENT behaviour, not a requirement: the timeout covers only the
    /// handshake stage, so an attempt that waits longer than the timeout to
    /// be queued still connects. That leaves a stalled listener able to hang
    /// the connect forever; when #5947 bounds the enqueue stage, change this
    /// test to assert the bound.
    #[tokio::test(start_paused = true)]
    async fn outbound_connect_enqueue_stage_is_not_yet_under_the_timeout() {
        let run = run_connect(Duration::from_secs(15), Handshake::Succeeds).await;
        assert!(
            matches!(run.event, Event::OutboundEstablished { .. }),
            "{:?}",
            run.event
        );
        assert_eq!(run.elapsed, Duration::from_secs(15));
        assert!(run.ready);
    }

    #[test]
    fn tracker_returns_registered_entry_exactly_once() {
        let mut tracker = matcher();
        let addr = make_peer(4100).socket_addr().unwrap();
        let tx = Transaction::new::<ConnectMsg>();
        tracker.register(addr, None, Some(tx), true);

        let entry = tracker
            .consume(addr)
            .expect("expected registered inbound entry");
        assert_eq!(entry.transaction, Some(tx));
        assert!(entry.transient);
        assert!(tracker.consume(addr).is_none());
    }

    fn key() -> TransportPublicKey {
        TransportKeypair::new().public().clone()
    }

    /// Inside the window a match carries the registration's key; at the
    /// window's end it still does.
    #[test]
    fn a_match_inside_the_window_carries_the_key() {
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let mut tracker = InboundMatcher::new(Arc::new(clock.clone()));
        let addr = make_peer(4110).socket_addr().unwrap();
        let peer_key = key();
        tracker.register(addr, Some(peer_key.clone()), None, false);
        clock.advance_time(EXPECTED_INBOUND_WINDOW);
        let entry = tracker.consume(addr).expect("registered");
        assert_eq!(entry.peer_key, Some(peer_key));
    }

    /// A match against a registration that has outlived the window is still
    /// a match, but without the key: the address may now be someone else's.
    #[test]
    fn a_match_after_the_window_carries_no_key() {
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let mut tracker = InboundMatcher::new(Arc::new(clock.clone()));
        let addr = make_peer(4120).socket_addr().unwrap();
        tracker.register(addr, Some(key()), None, false);
        clock.advance_time(EXPECTED_INBOUND_WINDOW + Duration::from_millis(1));
        let entry = tracker.consume(addr).expect("still matched");
        assert_eq!(entry.peer_key, None);
        assert!(tracker.consume(addr).is_none(), "consumed once");
    }

    /// With no further registration or match, the driver's periodic sweep
    /// still removes an expired registration: a connection from its address
    /// afterwards is unexpected rather than matched.
    #[tokio::test(start_paused = true)]
    async fn the_driver_sweeps_expired_registrations_without_traffic() {
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let (conn_tx, conn_rx) = mpsc::channel(4);
        let (cmd_tx, cmd_rx) = mpsc::channel(4);
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let driver = GlobalExecutor::spawn(run_driver(
            Box::new(ChannelAcceptor(conn_rx)),
            Arc::new(ScriptedConnector {
                enqueue: Duration::ZERO,
                handshake: Handshake::Hangs,
                origins: Default::default(),
            }),
            cmd_rx,
            event_tx,
            None,
            Arc::new(clock.clone()),
        ));
        let addr: SocketAddr = "127.0.0.1:4140".parse().unwrap();
        cmd_tx
            .send(Command::ExpectInbound {
                addr,
                peer_key: Some(key()),
                transaction: None,
                transient: false,
            })
            .await
            .unwrap();
        let (done, processed) = tokio::sync::oneshot::channel();
        cmd_tx.send(Command::Barrier(done)).await.unwrap();
        processed.await.unwrap();

        // The registration expires; only the timer can sweep it now.
        clock.advance_time(EXPECTED_INBOUND_WINDOW + Duration::from_secs(1));
        tokio::time::sleep(EXPECTED_INBOUND_SWEEP_INTERVAL + Duration::from_secs(1)).await;

        conn_tx
            .send(Box::new(NullConnection(addr)))
            .await
            .ok()
            .unwrap();
        let Some(Event::InboundConnection { matched, .. }) = event_rx.recv().await else {
            panic!("expected an InboundConnection");
        };
        assert_eq!(
            matched,
            InboundMatch::Unexpected,
            "the expired entry was swept"
        );
        driver.abort();
    }

    /// Expired registrations are swept on the next registration and on the
    /// next match, so the map does not keep them.
    #[test]
    fn expired_registrations_are_swept() {
        let clock = crate::util::time_source::SharedMockTimeSource::new();
        let mut tracker = InboundMatcher::new(Arc::new(clock.clone()));
        let (old, recent, other) = (
            make_peer(4130).socket_addr().unwrap(),
            make_peer(4131).socket_addr().unwrap(),
            make_peer(4132).socket_addr().unwrap(),
        );
        tracker.register(old, Some(key()), None, false);
        clock.advance_time(EXPECTED_INBOUND_WINDOW + Duration::from_millis(1));
        tracker.register(recent, Some(key()), None, false);
        assert!(!tracker.contains(old), "swept on registration");
        assert!(tracker.contains(recent));

        clock.advance_time(EXPECTED_INBOUND_WINDOW + Duration::from_millis(1));
        assert!(tracker.consume(other).is_none());
        assert!(!tracker.contains(recent), "swept on a match");
        assert!(tracker.entries.is_empty());
    }

    #[test]
    fn tracker_drop_removes_entry() {
        let mut tracker = matcher();
        let addr = make_peer(4200).socket_addr().unwrap();
        tracker.register(addr, None, None, false);
        assert!(tracker.contains(addr));

        tracker.drop_addr(addr);
        assert!(!tracker.contains(addr));
        assert!(tracker.consume(addr).is_none());
    }

    #[test]
    fn tracker_overwrites_existing_expectation() {
        let mut tracker = matcher();
        let addr = make_peer(4300).socket_addr().unwrap();
        tracker.register(addr, None, None, false);
        let new_tx = Transaction::new::<ConnectMsg>();
        tracker.register(addr, None, Some(new_tx), true);
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
        let mut tracker = matcher();
        let addr_a = make_peer(4400).socket_addr().unwrap();
        let addr_b = make_peer(4401).socket_addr().unwrap();

        tracker.register(addr_a, None, None, false);
        tracker.register(addr_b, None, None, true);

        let first = tracker
            .consume(addr_a)
            .expect("first peer should be matched by exact socket");
        assert!(!first.transient);

        let second = tracker
            .consume(addr_b)
            .expect("second peer should still be tracked independently");
        assert!(second.transient);
    }

    #[test]
    fn command_sender_try_send_returns_false_when_full() {
        // Channel capacity 1 — fill it, then verify try_send returns false
        let (tx, _rx) = mpsc::channel(1);
        let sender = CommandSender(tx);
        let addr = make_peer(4500).socket_addr().unwrap();

        // First send should succeed
        assert!(sender.try_send(Command::DropConnection { addr }));
        // Channel is now full — second send should return false, not block
        assert!(!sender.try_send(Command::DropConnection { addr }));
    }

    #[test]
    fn command_sender_try_send_returns_false_when_closed() {
        let (tx, rx) = mpsc::channel(16);
        let sender = CommandSender(tx);
        let addr = make_peer(4600).socket_addr().unwrap();
        drop(rx); // Close the channel

        assert!(!sender.try_send(Command::DropConnection { addr }));
    }

    /// SITE 8 source-scrape pin (#4145): the run_driver inbound-accept arm must
    /// forward via the BOUNDED `events_tx.send_timeout(` (NOT an unconditional
    /// drop and NOT a blocking `.send().await`), and must distinguish Timeout
    /// (drop as last resort + keep accepting) from Closed (break). run_driver is
    /// not the event-loop task, so a bounded block here cannot reintroduce the
    /// #4145 self-stall; the bound preserves the inbound connection across a
    /// transient backlog instead of dropping it.
    #[test]
    fn run_driver_inbound_accept_is_bounded_send() {
        let full = include_str!("handshake.rs");
        // Scan production only (everything before the `mod tests {` block), so
        // this pin does not match the marker strings in its own assertions.
        let cut = full
            .find("\nmod tests {")
            .expect("handshake.rs must have a `mod tests {` block");
        let src = &full[..cut];

        // Anchor indentation-independently on the bounded send_timeout call in
        // run_driver (there is exactly one in production). Its presence is itself
        // the primary assertion: the forward must NOT be a blocking .send().await
        // (the spawn_outbound blocking send is `events_tx.send(event)`, a
        // different receiver expression) and must NOT be an unconditional
        // try_send-drop.
        let send_at = src.find(".send_timeout(").unwrap_or_else(|| {
            panic!(
                "#4145 SITE 8: run_driver's inbound-accept arm must forward via \
                 events_tx.send_timeout(Event::InboundConnection ...), not a blocking \
                 send or an unconditional try_send-drop."
            )
        });
        // Window from the send_timeout call through its Timeout/Closed match
        // arms (the Closed arm is ~22 lines / ~1.3k chars after the call).
        let end = (send_at + 1600).min(src.len());
        let arm = &src[send_at..end];
        assert!(
            arm.contains("SendTimeoutError::Timeout") && arm.contains("SendTimeoutError::Closed"),
            "#4145 SITE 8: the accept arm must explicitly distinguish Timeout \
             (drop + continue) from Closed (break). Slice:\n{arm}"
        );
        // The unconditional-drop form (try_send) must NOT be how the inbound
        // connection is forwarded anywhere in production — that would needlessly
        // drop a recoverable inbound connection.
        assert!(
            !src.contains("try_send(Event::InboundConnection"),
            "#4145 SITE 8: inbound forward must use the bounded send_timeout, not an \
             unconditional try_send-drop (see review item 3)."
        );
    }

    /// SITE 8 behavioral pin for the bounded send_timeout semantics:
    /// - a channel that drains WITHIN the bound must SUCCEED (connection preserved,
    ///   not dropped);
    /// - a channel that stays full PAST the bound returns Timeout (drop + continue);
    /// - a closed channel returns Closed (break).
    #[tokio::test(start_paused = true)]
    async fn bounded_inbound_send_preserves_then_times_out_then_closes() {
        use std::time::Duration;

        // (1) Drains within the bound -> Ok (preserved, NOT dropped).
        // Channel cap 1, pre-filled. A drainer frees the slot at t=1s, well
        // inside the 5s bound, so the bounded send must succeed.
        let (tx, mut rx) = mpsc::channel::<u32>(1);
        tx.try_send(1).expect("fill the single slot");
        let drainer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(
                rx.recv().await,
                Some(1),
                "drainer frees the pre-filled slot"
            );
            rx // keep rx alive (and now holding `2` after the send below lands)
        });
        let res = tx.send_timeout(2, Duration::from_secs(5)).await;
        assert!(
            res.is_ok(),
            "a channel that drains within the bound must preserve the connection (Ok), got {res:?}"
        );
        let mut rx = drainer.await.unwrap();
        // After (1) the channel holds `2` (the send that just succeeded). Drain
        // it so we start (2) from a known-empty channel.
        assert_eq!(rx.recv().await, Some(2), "drain the value sent in step (1)");

        // (2) Full past the bound -> Timeout (drop + continue).
        // Fill the slot and never drain; the paused clock auto-advances when the
        // runtime is otherwise idle, so the 5s timeout elapses deterministically.
        tx.try_send(3).expect("re-fill the single slot");
        let res = tx.send_timeout(4, Duration::from_secs(5)).await;
        assert!(
            matches!(res, Err(mpsc::error::SendTimeoutError::Timeout(_))),
            "a channel full past the bound must time out (drop as last resort), got {res:?}"
        );

        // (3) Closed -> Closed (break). Drain the `3` from step (2), then drop the
        // receiver so the cause is unambiguously Closed (not Timeout).
        assert_eq!(rx.recv().await, Some(3), "drain the value from step (2)");
        drop(rx);
        let res = tx.send_timeout(5, Duration::from_secs(5)).await;
        assert!(
            matches!(res, Err(mpsc::error::SendTimeoutError::Closed(_))),
            "a closed channel (event loop gone) must report Closed (break), got {res:?}"
        );
    }
}
