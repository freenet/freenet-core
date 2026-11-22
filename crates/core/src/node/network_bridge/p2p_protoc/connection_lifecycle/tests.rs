//! Connection identity telemetry and bookkeeping, driven through the real
//! `P2pConnManager` handlers: `handle_handshake_action`,
//! `handle_successful_connection`, `handle_transport_event` and
//! `handle_connect_peer`.
//!
//! Each test watches its own remote addresses on the process-global
//! `TRANSPORT_METRICS` (see `watch_conn_identity`), so tests running in
//! parallel in one process do not see each other's decisions. Addresses
//! are unique per test for the same reason.

use super::*;
use crate::message::Transaction;
use crate::operations::VisitedPeers;
use crate::operations::connect::ConnectRequest;
use crate::transport::{ExpectedInboundTracker, StreamId, TransportKeypair};
use freenet_stdlib::prelude::ContractInstanceId;

/// A connection that never delivers anything and accepts every send.
struct IdleConnection {
    addr: SocketAddr,
    receipt: Option<crate::transport::ReceiptStamp>,
}

impl IdleConnection {
    /// A connection that reports no receipt stamp, so a dial over it holds
    /// its key for as long as it is open.
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            receipt: None,
        }
    }
}

impl PeerConnectionApi for IdleConnection {
    fn remote_addr(&self) -> SocketAddr {
        self.addr
    }

    fn receipt_stamp(&self) -> Option<crate::transport::ReceiptStamp> {
        self.receipt.clone()
    }

    fn remote_version(&self) -> Option<(u8, u8, u16)> {
        None
    }

    fn send_bytes(
        &mut self,
        _data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn recv(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, TransportError>> + Send + '_>> {
        Box::pin(std::future::pending())
    }

    fn set_inbound_stream_registry(
        &mut self,
        _registry: Arc<dyn crate::transport::InboundStreamRegistry>,
    ) {
    }

    fn set_peer_identity(&mut self, key: TransportPublicKey) {
        TRANSPORT_KEYS
            .lock()
            .unwrap()
            .entry(self.addr)
            .or_default()
            .push(key);
    }

    fn send_stream_data(
        &mut self,
        _stream_id: StreamId,
        _data: bytes::Bytes,
        _metadata: Option<bytes::Bytes>,
        _completion_tx: Option<tokio::sync::oneshot::Sender<crate::transport::StreamSendOutcome>>,
        _progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn pipe_stream_data(
        &mut self,
        _outbound_stream_id: StreamId,
        _inbound_handle: crate::transport::IncomingStream,
        _metadata: Option<bytes::Bytes>,
        _progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

/// The keys each `IdleConnection` was given through `set_peer_identity`, by
/// remote address (unique per test, see [`Harness::use_addr`]).
static TRANSPORT_KEYS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<SocketAddr, Vec<TransportPublicKey>>>,
> = std::sync::LazyLock::new(Default::default);

/// A `P2pConnManager` built the way the node builds it, with the channels
/// its handlers need kept alive.
struct Harness {
    mgr: P2pConnManager,
    state: EventListenerState,
    handshake: HandshakeCommandSender,
    handshake_rx: mpsc::Receiver<HandshakeCommand>,
    _conn_events_rx: mpsc::Receiver<ConnEvent>,
    /// Identifies this harness in [`ADDRESS_OWNERS`].
    token: u64,
}

/// Which harness uses each remote address. Identity telemetry is recorded
/// per remote address, process-wide, so two tests using one address see
/// each other's records when they run in one process (plain `cargo test`).
/// [`Harness::use_addr`] refuses an address another harness already used.
static ADDRESS_OWNERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<SocketAddr, u64>>,
> = std::sync::LazyLock::new(Default::default);
static NEXT_HARNESS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl Harness {
    async fn new(id: &str) -> Self {
        Self::with_config(id, |_| {}).await
    }

    /// As [`Self::new`], with `adjust` applied to the node config first.
    async fn with_config(id: &str, adjust: impl FnOnce(&mut crate::node::NodeConfig)) -> Self {
        Self::build(id, adjust, true).await
    }

    /// As [`Self::new`], with a ring that is not a gateway's, so a transient
    /// connection is not promoted. The bridge is still built as a gateway's,
    /// since it needs no configured remote gateway that way.
    async fn with_non_gateway_ring(id: &str) -> Self {
        Self::build(id, |_| {}, false).await
    }

    async fn build(
        id: &str,
        adjust: impl FnOnce(&mut crate::node::NodeConfig),
        ring_is_gateway: bool,
    ) -> Self {
        let config_args = crate::config::ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let mut node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        // A gateway needs no configured remote gateway to build, and accepts
        // unidentified inbound connections, which is the path under test.
        node_config.is_gateway = true;
        adjust(&mut node_config);
        let (_notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, _ch_channel, _wait_for_event) =
            crate::contract::contract_handler_channel();
        node_config.is_gateway = ring_is_gateway;
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        node_config.is_gateway = true;
        let (result_router_tx, _result_router_rx) = mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let op_manager = Arc::new(
            crate::node::OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);
        let mut mgr = P2pConnManager::build(
            &node_config,
            op_manager,
            crate::tracing::DynamicRegister::new(vec![]),
            &task_monitor,
        )
        .await
        .expect("build P2pConnManager");
        let (conn_events_tx, conn_events_rx) = mpsc::channel(64);
        mgr.conn_event_tx = Some(conn_events_tx);
        // As a node whose transport has a QUIC endpoint, so promotions give
        // the transport its key; `quic_off_gives_no_transport_a_key` turns it
        // off.
        mgr.transport_has_quic = true;
        let (handshake_tx, handshake_rx) = mpsc::channel(128);
        Self {
            token: NEXT_HARNESS.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            mgr,
            state: EventListenerState::new(Arc::new(ExpectedInboundTracker::empty_for_test())),
            handshake: HandshakeCommandSender::for_test(handshake_tx),
            handshake_rx,
            _conn_events_rx: conn_events_rx,
        }
    }

    /// [`Self::use_addr`] for the remote address of a handshake event.
    fn use_event_addr(&self, event: &HandshakeEvent) {
        match event {
            HandshakeEvent::InboundConnection { connection, .. }
            | HandshakeEvent::OutboundEstablished { connection, .. } => {
                self.use_addr(connection.remote_addr())
            }
            HandshakeEvent::OutboundFailed { peer, .. } => {
                if let Some(addr) = peer.socket_addr() {
                    self.use_addr(addr)
                }
            }
        }
    }

    /// Record that this harness uses `addr`, panicking if another harness
    /// in this process already did (see [`ADDRESS_OWNERS`]).
    fn use_addr(&self, addr: SocketAddr) {
        let owner = *ADDRESS_OWNERS
            .lock()
            .unwrap()
            .entry(addr)
            .or_insert(self.token);
        assert_eq!(
            owner, self.token,
            "{addr} is already used by another test; give this test its own address"
        );
    }

    /// Wait until the task of the open connection at `addr`, if any, has
    /// handled every event sent to it so far: a barrier sent through the same
    /// channel is acknowledged after them.
    async fn settle(&self, addr: SocketAddr) {
        let Some(entry) = self.mgr.connections.get(&addr) else {
            return;
        };
        let (ack, acked) = tokio::sync::oneshot::channel();
        entry
            .sender
            .send(Right(ConnEvent::Barrier(ack)))
            .await
            .expect("the connection's task is running");
        acked
            .await
            .expect("the connection's task acknowledges the barrier");
    }

    /// The keys the transport of the connection at `addr` (an
    /// `IdleConnection`) has been given, once its task has handled
    /// everything sent to it.
    async fn transport_keys(&self, addr: SocketAddr) -> Vec<TransportPublicKey> {
        self.settle(addr).await;
        TRANSPORT_KEYS
            .lock()
            .unwrap()
            .get(&addr)
            .cloned()
            .unwrap_or_default()
    }

    /// The key decisions recorded for `addr` ([`is_transport_kind`]), once
    /// the connection's task has handled everything sent to it, so a real
    /// transport has recorded what it took.
    async fn transport_seen(&self, addr: SocketAddr) -> Vec<ConnIdentity> {
        self.settle(addr).await;
        TRANSPORT_METRICS
            .conn_identity_from(addr)
            .into_iter()
            .filter(|kind| is_transport_kind(*kind))
            .collect()
    }

    fn own_key(&self) -> TransportPublicKey {
        (*self.mgr.bridge.op_manager.ring.connection_manager.pub_key).clone()
    }

    /// A connection established by the transport. `peer` is `Some` for a
    /// dial, `None` for an inbound connection.
    async fn establish(
        &mut self,
        addr: SocketAddr,
        peer: Option<PeerKeyLocation>,
        transient: bool,
    ) {
        self.use_addr(addr);
        self.mgr
            .handle_successful_connection(
                peer,
                Box::new(IdleConnection::new(addr)),
                &mut self.state,
                None,
                transient,
                &self.handshake,
                IdentitySource::Dialled,
            )
            .await
            .expect("handle_successful_connection");
        assert!(self.mgr.connections.contains_key(&addr), "connection kept");
        self.assert_open_transports_match();
    }

    /// The outbound dial path: `HandshakeEvent::OutboundEstablished`.
    async fn dial(&mut self, addr: SocketAddr, key: &TransportPublicKey) {
        self.establish(addr, Some(PeerKeyLocation::new(key.clone(), addr)), false)
            .await;
    }

    /// The gateway inbound path: an inbound connection with no expectation.
    async fn accept(&mut self, addr: SocketAddr) {
        self.establish(addr, None, true).await;
    }

    /// A ConnectRequest from the connection at `remote` naming `joiner`,
    /// which the handler passes on.
    async fn connect_request(&mut self, remote: SocketAddr, joiner: PeerKeyLocation) {
        assert!(
            self.try_connect_request(remote, joiner).await,
            "message is passed on"
        );
    }

    /// As [`Self::connect_request`], for a request the handler drops.
    async fn refused_connect_request(&mut self, remote: SocketAddr, joiner: PeerKeyLocation) {
        assert!(
            !self.try_connect_request(remote, joiner).await,
            "message is dropped"
        );
    }

    /// A ConnectRequest from the connection at `remote` naming `joiner`;
    /// returns whether the handler passed it on.
    async fn try_connect_request(&mut self, remote: SocketAddr, joiner: PeerKeyLocation) -> bool {
        self.use_addr(remote);
        let tx = Transaction::new::<ConnectMsg>();
        let msg = NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request {
            id: tx,
            payload: ConnectRequest {
                desired_location: Location::new(0.5),
                joiner,
                ttl: 4,
                visited: VisitedPeers::new(&tx),
                uphill_budget: 8,
                native_offer: None,
            },
        }));
        let result = self
            .mgr
            .handle_transport_event(
                Some(ConnEvent::InboundMessage(IncomingMessage::with_remote(
                    msg, remote,
                ))),
                &mut self.state,
                &self.handshake,
            )
            .await
            .expect("handle_transport_event");
        match result {
            EventResult::Event(_) => true,
            EventResult::Continue => false,
        }
    }

    /// A ConnectPeer (connect op accepted `peer`) for an existing connection.
    async fn connect_peer(&mut self, peer: PeerKeyLocation) {
        assert!(
            self.try_connect_peer(peer).await,
            "the existing connection must be reused"
        );
    }

    /// As [`Self::connect_peer`], returning whether the callback got `Ok`.
    async fn try_connect_peer(&mut self, peer: PeerKeyLocation) -> bool {
        if let Some(addr) = peer.socket_addr() {
            self.use_addr(addr);
        }
        let (callback, mut result_rx) = mpsc::channel::<crate::message::ConnectResult>(1);
        self.mgr
            .handle_connect_peer(
                peer,
                Box::new(callback),
                Transaction::new::<ConnectMsg>(),
                &self.handshake,
                &mut self.state,
                true,
                crate::transport::NatAttemptOrigin::ConnectAcceptor,
                crate::operations::connect::native::DialTransport::Udp,
            )
            .await
            .expect("handle_connect_peer");
        matches!(result_rx.try_recv(), Ok(Ok(_)))
    }

    /// An inbound connection matched to an expected-inbound registration
    /// (`NodeEvent::ExpectPeerConnection`) for the peer the connect op dialled
    /// under `dialled`, as the handshake driver reports it: not transient.
    async fn accept_expected(&mut self, addr: SocketAddr, dialled: &TransportPublicKey) {
        self.accept_matched(addr, HandshakeInboundMatch::Expected(Some(dialled.clone())))
            .await;
    }

    /// As [`Self::accept_expected`], for a registration that carried no key.
    async fn accept_expected_unkeyed(&mut self, addr: SocketAddr) {
        self.accept_matched(addr, HandshakeInboundMatch::Expected(None))
            .await;
    }

    async fn accept_matched(&mut self, addr: SocketAddr, matched: HandshakeInboundMatch) {
        self.handshake_event(HandshakeEvent::InboundConnection {
            transaction: None,
            matched,
            connection: Box::new(IdleConnection::new(addr)),
            transient: false,
        })
        .await;
        assert!(self.mgr.connections.contains_key(&addr), "connection kept");
        self.assert_open_transports_match();
    }

    /// A handshake event, handled by the event loop's handler.
    /// Stands in for the listener's first-frame report of the native
    /// connection at `addr`: the entry's own, or the one held beside it.
    fn prove_native(&mut self, addr: SocketAddr) {
        let id = match self.mgr.held_native.get(&addr) {
            Some(held) => held.connection_id,
            None => self.mgr.connections[&addr].connection_id,
        };
        self.mgr.on_native_first_frame(addr, id);
    }

    async fn handshake_event(&mut self, event: HandshakeEvent) {
        self.use_event_addr(&event);
        self.mgr
            .handle_handshake_action(event, &mut self.state, &self.handshake)
            .await
            .expect("handle_handshake_action");
    }

    /// Nothing this node holds is keyed by its own key: not the
    /// key-to-address map, not a connection entry, not the ring.
    fn assert_own_key_absent(&self) {
        let own = self.own_key();
        assert!(
            !self.mgr.addr_by_pub_key.contains_key(&own),
            "addr_by_pub_key holds this node's key"
        );
        assert!(
            self.mgr
                .connections
                .values()
                .all(|entry| entry.pub_key() != Some(&own)),
            "a connection is recorded under this node's key"
        );
        assert!(
            self.mgr
                .bridge
                .op_manager
                .ring
                .connection_manager
                .get_peer_by_pub_key(&own)
                .is_none(),
            "the ring holds a connection under this node's key"
        );
    }

    fn ring_key_at(&self, addr: SocketAddr) -> Option<TransportPublicKey> {
        self.mgr
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(addr)
            .map(|peer| peer.pub_key().clone())
    }

    async fn transport_closed(&mut self, addr: SocketAddr) {
        let connection_id = self.mgr.connections[&addr].connection_id;
        self.mgr
            .handle_transport_event(
                Some(ConnEvent::TransportClosed {
                    remote_addr: addr,
                    error: TransportError::ConnectionClosed(addr),
                    connection_id,
                    reason: crate::tracing::DisconnectReason::Timeout,
                }),
                &mut self.state,
                &self.handshake,
            )
            .await
            .expect("handle_transport_event");
        assert!(!self.mgr.connections.contains_key(&addr));
        self.assert_open_transports_match();
    }

    /// The connection manager's open-transport set, which the relay reads
    /// for its QUIC-native decision, holds exactly the bridge's connection
    /// map's addresses.
    fn assert_open_transports_match(&self) {
        let map: std::collections::BTreeSet<SocketAddr> =
            self.mgr.connections.keys().copied().collect();
        assert_eq!(
            self.mgr
                .bridge
                .op_manager
                .ring
                .connection_manager
                .open_transport_addrs(),
            map
        );
    }
}

fn key() -> TransportPublicKey {
    TransportKeypair::new().public().clone()
}

impl Harness {
    /// Record the identity decisions for `addrs`, which this test then owns
    /// (see [`Harness::use_addr`]).
    fn watch(&self, addrs: &[SocketAddr]) {
        for addr in addrs {
            self.use_addr(*addr);
            TRANSPORT_METRICS.watch_conn_identity(*addr);
        }
    }
}

/// The identity decisions the node made for `addr`. Leaves out the counts
/// of a key given to the transport ([`is_transport_kind`]), which
/// [`Harness::transport_seen`] reads: a real transport records its part from
/// the connection's task, after the handler returns.
fn seen(addr: SocketAddr) -> Vec<ConnIdentity> {
    TRANSPORT_METRICS
        .conn_identity_from(addr)
        .into_iter()
        .filter(|kind| !is_transport_kind(*kind))
        .collect()
}

fn is_transport_kind(kind: ConnIdentity) -> bool {
    matches!(
        kind,
        ConnIdentity::TransportKeySent
            | ConnIdentity::TransportKeyUndelivered
            | ConnIdentity::TransportKeySet
            | ConnIdentity::TransportKeySame
            | ConnIdentity::TransportKeyRefused
    )
}

fn is_set(kind: ConnIdentity) -> bool {
    match kind {
        ConnIdentity::SetDialed
        | ConnIdentity::SetProven
        | ConnIdentity::ExpectedInboundBound
        | ConnIdentity::ExpectedInboundUnbound
        | ConnIdentity::SetConnectRequest
        | ConnIdentity::SetConnectPeer
        | ConnIdentity::SameConnectRequest
        | ConnIdentity::SameConnectPeer
        | ConnIdentity::PendingClaimBound => true,
        ConnIdentity::ChangedConnectRequest
        | ConnIdentity::ConnectRequestKnownAddr
        | ConnIdentity::ChangedConnectPeer
        | ConnIdentity::ClaimHeld
        | ConnIdentity::QuietHolderReplaced
        | ConnIdentity::ClaimHeldLong
        | ConnIdentity::OwnKey
        | ConnIdentity::DuplicateKey
        | ConnIdentity::ChangedRingPromotion
        | ConnIdentity::TeardownKeyElsewhere
        | ConnIdentity::TeardownUnidentified
        | ConnIdentity::TeardownKeyUnmapped
        | ConnIdentity::PromotionKeyMismatch
        | ConnIdentity::TransportKeySet
        | ConnIdentity::TransportKeySame
        | ConnIdentity::TransportKeyRefused
        | ConnIdentity::TransportKeySent
        | ConnIdentity::TransportKeyUndelivered => false,
    }
}

/// Ordinary establishment gives the field data a known baseline: a dial, a
/// gateway accepting an unidentified inbound connection that identifies
/// itself and is then accepted by the connect op, and a peer restarting at
/// the same address produce only `set_*` and `same_*` decisions.
#[tokio::test]
async fn ordinary_establishment_counts_only_set_and_same() {
    let mut h = Harness::new("conn-identity-baseline").await;
    let dialed: SocketAddr = "203.0.113.10:4101".parse().unwrap();
    let joiner: SocketAddr = "203.0.113.11:4102".parse().unwrap();
    h.watch(&[dialed, joiner]);
    let (dialed_key, joiner_key) = (key(), key());

    h.dial(dialed, &dialed_key).await;
    assert_eq!(seen(dialed), vec![ConnIdentity::SetDialed]);

    // A joiner reaches the gateway: unidentified until its ConnectRequest.
    // A CONNECT round sends several Requests on one connection.
    h.accept(joiner).await;
    assert!(seen(joiner).is_empty(), "nothing is known at accept");
    h.connect_request(
        joiner,
        PeerKeyLocation::with_unknown_addr(joiner_key.clone()),
    )
    .await;
    h.connect_request(
        joiner,
        PeerKeyLocation::with_unknown_addr(joiner_key.clone()),
    )
    .await;
    h.connect_peer(PeerKeyLocation::new(joiner_key.clone(), joiner))
        .await;
    assert_eq!(
        seen(joiner),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::SameConnectRequest,
            ConnIdentity::SameConnectPeer,
        ]
    );

    // The joiner restarts at the same address: the transport hands over a
    // new connection, which replaces the old entry, then identifies itself.
    h.accept(joiner).await;
    h.connect_request(
        joiner,
        PeerKeyLocation::with_unknown_addr(joiner_key.clone()),
    )
    .await;

    for addr in [dialed, joiner] {
        let kinds = seen(addr);
        assert!(
            kinds.iter().all(|k| is_set(*k)),
            "ordinary establishment at {addr} counted {kinds:?}"
        );
    }
    assert_eq!(
        seen(joiner).last(),
        Some(&ConnIdentity::SetConnectRequest),
        "the restarted connection is identified afresh"
    );
}

/// A second ConnectRequest naming another key does not change the
/// connection's key: it counts `changed_connect_request`, the request is
/// dropped, and the connection is closed. Nothing is recorded under the new
/// key, and the first key's mapping goes with the connection.
#[tokio::test]
async fn connect_request_naming_another_key_closes_the_connection() {
    let mut h = Harness::new("conn-identity-changed-request").await;
    let remote: SocketAddr = "203.0.113.20:4201".parse().unwrap();
    h.watch(&[remote]);
    let (first, second) = (key(), key());

    h.accept(remote).await;
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(first.clone()))
        .await;
    assert_eq!(h.mgr.connections[&remote].pub_key(), Some(&first));
    h.refused_connect_request(remote, PeerKeyLocation::with_unknown_addr(second.clone()))
        .await;

    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::ChangedConnectRequest
        ]
    );
    assert!(
        !h.mgr.connections.contains_key(&remote),
        "the connection is closed"
    );
    assert!(!h.mgr.addr_by_pub_key.contains_key(&second));
    assert!(!h.mgr.addr_by_pub_key.contains_key(&first));
}

/// A ConnectRequest whose joiner address is given as the connection's own
/// remote address counts `connect_request_known_addr` alongside its kind; one
/// whose joiner address is another known address is not an identity claim
/// for this connection and counts nothing.
#[tokio::test]
async fn connect_request_with_known_addr_counts_known_addr() {
    let mut h = Harness::new("conn-identity-known-addr").await;
    let remote: SocketAddr = "203.0.113.30:4301".parse().unwrap();
    let elsewhere: SocketAddr = "203.0.113.31:4302".parse().unwrap();
    h.watch(&[remote]);

    h.accept(remote).await;
    h.connect_request(remote, PeerKeyLocation::new(key(), elsewhere))
        .await;
    assert!(
        seen(remote).is_empty(),
        "a forwarded request is not a claim"
    );

    h.connect_request(remote, PeerKeyLocation::new(key(), remote))
        .await;
    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::ConnectRequestKnownAddr
        ]
    );
}

/// A ConnectPeer for an unidentified connection sets its key.
#[tokio::test]
async fn connect_peer_on_unidentified_connection_counts_set() {
    let mut h = Harness::new("conn-identity-set-connect-peer").await;
    let remote: SocketAddr = "203.0.113.40:4401".parse().unwrap();
    h.watch(&[remote]);
    let peer_key = key();

    h.accept(remote).await;
    h.connect_peer(PeerKeyLocation::new(peer_key.clone(), remote))
        .await;

    assert_eq!(seen(remote), vec![ConnIdentity::SetConnectPeer]);
    assert_eq!(h.mgr.connections[&remote].pub_key(), Some(&peer_key));
}

/// A ConnectPeer naming a key other than the connection's is refused: the
/// connect op is told it failed, the connection keeps its key and is not
/// promoted into the ring, and nothing is recorded under the other key.
#[tokio::test]
async fn connect_peer_naming_another_key_is_refused() {
    let mut h = Harness::new("conn-identity-changed-connect-peer").await;
    let remote: SocketAddr = "203.0.113.50:4501".parse().unwrap();
    h.watch(&[remote]);
    let (recorded, other) = (key(), key());

    h.accept(remote).await;
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(recorded.clone()))
        .await;
    assert!(
        !h.try_connect_peer(PeerKeyLocation::new(other.clone(), remote))
            .await,
        "the ConnectPeer is refused"
    );

    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::ChangedConnectPeer,
        ]
    );
    assert_eq!(
        h.mgr.connections[&remote].pub_key(),
        Some(&recorded),
        "the connection keeps its key"
    );
    assert_eq!(h.ring_key_at(remote), None, "nothing is promoted");
    assert!(!h.mgr.addr_by_pub_key.contains_key(&other));

    // The ConnectPeer naming the connection's own key still promotes it.
    h.connect_peer(PeerKeyLocation::new(recorded.clone(), remote))
        .await;
    assert_eq!(h.ring_key_at(remote).as_ref(), Some(&recorded));
}

/// An inbound connection matched to an expected-inbound registration that
/// carried no key starts without a peer identity, exactly like an unexpected
/// one: no key on the connection, nothing in the key-to-address map, nothing
/// in the ring. Only the match is counted.
#[tokio::test]
async fn expected_inbound_without_a_key_starts_without_a_peer_identity() {
    let mut h = Harness::new("conn-identity-expected-no-key").await;
    let remote: SocketAddr = "203.0.113.60:4601".parse().unwrap();
    h.watch(&[remote]);
    h.accept_expected_unkeyed(remote).await;

    assert_eq!(seen(remote), vec![ConnIdentity::ExpectedInboundUnbound]);
    assert_eq!(h.mgr.connections[&remote].pub_key(), None);
    assert!(h.mgr.addr_by_pub_key.is_empty(), "nothing is mapped");
    assert_eq!(h.ring_key_at(remote), None, "not in the ring");
    h.assert_own_key_absent();

    // Its close is the close of an unidentified connection.
    h.transport_closed(remote).await;
    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::ExpectedInboundUnbound,
            ConnIdentity::TeardownUnidentified
        ]
    );
}

/// Dropping a connection that never identified itself counts
/// `teardown_unidentified`.
#[tokio::test]
async fn dropping_an_unidentified_connection_counts_teardown_unidentified() {
    let mut h = Harness::new("conn-identity-teardown-unidentified").await;
    let remote: SocketAddr = "203.0.113.80:4801".parse().unwrap();
    h.watch(&[remote]);

    h.accept(remote).await;
    let handshake = h.handshake.clone();
    assert!(
        h.mgr
            .drop_connection_by_addr(remote, &handshake, crate::tracing::DisconnectReason::Pruned,)
            .await
    );

    assert_eq!(seen(remote), vec![ConnIdentity::TeardownUnidentified]);
}

/// A matched inbound connection whose registration carried no key is named
/// by the ConnectPeer for its address or by the peer's ConnectRequest,
/// whichever comes first; the ConnectPeer promotes it into the ring under
/// the peer's key, and at no point is anything recorded or promoted under
/// this node's own key. Several such connections can be open at once.
#[tokio::test]
async fn expected_inbound_without_a_key_is_identified_by_the_named_peer() {
    let mut h = Harness::new("conn-identity-expected-named").await;
    let by_connect_peer: SocketAddr = "203.0.113.90:4901".parse().unwrap();
    let by_request: SocketAddr = "203.0.113.91:4902".parse().unwrap();
    h.watch(&[by_connect_peer, by_request]);
    let (first, second) = (key(), key());

    h.accept_expected_unkeyed(by_connect_peer).await;
    h.accept_expected_unkeyed(by_request).await;
    h.assert_own_key_absent();

    // ConnectPeer first: it records the key and promotes under it.
    h.connect_peer(PeerKeyLocation::new(first.clone(), by_connect_peer))
        .await;
    h.assert_own_key_absent();
    h.connect_request(
        by_connect_peer,
        PeerKeyLocation::with_unknown_addr(first.clone()),
    )
    .await;
    assert_eq!(
        seen(by_connect_peer),
        vec![
            ConnIdentity::ExpectedInboundUnbound,
            ConnIdentity::SetConnectPeer,
            ConnIdentity::SameConnectRequest,
        ]
    );

    // ConnectRequest first: it records the key; the ConnectPeer that
    // follows promotes under the same key.
    h.connect_request(
        by_request,
        PeerKeyLocation::with_unknown_addr(second.clone()),
    )
    .await;
    assert_eq!(
        h.ring_key_at(by_request),
        None,
        "a request does not promote"
    );
    h.connect_peer(PeerKeyLocation::new(second.clone(), by_request))
        .await;
    assert_eq!(
        seen(by_request),
        vec![
            ConnIdentity::ExpectedInboundUnbound,
            ConnIdentity::SetConnectRequest,
            ConnIdentity::SameConnectPeer,
        ]
    );

    for (addr, peer_key) in [(by_connect_peer, &first), (by_request, &second)] {
        assert_eq!(h.mgr.connections[&addr].pub_key(), Some(peer_key));
        assert_eq!(h.mgr.addr_by_pub_key.get(peer_key), Some(&addr));
        assert_eq!(
            h.ring_key_at(addr).as_ref(),
            Some(peer_key),
            "promoted under the peer's key"
        );
        let kinds = seen(addr);
        assert!(
            kinds.iter().all(|k| is_set(*k)),
            "the expected-inbound path at {addr} counted {kinds:?}"
        );
    }
    h.assert_own_key_absent();

    // A close of one counts nothing: its key is mapped to it.
    h.transport_closed(by_connect_peer).await;
    assert_eq!(
        seen(by_connect_peer).len(),
        3,
        "{:?}",
        seen(by_connect_peer)
    );
    h.assert_own_key_absent();
}

/// A ConnectRequest naming this node's own key counts `own_key` too.
#[tokio::test]
async fn connect_request_naming_own_key_counts_own_key() {
    let mut h = Harness::new("conn-identity-own-key-request").await;
    let remote: SocketAddr = "203.0.113.100:5001".parse().unwrap();
    h.watch(&[remote]);

    h.accept(remote).await;
    let own = h.own_key();
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(own))
        .await;

    assert_eq!(
        seen(remote),
        vec![ConnIdentity::OwnKey, ConnIdentity::SetConnectRequest]
    );
}

/// A ConnectPeer naming this node's own key counts `own_key` too.
#[tokio::test]
async fn connect_peer_naming_own_key_counts_own_key() {
    let mut h = Harness::new("conn-identity-own-key-connect-peer").await;
    let remote: SocketAddr = "203.0.113.110:5101".parse().unwrap();
    h.watch(&[remote]);

    h.accept(remote).await;
    let own = h.own_key();
    h.connect_peer(PeerKeyLocation::new(own, remote)).await;

    assert_eq!(
        seen(remote),
        vec![ConnIdentity::OwnKey, ConnIdentity::SetConnectPeer]
    );
}

/// A matched inbound connection is recorded and promoted under the key the
/// connect op dialled, as the dial would be. The ConnectPeer that follows
/// (the order when the inbound connection completes first) finds it already
/// in the ring and promotes nothing again; the peer's own ConnectRequest
/// naming the same key changes nothing.
#[tokio::test]
async fn expected_inbound_is_promoted_under_the_dialled_key_once() {
    let mut h = Harness::new("conn-identity-expected-keyed").await;
    let in_order: SocketAddr = "203.0.113.130:5301".parse().unwrap();
    h.watch(&[in_order]);
    let peer = key();

    h.accept_expected(in_order, &peer).await;
    assert_eq!(h.mgr.connections[&in_order].pub_key(), Some(&peer));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&in_order));
    assert_eq!(
        h.ring_key_at(in_order).as_ref(),
        Some(&peer),
        "promoted on arrival"
    );
    assert_eq!(h_count(&h), 1);
    let age = aged_ring_entry(&h, in_order).await;

    h.connect_peer(PeerKeyLocation::new(peer.clone(), in_order))
        .await;
    assert_eq!(h_count(&h), 1, "the ConnectPeer does not add it again");
    assert!(
        ring_age_ms(&h, in_order) >= age,
        "the ConnectPeer added the connection again (its age restarted)"
    );
    h.connect_request(in_order, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(
        seen(in_order),
        vec![
            ConnIdentity::ExpectedInboundBound,
            ConnIdentity::SameConnectPeer,
            ConnIdentity::SameConnectRequest,
        ]
    );
    h.assert_own_key_absent();
}

/// How long the ring has held its connection at `addr`. The ring
/// deduplicates by address, so adding a connection again leaves the count
/// unchanged and is visible only as a fresh entry whose age restarts.
fn ring_age_ms(h: &Harness, addr: SocketAddr) -> u64 {
    h.mgr
        .bridge
        .op_manager
        .ring
        .connection_manager
        .get_connection_duration_ms(addr)
        .expect("a ring connection")
}

/// Lets the ring connection at `addr` age, and returns its age.
async fn aged_ring_entry(h: &Harness, addr: SocketAddr) -> u64 {
    tokio::time::sleep(Duration::from_millis(50)).await;
    let age = ring_age_ms(h, addr);
    assert!(age >= 50, "premise: the entry has aged ({age} ms)");
    age
}

fn h_count(h: &Harness) -> usize {
    h.mgr
        .bridge
        .op_manager
        .ring
        .connection_manager
        .connection_count()
}

/// Expected-inbound connections, bound to a dialled key or not, put
/// nothing under this node's key into the key-to-address map, so they do not
/// make a later claim of this node's own key on another connection a
/// duplicate: that claim counts `own_key` alone, and a later claim of another
/// key is refused.
#[tokio::test]
async fn own_key_claim_beside_expected_inbound_connections_is_not_a_duplicate() {
    let mut h = Harness::new("conn-identity-own-key-beside-expected").await;
    let remote: SocketAddr = "203.0.113.140:5401".parse().unwrap();
    let keyed: SocketAddr = "203.0.113.141:5402".parse().unwrap();
    let unkeyed: SocketAddr = "203.0.113.142:5403".parse().unwrap();
    h.watch(&[remote]);

    h.accept_expected(keyed, &key()).await;
    h.accept_expected_unkeyed(unkeyed).await;
    h.assert_own_key_absent();
    h.accept(remote).await;
    let own = h.own_key();
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(own))
        .await;
    h.refused_connect_request(remote, PeerKeyLocation::with_unknown_addr(key()))
        .await;

    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::OwnKey,
            ConnIdentity::SetConnectRequest,
            ConnIdentity::ChangedConnectRequest,
        ]
    );
}

/// A ConnectPeer for the peer, as the event loop handles it, with no
/// connection at the address yet: it waits for the dial. Returns the
/// callback's receiver.
async fn pending_connect_peer(
    h: &mut Harness,
    peer: PeerKeyLocation,
) -> mpsc::Receiver<crate::message::ConnectResult> {
    pending_connect_peer_over(
        h,
        peer,
        crate::operations::connect::native::DialTransport::Udp,
    )
    .await
}

/// As [`pending_connect_peer`], dialling over `transport`.
async fn pending_connect_peer_over(
    h: &mut Harness,
    peer: PeerKeyLocation,
    transport: crate::operations::connect::native::DialTransport,
) -> mpsc::Receiver<crate::message::ConnectResult> {
    if let Some(addr) = peer.socket_addr() {
        h.use_addr(addr);
    }
    let (callback, result_rx) = mpsc::channel(1);
    let handshake = h.handshake.clone();
    h.mgr
        .handle_connect_peer(
            peer,
            Box::new(callback),
            Transaction::new::<ConnectMsg>(),
            &handshake,
            &mut h.state,
            false,
            crate::transport::NatAttemptOrigin::ConnectAcceptor,
            transport,
        )
        .await
        .expect("handle_connect_peer");
    result_rx
}

/// A ConnectPeer whose dial is still pending is answered `Ok` by the matched
/// inbound connection from its address, as by the dial: the connection is
/// the one it asked for, under its key.
#[tokio::test]
async fn pending_connect_peer_is_answered_by_the_matched_inbound_connection() {
    let mut h = Harness::new("conn-identity-expected-answers-pending").await;
    let remote: SocketAddr = "203.0.113.160:5601".parse().unwrap();
    h.watch(&[remote]);
    let peer = key();

    let mut result = pending_connect_peer(&mut h, PeerKeyLocation::new(peer.clone(), remote)).await;
    assert!(result.try_recv().is_err(), "premise: the dial is pending");
    h.accept_expected(remote, &peer).await;

    assert_eq!(result.try_recv(), Ok(Ok((remote, None))));
    assert!(
        !h.state.awaiting_connection.contains_key(&remote),
        "the pending ConnectPeer is cleared"
    );
    assert_eq!(h.ring_key_at(remote).as_ref(), Some(&peer));
    assert_eq!(seen(remote), vec![ConnIdentity::ExpectedInboundBound]);
}

/// At the ring's connection cap a matched inbound connection is refused as a
/// successful dial would be: a pending ConnectPeer gets the same answer, and
/// the connection is dropped with nothing left under its key in the
/// connection map, the key-to-address map or the ring.
#[tokio::test]
async fn matched_inbound_at_the_cap_is_refused_like_a_dial() {
    let mut h = Harness::with_config("conn-identity-expected-at-cap", |config| {
        config.min_number_conn = Some(1);
        config.max_number_conn = Some(2);
    })
    .await;
    for port in [5701, 5702] {
        let addr: SocketAddr = format!("203.0.113.170:{port}").parse().unwrap();
        h.dial(addr, &key()).await;
    }
    assert_eq!(h_count(&h), 2, "premise: at the cap");

    let mut answers = Vec::new();
    for (port, matched) in [(5703, true), (5704, false)] {
        let addr: SocketAddr = format!("203.0.113.171:{port}").parse().unwrap();
        let peer = key();
        let mut result =
            pending_connect_peer(&mut h, PeerKeyLocation::new(peer.clone(), addr)).await;
        let connection = Box::new(IdleConnection::new(addr));
        h.handshake_event(if matched {
            HandshakeEvent::InboundConnection {
                transaction: None,
                matched: HandshakeInboundMatch::Expected(Some(peer.clone())),
                connection,
                transient: false,
            }
        } else {
            HandshakeEvent::OutboundEstablished {
                transaction: Transaction::new::<ConnectMsg>(),
                peer: PeerKeyLocation::new(peer.clone(), addr),
                connection,
                transient: false,
                transport: crate::operations::connect::native::DialTransport::Udp,
                origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
            }
        })
        .await;
        answers.push(result.try_recv());
        assert!(!h.mgr.connections.contains_key(&addr), "dropped");
        assert!(!h.mgr.addr_by_pub_key.contains_key(&peer));
        assert_eq!(h.ring_key_at(addr), None, "not in the ring");
        assert_eq!(h_count(&h), 2);
    }
    assert_eq!(
        answers[0], answers[1],
        "the matched inbound is answered as the dial"
    );
    h.assert_own_key_absent();
}

/// The mock gateway transport's accepted connections, offered to the
/// handshake driver as `InboundConnectionHandler` offers the real ones.
struct MockAcceptor(
    mpsc::Receiver<crate::transport::PeerConnection<crate::transport::mock_transport::MockSocket>>,
);

impl crate::transport::InboundAcceptor for MockAcceptor {
    fn next_connection(&mut self) -> crate::transport::NextConnection<'_> {
        Box::pin(async move {
            self.0
                .recv()
                .await
                .map(|conn| Box::new(conn) as Box<dyn PeerConnectionApi>)
        })
    }
}

/// This node as a gateway on a mock-socket transport, run through the real
/// handshake driver, and one remote peer on the same mock network.
struct DriverRig {
    driver: HandshakeHandler,
    commands: HandshakeCommandSender,
    channels: crate::transport::mock_transport::Channels,
    gw_key: TransportPublicKey,
    gw_addr: SocketAddr,
    peer_key: TransportPublicKey,
    peer_addr: SocketAddr,
    peer_outbound: crate::transport::connection_handler::OutboundConnectionHandler<
        crate::transport::mock_transport::MockSocket,
    >,
    /// For a peer run as a gateway: its accepted connections, kept open so
    /// its listener keeps running.
    _peer_accepted: Option<
        mpsc::Receiver<
            crate::transport::PeerConnection<crate::transport::mock_transport::MockSocket>,
        >,
    >,
}

impl DriverRig {
    async fn new(h: &Harness) -> Self {
        Self::with_clock(h, Arc::new(crate::util::time_source::InstantTimeSrc::new())).await
    }

    async fn with_clock(h: &Harness, clock: crate::util::time_source::DynTimeSource) -> Self {
        Self::build(h, clock, false).await
    }

    /// The remote peer runs as a gateway, so a dial to it succeeds without
    /// it dialling back.
    async fn with_gateway_peer(h: &Harness) -> Self {
        Self::build(
            h,
            Arc::new(crate::util::time_source::InstantTimeSrc::new()),
            true,
        )
        .await
    }

    async fn build(
        h: &Harness,
        clock: crate::util::time_source::DynTimeSource,
        peer_is_gateway: bool,
    ) -> Self {
        use crate::transport::mock_transport::{
            PacketDropPolicy, create_mock_gateway, create_mock_peer,
        };
        let channels: crate::transport::mock_transport::Channels = Default::default();
        let (gw_key, (gw_outbound, gw_accepted), gw_addr) =
            create_mock_gateway(PacketDropPolicy::ReceiveAll, channels.clone())
                .await
                .expect("mock gateway");
        let (peer_key, peer_outbound, peer_addr, peer_accepted) = if peer_is_gateway {
            let (key, (outbound, accepted), addr) =
                create_mock_gateway(PacketDropPolicy::ReceiveAll, channels.clone())
                    .await
                    .expect("mock peer gateway");
            (key, outbound, addr, Some(accepted))
        } else {
            let (key, outbound, addr) =
                create_mock_peer(PacketDropPolicy::ReceiveAll, channels.clone())
                    .await
                    .expect("mock peer");
            (key, outbound, addr, None)
        };
        let (driver, commands) = HandshakeHandler::new(
            Box::new(MockAcceptor(gw_accepted)),
            Arc::new(gw_outbound),
            h.mgr.bridge.op_manager.ring.connection_manager.clone(),
            h.mgr.bridge.op_manager.ring.router.clone(),
            None,
            true,
            None,
            clock,
        );
        Self {
            driver,
            commands,
            channels,
            gw_key,
            gw_addr,
            peer_key,
            peer_addr,
            peer_outbound,
            _peer_accepted: peer_accepted,
        }
    }

    /// The next handshake event, not yet handled.
    async fn next_event(&mut self) -> HandshakeEvent {
        use futures::StreamExt;
        tokio::time::timeout(Duration::from_secs(20), self.driver.next())
            .await
            .expect("the driver reports an event")
            .expect("the driver is running")
    }

    /// The connect op expects the peer, through the event loop's handler,
    /// and the driver has processed the registration when this returns.
    async fn expect_peer(&self, h: &Harness) {
        P2pConnManager::handle_expect_peer_connection(
            self.peer_addr,
            self.peer_key.clone(),
            &h.state,
            &self.commands,
        );
        let (done, processed) = tokio::sync::oneshot::channel();
        assert!(self.commands.try_send(HandshakeCommand::Barrier(done)));
        tokio::time::timeout(Duration::from_secs(5), processed)
            .await
            .expect("the driver processes commands")
            .expect("the driver is running");
    }

    /// The peer dials this node; returns the dial, which completes once the
    /// transport handshake does.
    fn peer_dials(&self) -> tokio::task::JoinHandle<bool> {
        let outbound = self.peer_outbound.clone();
        let (gw_key, gw_addr) = (self.gw_key.clone(), self.gw_addr);
        tokio::spawn(async move {
            let pending = OutboundConnector::connect(
                &outbound,
                gw_key,
                gw_addr,
                crate::transport::NatAttemptOrigin::ConnectJoiner,
            )
            .await;
            matches!(
                tokio::time::timeout(Duration::from_secs(10), pending).await,
                Ok(Ok(_))
            )
        })
    }

    /// The next handshake event, handled by the event loop's handler.
    async fn handle_next_event(&mut self, h: &mut Harness) -> &'static str {
        use futures::StreamExt;
        let event = tokio::time::timeout(Duration::from_secs(20), self.driver.next())
            .await
            .expect("the driver reports an event")
            .expect("the driver is running");
        let kind = match &event {
            HandshakeEvent::InboundConnection { .. } => "inbound",
            HandshakeEvent::OutboundEstablished { .. } => "outbound_established",
            HandshakeEvent::OutboundFailed { .. } => "outbound_failed",
        };
        h.use_event_addr(&event);
        h.mgr
            .handle_handshake_action(event, &mut h.state, &self.commands)
            .await
            .expect("handle_handshake_action");
        kind
    }

    /// The connect op's ConnectPeer for the peer, as the event loop handles
    /// it; returns the callback's receiver.
    async fn connect_peer(&self, h: &mut Harness) -> mpsc::Receiver<crate::message::ConnectResult> {
        h.use_addr(self.peer_addr);
        let (callback, result_rx) = mpsc::channel(1);
        h.mgr
            .handle_connect_peer(
                PeerKeyLocation::new(self.peer_key.clone(), self.peer_addr),
                Box::new(callback),
                Transaction::new::<ConnectMsg>(),
                &self.commands,
                &mut h.state,
                false,
                crate::transport::NatAttemptOrigin::ConnectAcceptor,
                crate::operations::connect::native::DialTransport::Udp,
            )
            .await
            .expect("handle_connect_peer");
        result_rx
    }
}

/// End to end through the real handshake driver and a mock-socket
/// transport, in the order where the peer's handshake reaches us before we
/// dial it: the transport accepts it as an inbound connection, the driver
/// matches it to the expectation, and it is recorded and promoted under the
/// key the connect op dialled. The ConnectPeer that follows reuses it and
/// promotes nothing again. This node's key is never stored or promoted.
///
/// The simulation rarely takes the matched path: its hole punches complete
/// on the dialling side, and the other way in, a reconnection accepted from
/// an address with a live session while an expectation for it is live, needs
/// a restart inside the expectation window (see
/// `restart_accept_with_a_live_expectation_binds_a_claim`). So this test and
/// that one cover it. The watched
/// `expected_inbound_bound` record is counted only when the driver reported
/// the connection as matched with a key, so it proves the matched path ran.
#[tokio::test]
async fn expected_inbound_matched_by_the_handshake_driver_gets_the_peer_key() {
    let mut h = Harness::new("conn-identity-expected-driver").await;
    let mut rig = DriverRig::new(&h).await;
    h.watch(&[rig.peer_addr]);

    rig.expect_peer(&h).await;
    let dial = rig.peer_dials();
    assert_eq!(rig.handle_next_event(&mut h).await, "inbound");
    assert!(dial.await.expect("dial task"), "the peer's dial completes");

    let peer_addr = rig.peer_addr;
    assert_eq!(
        seen(peer_addr),
        vec![ConnIdentity::ExpectedInboundBound],
        "the driver matched the connection to the expectation"
    );
    assert_eq!(h.mgr.connections[&peer_addr].pub_key(), Some(&rig.peer_key));
    assert_eq!(h.mgr.addr_by_pub_key.get(&rig.peer_key), Some(&peer_addr));
    assert_eq!(
        h.ring_key_at(peer_addr).as_ref(),
        Some(&rig.peer_key),
        "promoted under the dialled key"
    );
    assert_eq!(h_count(&h), 1);
    h.assert_own_key_absent();
    let age = aged_ring_entry(&h, peer_addr).await;

    let mut result = rig.connect_peer(&mut h).await;
    assert!(
        matches!(result.try_recv(), Ok(Ok(_))),
        "ConnectPeer reuses the inbound connection"
    );
    assert_eq!(h_count(&h), 1, "the ConnectPeer does not add it again");
    assert!(
        ring_age_ms(&h, peer_addr) >= age,
        "the ConnectPeer added the connection again (its age restarted)"
    );
    assert_eq!(
        seen(peer_addr),
        vec![
            ConnIdentity::ExpectedInboundBound,
            ConnIdentity::SameConnectPeer
        ]
    );
    assert_eq!(
        h.transport_seen(peer_addr).await,
        vec![
            ConnIdentity::TransportKeySent,
            ConnIdentity::TransportKeySet
        ],
        "the inbound transport learned the dialled key once, at the promotion \
         on arrival, and not again for the ConnectPeer"
    );
    h.assert_own_key_absent();
}

/// The order the connect op normally produces: the expectation, then the
/// ConnectPeer, whose dial is handled first. Here that dial fails (the peer
/// is unreachable until after the NAT deadline), and then the peer's own
/// handshake reaches us. The matched inbound connection must still be linked
/// in the ring under the key the connect op dialled; the ConnectPeer's
/// callback has already reported the failed dial, as before.
#[tokio::test]
async fn expected_inbound_after_a_failed_dial_is_promoted_under_the_dialled_key() {
    let mut h = Harness::new("conn-identity-expected-failed-dial").await;
    let mut rig = DriverRig::new(&h).await;
    h.watch(&[rig.peer_addr]);
    let peer_addr = rig.peer_addr;

    // Our dial cannot reach the peer: its socket is off the mock network.
    let (_, peer_socket) = rig
        .channels
        .remove(&peer_addr)
        .expect("the peer is on the mock network");

    rig.expect_peer(&h).await;
    let mut result = rig.connect_peer(&mut h).await;
    assert_eq!(rig.handle_next_event(&mut h).await, "outbound_failed");
    assert!(
        matches!(result.try_recv(), Ok(Err(None))),
        "the ConnectPeer callback reports the failed dial"
    );
    assert!(!h.mgr.connections.contains_key(&peer_addr));
    assert!(
        failed_addrs(&h).contains(&peer_addr),
        "premise: the failed dial is recorded for CONNECT routing"
    );

    // The peer comes back and dials us; the expectation still holds.
    rig.channels.insert(peer_addr, peer_socket);
    let dial = rig.peer_dials();
    assert_eq!(rig.handle_next_event(&mut h).await, "inbound");
    assert!(dial.await.expect("dial task"), "the peer's dial completes");

    assert_eq!(seen(peer_addr), vec![ConnIdentity::ExpectedInboundBound]);
    assert_eq!(h.mgr.connections[&peer_addr].pub_key(), Some(&rig.peer_key));
    assert_eq!(
        h.ring_key_at(peer_addr).as_ref(),
        Some(&rig.peer_key),
        "linked in the ring under the dialled key"
    );
    assert!(
        !failed_addrs(&h).contains(&peer_addr),
        "a connected peer is no longer avoided, as after a successful dial"
    );
    assert_eq!(h_count(&h), 1);
    assert_eq!(
        h.transport_seen(peer_addr).await,
        vec![
            ConnIdentity::TransportKeySent,
            ConnIdentity::TransportKeySet
        ],
        "the inbound transport learned the dialled key at promotion"
    );
    h.assert_own_key_absent();
}

fn failed_addrs(h: &Harness) -> Vec<SocketAddr> {
    h.mgr
        .bridge
        .op_manager
        .ring
        .connection_manager
        .recently_failed_addrs()
}

/// A handshake from the expected address that arrives after
/// `EXPECTED_INBOUND_WINDOW` is not the connection the connect op asked for
/// (the address may since be someone else's): it is matched, counted as
/// `expected_inbound_unbound`, starts without a key and is not promoted.
#[tokio::test]
async fn expected_inbound_after_the_window_is_not_bound_to_the_key() {
    let mut h = Harness::new("conn-identity-expected-window").await;
    let clock = crate::util::time_source::SharedMockTimeSource::new();
    let mut rig = DriverRig::with_clock(&h, Arc::new(clock.clone())).await;
    h.watch(&[rig.peer_addr]);
    let peer_addr = rig.peer_addr;

    rig.expect_peer(&h).await;
    clock.advance_time(
        crate::node::network_bridge::handshake::EXPECTED_INBOUND_WINDOW + Duration::from_secs(1),
    );
    let dial = rig.peer_dials();
    assert_eq!(rig.handle_next_event(&mut h).await, "inbound");
    assert!(dial.await.expect("dial task"), "the peer's dial completes");

    assert_eq!(seen(peer_addr), vec![ConnIdentity::ExpectedInboundUnbound]);
    assert_eq!(h.mgr.connections[&peer_addr].pub_key(), None);
    assert!(!h.mgr.addr_by_pub_key.contains_key(&rig.peer_key));
    assert_eq!(h.ring_key_at(peer_addr), None, "not promoted");
    h.assert_own_key_absent();

    // The unmatched connection is then named by the remote with a key other
    // than the one the connect op dialled: the remote's key is bound, and
    // the connect op's ConnectPeer for the dialled key is refused.
    let named = key();
    h.connect_request(peer_addr, PeerKeyLocation::with_unknown_addr(named.clone()))
        .await;
    assert!(
        !h.try_connect_peer(PeerKeyLocation::new(rig.peer_key.clone(), peer_addr))
            .await,
        "the ConnectPeer for the dialled key is refused"
    );
    assert_eq!(
        seen(peer_addr),
        vec![
            ConnIdentity::ExpectedInboundUnbound,
            ConnIdentity::SetConnectRequest,
            ConnIdentity::ChangedConnectPeer,
        ]
    );
    assert_eq!(h.mgr.connections[&peer_addr].pub_key(), Some(&named));
    assert!(!h.mgr.addr_by_pub_key.contains_key(&rig.peer_key));
    assert_eq!(h.ring_key_at(peer_addr), None, "not promoted");
}

/// End to end through the real driver: the driver matches an inbound
/// connection to an expectation for a key that a dialled connection at
/// another address still holds. The claim is held back, and bound and
/// promoted when the dialled connection closes.
#[tokio::test]
async fn driver_matched_claim_held_by_a_dial_is_bound_when_the_dial_closes() {
    let mut h = Harness::new("conn-identity-driver-held").await;
    let mut rig = DriverRig::new(&h).await;
    let dialled_at: SocketAddr = "203.0.113.192:5813".parse().unwrap();
    let peer_addr = rig.peer_addr;
    h.watch(&[peer_addr]);

    h.dial(dialled_at, &rig.peer_key).await;
    rig.expect_peer(&h).await;
    let dial = rig.peer_dials();
    assert_eq!(rig.handle_next_event(&mut h).await, "inbound");
    assert!(dial.await.expect("dial task"), "the peer's dial completes");

    assert_eq!(seen(peer_addr), vec![ConnIdentity::ClaimHeld]);
    assert_eq!(h.mgr.connections[&peer_addr].pub_key(), None);
    assert_eq!(h.ring_key_at(peer_addr), None);
    assert_eq!(h.mgr.addr_by_pub_key.get(&rig.peer_key), Some(&dialled_at));

    h.transport_closed(dialled_at).await;
    assert_eq!(
        seen(peer_addr),
        vec![ConnIdentity::ClaimHeld, ConnIdentity::PendingClaimBound]
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&rig.peer_key), Some(&peer_addr));
    assert_eq!(h.ring_key_at(peer_addr).as_ref(), Some(&rig.peer_key));
    h.assert_own_key_absent();
}

/// End to end: once our dial to the expected peer succeeds, its expectation
/// is gone, so a later inbound connection from the same address (here the
/// peer reconnecting) reaches the driver as unexpected and is not bound to
/// the dialled key.
#[tokio::test]
async fn inbound_after_a_successful_dial_is_unexpected() {
    let mut h = Harness::new("conn-identity-dial-then-inbound").await;
    let mut rig = DriverRig::with_gateway_peer(&h).await;
    let peer_addr = rig.peer_addr;

    h.watch(&[peer_addr]);

    rig.expect_peer(&h).await;
    let mut result = rig.connect_peer(&mut h).await;
    assert_eq!(rig.handle_next_event(&mut h).await, "outbound_established");
    assert!(matches!(result.try_recv(), Ok(Ok(_))), "the dial succeeded");
    assert_eq!(
        h.transport_seen(peer_addr).await,
        vec![
            ConnIdentity::TransportKeySent,
            ConnIdentity::TransportKeySame
        ],
        "the dialled transport already held the key it is promoted under"
    );
    // The drop the dial sent reaches the driver before what follows.
    let (done, processed) = tokio::sync::oneshot::channel();
    assert!(rig.commands.try_send(HandshakeCommand::Barrier(done)));
    processed.await.expect("the driver is running");

    let dial = rig.peer_dials();
    let event = rig.next_event().await;
    let HandshakeEvent::InboundConnection { matched, .. } = &event else {
        panic!("expected the peer's inbound connection, got {event:?}");
    };
    assert_eq!(
        *matched,
        HandshakeInboundMatch::Unexpected,
        "the expectation for {peer_addr} was dropped"
    );
    drop(event);
    assert!(dial.await.expect("dial task"), "the peer's dial completes");
}

/// A successful dial to an address drops that address's expectation, so a
/// later inbound connection from it is not bound to the dialled key.
#[tokio::test]
async fn successful_dial_drops_the_expectation_for_its_address() {
    let mut h = Harness::new("conn-identity-dial-drops-expectation").await;
    let addr: SocketAddr = "203.0.113.180:5801".parse().unwrap();
    while h.handshake_rx.try_recv().is_ok() {}
    h.handshake_event(HandshakeEvent::OutboundEstablished {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: PeerKeyLocation::new(key(), addr),
        connection: Box::new(IdleConnection::new(addr)),
        transient: false,
        transport: crate::operations::connect::native::DialTransport::Udp,
        origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    let mut dropped = false;
    while let Ok(command) = h.handshake_rx.try_recv() {
        if matches!(command, HandshakeCommand::DropConnection { addr: a } if a == addr) {
            dropped = true;
        }
    }
    assert!(
        dropped,
        "the driver is told to drop the expectation for {addr}"
    );
}

/// A contract hosting entry for `peer`, as a neighbour's announcement
/// records it, so teardown's per-peer cleanup is observable.
fn seed_neighbour_hosting(h: &Harness, peer: &TransportPublicKey) -> ContractInstanceId {
    let id = ContractInstanceId::new([7u8; 32]);
    h.mgr.bridge.op_manager.neighbor_hosting.handle_message(
        peer,
        crate::message::NeighborHostingMessage::HostingAnnounce {
            added: vec![id],
            removed: vec![],
            is_response: true,
        },
    );
    assert!(
        neighbour_hosts(h, peer, &id),
        "premise: the entry is recorded"
    );
    id
}

fn neighbour_hosts(h: &Harness, peer: &TransportPublicKey, id: &ContractInstanceId) -> bool {
    h.mgr
        .bridge
        .op_manager
        .neighbor_hosting
        .contracts_for_peer(peer)
        .contains(id)
}

/// The teardown paths of a connection, so one test can drive each.
#[derive(Debug, Clone, Copy)]
enum Teardown {
    TransportClosed,
    ExplicitDrop,
    UnpromotedSweep,
    Replacement,
}

impl Teardown {
    const ALL: [Self; 4] = [
        Self::TransportClosed,
        Self::ExplicitDrop,
        Self::UnpromotedSweep,
        Self::Replacement,
    ];

    async fn run(self, h: &mut Harness, addr: SocketAddr) {
        let handshake = h.handshake.clone();
        match self {
            Self::TransportClosed => h.transport_closed(addr).await,
            Self::ExplicitDrop => {
                assert!(
                    h.mgr
                        .drop_connection_by_addr(
                            addr,
                            &handshake,
                            crate::tracing::DisconnectReason::Pruned,
                        )
                        .await
                );
            }
            Self::UnpromotedSweep => h.mgr.drop_zombie_connection(addr, &handshake).await,
            Self::Replacement => {
                // A transient connection's replacement only releases its
                // slot; the per-peer cleanup runs for one that is not (an
                // expired transient, or a ring connection). A dial replaces
                // it: an inbound replacement registers a transient slot for
                // the address first.
                h.mgr
                    .bridge
                    .op_manager
                    .ring
                    .connection_manager
                    .drop_transient(addr);
                h.dial(addr, &key()).await;
            }
        }
    }
}

/// Teardown of a connection with no key runs no per-peer cleanup, so state
/// held under this node's own key survives it on every path; teardown of a
/// keyed connection still clears that peer's state.
#[tokio::test]
async fn teardown_of_an_unkeyed_connection_runs_no_per_peer_cleanup() {
    let mut h = Harness::new("conn-identity-teardown-own-key").await;
    let own = h.own_key();
    for (i, path) in Teardown::ALL.into_iter().enumerate() {
        let unkeyed: SocketAddr = format!("203.0.113.150:{}", 5500 + i).parse().unwrap();
        let id = seed_neighbour_hosting(&h, &own);
        h.accept(unkeyed).await;
        path.run(&mut h, unkeyed).await;
        assert!(
            neighbour_hosts(&h, &own, &id),
            "{path:?} of an unkeyed connection cleared state held under this node's key"
        );

        let keyed: SocketAddr = format!("203.0.113.151:{}", 5500 + i).parse().unwrap();
        let peer = key();
        h.dial(keyed, &peer).await;
        let id = seed_neighbour_hosting(&h, &peer);
        path.run(&mut h, keyed).await;
        assert!(
            !neighbour_hosts(&h, &peer, &id),
            "{path:?} of a keyed connection must clear that peer's state"
        );
    }
}

/// The source a connection's identity was bound from, if it has one.
fn bound_source(h: &Harness, addr: SocketAddr) -> Option<IdentitySource> {
    h.mgr.connections[&addr].identity.source()
}

/// A connection the node dialled keeps the dialled key: the remote's
/// ConnectRequest naming another key is dropped and the connection closed,
/// and nothing is recorded or promoted under the named key.
#[tokio::test]
async fn dialled_connection_is_closed_when_its_peer_names_another_key() {
    let mut h = Harness::new("conn-identity-dialled-renamed").await;
    let remote: SocketAddr = "198.51.100.150:5501".parse().unwrap();
    h.watch(&[remote]);
    let (dialled, named) = (key(), key());

    h.dial(remote, &dialled).await;
    assert_eq!(bound_source(&h, remote), Some(IdentitySource::Dialled));
    assert_eq!(h.ring_key_at(remote).as_ref(), Some(&dialled));
    h.refused_connect_request(remote, PeerKeyLocation::with_unknown_addr(named.clone()))
        .await;

    assert_eq!(
        seen(remote),
        vec![ConnIdentity::SetDialed, ConnIdentity::ChangedConnectRequest]
    );
    assert!(!h.mgr.connections.contains_key(&remote), "closed");
    assert!(!h.mgr.addr_by_pub_key.contains_key(&named));
    assert!(!h.mgr.addr_by_pub_key.contains_key(&dialled));
    assert_eq!(h.ring_key_at(remote), None, "pruned from the ring");
    assert!(
        h.mgr
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_peer_by_pub_key(&named)
            .is_none()
    );
}

/// A matched expected-inbound binding is a claim: the address matched, and
/// the key is the one the connect op dialled, not one the remote named. When
/// the remote then names another key in its own ConnectRequest (another
/// peer reached us from the address inside the expectation window), the
/// connection is closed, and nothing stays recorded or promoted under either
/// key.
#[tokio::test]
async fn matched_claim_is_closed_when_the_peer_names_another_key() {
    let mut h = Harness::new("conn-identity-matched-renamed").await;
    let remote: SocketAddr = "198.51.100.202:7201".parse().unwrap();
    h.watch(&[remote]);
    let (expected, named) = (key(), key());

    h.accept_expected(remote, &expected).await;
    assert_eq!(bound_source(&h, remote), Some(IdentitySource::Claimed));
    assert_eq!(h.ring_key_at(remote).as_ref(), Some(&expected));
    h.refused_connect_request(remote, PeerKeyLocation::with_unknown_addr(named.clone()))
        .await;

    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::ExpectedInboundBound,
            ConnIdentity::ChangedConnectRequest
        ]
    );
    assert!(!h.mgr.connections.contains_key(&remote), "closed");
    assert!(!h.mgr.addr_by_pub_key.contains_key(&expected));
    assert!(!h.mgr.addr_by_pub_key.contains_key(&named));
    assert_eq!(h.ring_key_at(remote), None);
    h.assert_own_key_absent();
}

/// The same two claims in the other order, on an unmatched connection: the
/// remote names itself first, then the connect op's ConnectPeer names the key
/// it dialled for the address. The connection keeps the remote's key, the
/// ConnectPeer is refused, and nothing is promoted.
#[tokio::test]
async fn self_named_connection_refuses_a_connect_peer_for_another_key() {
    let mut h = Harness::new("conn-identity-named-then-connect-peer").await;
    let remote: SocketAddr = "198.51.100.170:5701".parse().unwrap();
    h.watch(&[remote]);
    let (named, dialled) = (key(), key());

    h.accept(remote).await;
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(named.clone()))
        .await;
    assert!(
        !h.try_connect_peer(PeerKeyLocation::new(dialled.clone(), remote))
            .await
    );

    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::ChangedConnectPeer
        ]
    );
    assert_eq!(h.mgr.connections[&remote].pub_key(), Some(&named));
    assert_eq!(h.ring_key_at(remote), None);
    assert!(!h.mgr.addr_by_pub_key.contains_key(&dialled));
}

/// A matched expected-inbound claim for a key that a dialled connection at
/// another address still holds is not bound: the new connection continues
/// unidentified and unpromoted, with the claim pending, and the dialled
/// connection keeps the key's address. When the dialled connection closes
/// (a peer that restarted at a new address, whose old session times out),
/// the pending claim is bound and the connection promoted under the key,
/// and the peer's state is not lost in between.
#[tokio::test]
async fn matched_claim_held_by_a_dial_is_bound_when_the_dial_closes() {
    let mut h = Harness::new("conn-identity-matched-held").await;
    let dialled_at: SocketAddr = "198.51.100.203:7202".parse().unwrap();
    let claimed_at: SocketAddr = "203.0.113.181:5802".parse().unwrap();
    h.watch(&[dialled_at, claimed_at]);
    let peer = key();

    h.dial(dialled_at, &peer).await;
    h.accept_expected(claimed_at, &peer).await;

    assert_eq!(seen(claimed_at), vec![ConnIdentity::ClaimHeld]);
    assert_eq!(h.mgr.connections[&claimed_at].pub_key(), None);
    assert!(h.mgr.connections[&claimed_at].identity.pending().is_some());
    assert_eq!(h.ring_key_at(claimed_at), None, "not promoted");
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dialled_at));
    assert_eq!(h.ring_key_at(dialled_at).as_ref(), Some(&peer));

    let hosted = seed_neighbour_hosting(&h, &peer);
    h.transport_closed(dialled_at).await;
    assert!(
        neighbour_hosts(&h, &peer, &hosted),
        "the peer stays in the ring through the bound claim, so its state is kept"
    );
    assert_eq!(
        seen(claimed_at),
        vec![ConnIdentity::ClaimHeld, ConnIdentity::PendingClaimBound]
    );
    assert_eq!(h.mgr.connections[&claimed_at].pub_key(), Some(&peer));
    assert_eq!(bound_source(&h, claimed_at), Some(IdentitySource::Claimed));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&claimed_at));
    assert_eq!(
        h.ring_key_at(claimed_at).as_ref(),
        Some(&peer),
        "promoted, as the match would have been"
    );
    h.assert_own_key_absent();
}

/// A ConnectRequest claim held back by a dial outside the ring is bound when
/// the dial closes, so the restarted peer's later requests are passed on; it
/// is not promoted, since a request does not promote and the dial had no
/// ring place to take.
#[tokio::test]
async fn connect_request_claim_held_by_a_dial_is_bound_when_the_dial_closes() {
    let mut h = Harness::new("conn-identity-request-held-released").await;
    let dialled_at: SocketAddr = "203.0.113.183:5804".parse().unwrap();
    let claimed_at: SocketAddr = "203.0.113.184:5805".parse().unwrap();
    h.watch(&[claimed_at]);
    let peer = key();

    h.dial(dialled_at, &peer).await;
    h.mgr
        .bridge
        .op_manager
        .ring
        .connection_manager
        .prune_alive_connection(dialled_at);
    h.accept(claimed_at).await;
    h.refused_connect_request(claimed_at, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.transport_closed(dialled_at).await;

    assert_eq!(h.mgr.connections[&claimed_at].pub_key(), Some(&peer));
    assert_eq!(
        h.ring_key_at(claimed_at),
        None,
        "a request does not promote"
    );
    h.connect_request(claimed_at, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(
        seen(claimed_at),
        vec![
            ConnIdentity::ClaimHeld,
            ConnIdentity::PendingClaimBound,
            ConnIdentity::SameConnectRequest,
        ]
    );
}

/// Two dials of one key at two addresses: the second waits while the first
/// is heard from, and is bound and promoted when the first closes.
#[tokio::test]
async fn a_second_dial_of_a_key_waits_for_the_first() {
    let mut h = Harness::new("conn-identity-dial-dial").await;
    let first: SocketAddr = "203.0.113.190:5811".parse().unwrap();
    let second: SocketAddr = "203.0.113.191:5812".parse().unwrap();
    h.watch(&[first, second]);
    let peer = key();

    h.dial(first, &peer).await;
    h.dial(second, &peer).await;
    assert_eq!(seen(first), vec![ConnIdentity::SetDialed]);
    assert_eq!(seen(second), vec![ConnIdentity::ClaimHeld]);
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&first));
    let hosted = seed_neighbour_hosting(&h, &peer);

    h.transport_closed(first).await;
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&second));
    assert_eq!(h.ring_addr_of(&peer), Some(second));
    assert!(
        neighbour_hosts(&h, &peer, &hosted),
        "the peer stays in the ring"
    );
}

/// A retry pass does at most `MAX_PENDING_CLAIM_WORK_PER_PASS` replacements,
/// reports that work is left, and keeps the pending-claim flag set; the next
/// pass finishes the rest.
#[tokio::test]
async fn a_retry_pass_does_bounded_work() {
    let mut h = Harness::new("conn-identity-retry-bounded").await;
    let total = MAX_PENDING_CLAIM_WORK_PER_PASS + 3;
    let mut pairs = Vec::new();
    let mut clocks = Vec::new();
    for i in 0..total {
        let holder: SocketAddr = format!("198.51.100.28:{}", 7000 + 2 * i).parse().unwrap();
        let claimant: SocketAddr = format!("198.51.100.28:{}", 7001 + 2 * i).parse().unwrap();
        let peer = key();
        clocks.push(h.dial_with_receipt(holder, &peer).await);
        h.accept_expected(claimant, &peer).await;
        pairs.push((holder, claimant, peer));
    }
    for clock in &clocks {
        clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
    }
    let handshake = h.handshake.clone();
    let replaced = |h: &Harness| {
        pairs
            .iter()
            .filter(|(holder, _, _)| !h.mgr.connections.contains_key(holder))
            .count()
    };

    assert!(h.mgr.retry_pending_claims(&handshake).await, "work is left");
    assert_eq!(replaced(&h), MAX_PENDING_CLAIM_WORK_PER_PASS);
    assert!(h.mgr.addr_by_pub_key.may_have_pending_claims());

    assert!(!h.mgr.retry_pending_claims(&handshake).await);
    assert_eq!(replaced(&h), total);
    for (_, claimant, peer) in &pairs {
        assert_eq!(h.ring_addr_of(peer), Some(*claimant));
    }
    assert!(!h.mgr.addr_by_pub_key.may_have_pending_claims());
}

/// A ConnectRequest naming a key that a dialled connection at another address
/// holds is dropped and leaves the requesting connection unidentified; the
/// dialled connection keeps the key's address.
#[tokio::test]
async fn connect_request_claim_of_a_key_held_by_a_dial_is_dropped() {
    let mut h = Harness::new("conn-identity-request-held").await;
    let dialled_at: SocketAddr = "203.0.113.190:5901".parse().unwrap();
    let claimed_at: SocketAddr = "203.0.113.191:5902".parse().unwrap();
    h.watch(&[claimed_at]);
    let peer = key();

    h.dial(dialled_at, &peer).await;
    h.accept(claimed_at).await;
    h.refused_connect_request(claimed_at, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;

    assert_eq!(seen(claimed_at), vec![ConnIdentity::ClaimHeld]);
    assert!(h.mgr.connections.contains_key(&claimed_at), "left open");
    assert_eq!(h.mgr.connections[&claimed_at].pub_key(), None);
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dialled_at));
}

/// One connection per key applies to dials too. An outbound dial for a key
/// that a live claimed connection holds in the ring waits: it is not bound
/// or promoted, so the ring keeps the key at one address. When the holder
/// closes, the dial is bound (as a key bound after waiting) and promoted.
#[tokio::test]
async fn a_dial_waits_for_a_live_holder_of_its_key() {
    let mut h = Harness::new("conn-identity-dial-after-claim").await;
    let claimed_at: SocketAddr = "203.0.113.200:6001".parse().unwrap();
    let dialled_at: SocketAddr = "203.0.113.201:6002".parse().unwrap();
    h.watch(&[claimed_at, dialled_at]);
    let peer = key();

    h.accept_expected(claimed_at, &peer).await;
    let ring_before = h_count(&h);
    h.dial(dialled_at, &peer).await;

    assert_eq!(seen(dialled_at), vec![ConnIdentity::ClaimHeld]);
    assert_eq!(h.mgr.connections[&dialled_at].pub_key(), None);
    assert_eq!(h.ring_key_at(dialled_at), None);
    assert_eq!(
        h_count(&h),
        ring_before,
        "the key stays in the ring at one address"
    );
    assert_eq!(h.ring_addr_of(&peer), Some(claimed_at));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&claimed_at));

    h.transport_closed(claimed_at).await;
    assert_eq!(h.mgr.connections[&dialled_at].pub_key(), Some(&peer));
    assert_eq!(bound_source(&h, dialled_at), Some(IdentitySource::Claimed));
    assert_eq!(h.ring_addr_of(&peer), Some(dialled_at));
}

/// This node as a NON-gateway on a mock-socket transport with the real
/// handshake driver, and a remote gateway at a fixed address that this node
/// dials. The remote can then be stopped and a new node started at its
/// address, with the same key or another one, which reconnects to this
/// node. A non-gateway accepts that reconnection only on its restart path
/// (an intro from an address with an established session).
struct RestartRig {
    driver: HandshakeHandler,
    commands: HandshakeCommandSender,
    channels: crate::transport::mock_transport::Channels,
    node_key: TransportPublicKey,
    node_addr: SocketAddr,
    remote_addr: SocketAddr,
    remote: Option<(
        crate::transport::connection_handler::OutboundConnectionHandler<
            crate::transport::mock_transport::MockSocket,
        >,
        mpsc::Receiver<
            crate::transport::PeerConnection<crate::transport::mock_transport::MockSocket>,
        >,
    )>,
}

impl RestartRig {
    /// `ports` are this node's and the remote's, unique per test: identity
    /// telemetry is watched per address, process-wide.
    async fn new(h: &Harness, ports: (u16, u16), remote_keypair: TransportKeypair) -> Self {
        use crate::transport::mock_transport::create_mock_node_at_addr;
        let channels: crate::transport::mock_transport::Channels = Default::default();
        let node_addr: SocketAddr = (Ipv4Addr::LOCALHOST, ports.0).into();
        let remote_addr: SocketAddr = (Ipv4Addr::LOCALHOST, ports.1).into();
        let node_keypair = TransportKeypair::new();
        let node_key = node_keypair.public().clone();
        let (node_outbound, node_accepted) =
            create_mock_node_at_addr(false, node_addr, node_keypair, channels.clone())
                .await
                .expect("mock node");
        let remote = create_mock_node_at_addr(true, remote_addr, remote_keypair, channels.clone())
            .await
            .expect("mock remote");
        let (driver, commands) = HandshakeHandler::new(
            Box::new(MockAcceptor(node_accepted)),
            Arc::new(node_outbound),
            h.mgr.bridge.op_manager.ring.connection_manager.clone(),
            h.mgr.bridge.op_manager.ring.router.clone(),
            None,
            false,
            None,
            Arc::new(crate::util::time_source::InstantTimeSrc::new()),
        );
        Self {
            driver,
            commands,
            channels,
            node_key,
            node_addr,
            remote_addr,
            remote: Some(remote),
        }
    }

    /// The next handshake event, handled by the event loop's handler.
    async fn handle_next_event(&mut self, h: &mut Harness) -> &'static str {
        use futures::StreamExt;
        let event = tokio::time::timeout(Duration::from_secs(20), self.driver.next())
            .await
            .expect("the driver reports an event")
            .expect("the driver is running");
        let kind = match &event {
            HandshakeEvent::InboundConnection {
                matched: HandshakeInboundMatch::Expected(Some(_)),
                ..
            } => "inbound_matched",
            HandshakeEvent::InboundConnection { .. } => "inbound",
            HandshakeEvent::OutboundEstablished { .. } => "outbound_established",
            HandshakeEvent::OutboundFailed { .. } => "outbound_failed",
        };
        h.use_event_addr(&event);
        h.mgr
            .handle_handshake_action(event, &mut h.state, &self.commands)
            .await
            .expect("handle_handshake_action");
        kind
    }

    /// The connect op dials the remote under `key`, and the dial completes.
    async fn dial_remote(&mut self, h: &mut Harness, key: &TransportPublicKey) {
        let (callback, mut result_rx) = mpsc::channel(1);
        h.mgr
            .handle_connect_peer(
                PeerKeyLocation::new(key.clone(), self.remote_addr),
                Box::new(callback),
                Transaction::new::<ConnectMsg>(),
                &self.commands,
                &mut h.state,
                false,
                crate::transport::NatAttemptOrigin::ConnectJoiner,
                crate::operations::connect::native::DialTransport::Udp,
            )
            .await
            .expect("handle_connect_peer");
        assert_eq!(self.handle_next_event(h).await, "outbound_established");
        assert!(
            matches!(result_rx.try_recv(), Ok(Ok(_))),
            "the dial succeeded"
        );
    }

    /// A connect op expects the remote's address under `key`; the driver has
    /// processed the registration when this returns.
    async fn expect_remote(&self, h: &Harness, key: &TransportPublicKey) {
        P2pConnManager::handle_expect_peer_connection(
            self.remote_addr,
            key.clone(),
            &h.state,
            &self.commands,
        );
        let (done, processed) = tokio::sync::oneshot::channel();
        assert!(self.commands.try_send(HandshakeCommand::Barrier(done)));
        tokio::time::timeout(Duration::from_secs(5), processed)
            .await
            .expect("the driver processes commands")
            .expect("the driver is running");
    }

    /// Stops the remote and starts a new node at its address with `keypair`,
    /// which dials this node. Returns the dial, which yields whether it
    /// completed and the restarted node, kept so its transport stays up
    /// while the test inspects. The old remote's socket only removes its own
    /// registration when it is dropped (see `MockSocket::registered`), so the
    /// new node can start at the address at once.
    async fn restart_remote(
        &mut self,
        keypair: TransportKeypair,
    ) -> tokio::task::JoinHandle<RestartedRemote> {
        use crate::transport::mock_transport::create_mock_node_at_addr;
        drop(self.remote.take());
        let (outbound, accepted) =
            create_mock_node_at_addr(false, self.remote_addr, keypair, self.channels.clone())
                .await
                .expect("restarted remote");
        let (node_key, node_addr) = (self.node_key.clone(), self.node_addr);
        tokio::spawn(async move {
            let pending = OutboundConnector::connect(
                &outbound,
                node_key,
                node_addr,
                crate::transport::NatAttemptOrigin::ConnectJoiner,
            )
            .await;
            let connection = tokio::time::timeout(Duration::from_secs(10), pending)
                .await
                .ok()
                .and_then(Result::ok);
            RestartedRemote {
                connected: connection.is_some(),
                _connection: connection,
                _node: (outbound, accepted),
            }
        })
    }
}

/// A restarted remote from [`RestartRig::restart_remote`].
struct RestartedRemote {
    connected: bool,
    _connection: Option<Box<dyn PeerConnectionApi>>,
    _node: (
        crate::transport::connection_handler::OutboundConnectionHandler<
            crate::transport::mock_transport::MockSocket,
        >,
        mpsc::Receiver<
            crate::transport::PeerConnection<crate::transport::mock_transport::MockSocket>,
        >,
    ),
}

/// A non-gateway accepts a reconnection from an address it has an
/// established session with (the remote restarted there), and the handshake
/// driver matches it to an expectation for that address that is still live.
/// The connection is bound to the expected key as a claim, replacing the
/// dialled connection at the address, and `expected_inbound_bound` is
/// counted on a node that is not a gateway.
#[tokio::test]
async fn restart_accept_with_a_live_expectation_binds_a_claim() {
    let mut h = Harness::new("conn-identity-restart-accept").await;
    let remote_keypair = TransportKeypair::new();
    let remote_key = remote_keypair.public().clone();
    let mut rig = RestartRig::new(&h, (49311, 49321), remote_keypair.clone()).await;
    let remote_addr = rig.remote_addr;
    h.watch(&[remote_addr]);

    rig.dial_remote(&mut h, &remote_key).await;
    assert_eq!(bound_source(&h, remote_addr), Some(IdentitySource::Dialled));
    rig.expect_remote(&h, &remote_key).await;

    let dial = rig.restart_remote(remote_keypair).await;
    assert_eq!(
        rig.handle_next_event(&mut h).await,
        "inbound_matched",
        "the reconnection is accepted and matched to the live expectation"
    );

    assert_eq!(
        seen(remote_addr),
        vec![ConnIdentity::SetDialed, ConnIdentity::ExpectedInboundBound]
    );
    assert_eq!(h.mgr.connections[&remote_addr].pub_key(), Some(&remote_key));
    assert_eq!(bound_source(&h, remote_addr), Some(IdentitySource::Claimed));
    assert_eq!(h.mgr.addr_by_pub_key.get(&remote_key), Some(&remote_addr));
    assert_eq!(h.ring_key_at(remote_addr).as_ref(), Some(&remote_key));
    h.assert_own_key_absent();
    let restarted = dial.await.expect("dial task");
    assert!(restarted.connected, "the restarted node's dial completes");
}

/// As above, but another peer, with another key, reconnects from the address
/// inside the expectation window. The match binds the expected key as a
/// claim; when the peer names its own key in a ConnectRequest, the connection
/// is closed and nothing stays recorded or promoted under either key.
#[tokio::test]
async fn restart_accept_by_another_peer_is_closed_when_it_names_itself() {
    let mut h = Harness::new("conn-identity-restart-other-peer").await;
    let first_keypair = TransportKeypair::new();
    let first_key = first_keypair.public().clone();
    let mut rig = RestartRig::new(&h, (49312, 49322), first_keypair).await;
    let remote_addr = rig.remote_addr;
    h.watch(&[remote_addr]);

    rig.dial_remote(&mut h, &first_key).await;
    rig.expect_remote(&h, &first_key).await;

    let other_keypair = TransportKeypair::new();
    let other_key = other_keypair.public().clone();
    let dial = rig.restart_remote(other_keypair).await;
    assert_eq!(rig.handle_next_event(&mut h).await, "inbound_matched");
    assert_eq!(h.mgr.connections[&remote_addr].pub_key(), Some(&first_key));
    assert_eq!(bound_source(&h, remote_addr), Some(IdentitySource::Claimed));

    h.refused_connect_request(
        remote_addr,
        PeerKeyLocation::with_unknown_addr(other_key.clone()),
    )
    .await;
    assert_eq!(
        seen(remote_addr),
        vec![
            ConnIdentity::SetDialed,
            ConnIdentity::ExpectedInboundBound,
            ConnIdentity::ChangedConnectRequest,
        ]
    );
    assert!(!h.mgr.connections.contains_key(&remote_addr), "closed");
    assert!(!h.mgr.addr_by_pub_key.contains_key(&first_key));
    assert!(!h.mgr.addr_by_pub_key.contains_key(&other_key));
    assert_eq!(h.ring_key_at(remote_addr), None);
    let restarted = dial.await.expect("dial task");
    assert!(restarted.connected, "the other peer's dial completed");
}

/// A clock for a dial's receipt stamp: the time now and the last receipt.
#[derive(Clone, Default)]
struct ReceiptClock {
    now: Arc<std::sync::atomic::AtomicU64>,
    last: Arc<std::sync::atomic::AtomicU64>,
}

impl ReceiptClock {
    fn stamp(&self) -> crate::transport::ReceiptStamp {
        let now = self.now.clone();
        crate::transport::ReceiptStamp::new(
            self.last.clone(),
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
            Arc::new(move || now.load(std::sync::atomic::Ordering::Relaxed)),
        )
    }

    /// Time passes with nothing received.
    fn go_quiet(&self, for_: Duration) {
        self.now
            .fetch_add(for_.as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// A packet arrives now.
    fn receive(&self) {
        self.last.store(
            self.now.load(std::sync::atomic::Ordering::Relaxed),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

impl Harness {
    /// A dial whose transport reports receipt on `clock`.
    async fn dial_with_receipt(
        &mut self,
        addr: SocketAddr,
        key: &TransportPublicKey,
    ) -> ReceiptClock {
        self.use_addr(addr);
        let clock = ReceiptClock::default();
        let connection = IdleConnection {
            addr,
            receipt: Some(clock.stamp()),
        };
        self.mgr
            .handle_successful_connection(
                Some(PeerKeyLocation::new(key.clone(), addr)),
                Box::new(connection),
                &mut self.state,
                None,
                false,
                &self.handshake,
                IdentitySource::Dialled,
            )
            .await
            .expect("handle_successful_connection");
        clock
    }

    /// An unexpected inbound connection whose transport reports receipt on
    /// the returned clock.
    async fn accept_with_receipt(&mut self, addr: SocketAddr) -> ReceiptClock {
        self.use_addr(addr);
        let clock = ReceiptClock::default();
        let connection = IdleConnection {
            addr,
            receipt: Some(clock.stamp()),
        };
        self.mgr
            .handle_successful_connection(
                None,
                Box::new(connection),
                &mut self.state,
                None,
                true,
                &self.handshake,
                IdentitySource::Claimed,
            )
            .await
            .expect("handle_successful_connection");
        clock
    }

    /// The address the ring routes `key` to, if any.
    fn ring_addr_of(&self, key: &TransportPublicKey) -> Option<SocketAddr> {
        self.mgr
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_peer_by_pub_key(key)
            .and_then(|peer| peer.socket_addr())
    }

    fn pending_on(&self, addr: SocketAddr) -> Option<bool> {
        self.mgr.connections[&addr]
            .identity
            .pending()
            .map(|claim| claim.promote)
    }
}

/// A peer that restarted at a new port while this node's dial to its old
/// address is still open: the old dial holds the key only while it is heard
/// from. Once it has been quiet for `HOLD_WINDOW`, the next retry closes it,
/// and its release binds and promotes the new connection, well before the
/// old session's idle timeout. The peer's state is kept, since the peer is
/// back in the ring through the new connection.
#[tokio::test]
async fn restart_at_a_new_port_replaces_the_old_dial_once_it_goes_quiet() {
    let mut h = Harness::new("conn-identity-restart-quiet").await;
    let old: SocketAddr = "198.51.100.1:6001".parse().unwrap();
    let new: SocketAddr = "198.51.100.1:6002".parse().unwrap();
    h.watch(&[old, new]);
    let peer = key();

    let clock = h.dial_with_receipt(old, &peer).await;
    let hosted = seed_neighbour_hosting(&h, &peer);
    h.accept_expected(new, &peer).await;
    assert_eq!(seen(new), vec![ConnIdentity::ClaimHeld]);
    assert_eq!(h.pending_on(new), Some(true));

    clock.go_quiet(identity::HOLD_WINDOW);
    let handshake = h.handshake.clone();
    h.mgr.retry_pending_claims(&handshake).await;
    assert_eq!(h.pending_on(new), Some(true), "still within the window");
    assert!(h.mgr.connections.contains_key(&old));

    let old_connection_id = h.mgr.connections[&old].connection_id;
    clock.go_quiet(Duration::from_secs(1));
    h.mgr.retry_pending_claims(&handshake).await;
    assert!(
        !h.mgr.connections.contains_key(&old),
        "the quiet dial is closed"
    );
    assert_eq!(
        seen(old),
        vec![ConnIdentity::SetDialed, ConnIdentity::QuietHolderReplaced]
    );
    assert_eq!(
        seen(new),
        vec![ConnIdentity::ClaimHeld, ConnIdentity::PendingClaimBound]
    );
    assert_eq!(h.mgr.connections[&new].pub_key(), Some(&peer));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&new));
    assert_eq!(
        h.ring_addr_of(&peer),
        Some(new),
        "the ring routes to the new connection only"
    );
    assert!(
        neighbour_hosts(&h, &peer, &hosted),
        "the peer's state is kept"
    );
    assert!(!h.mgr.addr_by_pub_key.may_have_pending_claims());

    // The old session's own close arrives later; it changes nothing.
    let handshake = h.handshake.clone();
    h.mgr
        .handle_transport_event(
            Some(ConnEvent::TransportClosed {
                remote_addr: old,
                error: TransportError::ConnectionClosed(old),
                connection_id: old_connection_id,
                reason: crate::tracing::DisconnectReason::Timeout,
            }),
            &mut h.state,
            &handshake,
        )
        .await
        .expect("handle_transport_event");
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&new));
    assert_eq!(h.ring_addr_of(&peer), Some(new));
    assert!(neighbour_hosts(&h, &peer, &hosted));
}

/// A dial that keeps receiving keeps holding its key, however long it has
/// been open.
#[tokio::test]
async fn a_dial_that_keeps_receiving_keeps_holding_its_key() {
    let mut h = Harness::new("conn-identity-dial-receiving").await;
    let old: SocketAddr = "198.51.100.2:6001".parse().unwrap();
    let new: SocketAddr = "198.51.100.2:6002".parse().unwrap();
    let peer = key();

    let clock = h.dial_with_receipt(old, &peer).await;
    h.accept_expected(new, &peer).await;
    let handshake = h.handshake.clone();
    for _ in 0..4 {
        clock.go_quiet(Duration::from_secs(20));
        clock.receive();
        h.mgr.retry_pending_claims(&handshake).await;
    }
    assert_eq!(h.pending_on(new), Some(true));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&old));
}

/// A restarted joiner's ConnectRequest is passed on, and binds its key, as
/// soon as this node's old dial to it has gone quiet: the quiet dial is
/// replaced on the spot, so the joiner's requests are not dropped for the
/// rest of the old session.
#[tokio::test]
async fn connect_request_replaces_a_quiet_dial() {
    let mut h = Harness::new("conn-identity-request-quiet").await;
    let old: SocketAddr = "198.51.100.3:6001".parse().unwrap();
    let new: SocketAddr = "198.51.100.3:6002".parse().unwrap();
    h.watch(&[new]);
    let peer = key();

    let clock = h.dial_with_receipt(old, &peer).await;
    h.accept(new).await;
    h.refused_connect_request(new, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
    h.connect_request(new, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(
        seen(new),
        vec![ConnIdentity::ClaimHeld, ConnIdentity::PendingClaimBound]
    );
    assert!(!h.mgr.connections.contains_key(&old));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&new));
}

/// A pending claim whose connection closes first leaves the dialled holder
/// as it was: its mapping, its ring entry and the peer's state.
#[tokio::test]
async fn closing_a_pending_claimant_leaves_the_holder_untouched() {
    let mut h = Harness::new("conn-identity-claimant-closes").await;
    let holder: SocketAddr = "198.51.100.4:6001".parse().unwrap();
    let claimant: SocketAddr = "198.51.100.4:6002".parse().unwrap();
    let peer = key();

    h.dial(holder, &peer).await;
    let hosted = seed_neighbour_hosting(&h, &peer);
    h.accept_expected(claimant, &peer).await;
    h.transport_closed(claimant).await;

    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&holder));
    assert_eq!(h.ring_addr_of(&peer), Some(holder));
    assert!(neighbour_hosts(&h, &peer, &hosted));
}

/// Two claims held back by one dial: when the dial closes, the newer
/// connection's claim is bound and promoted, and the older one waits; when
/// the newer connection closes in turn, the older claim is bound.
#[tokio::test]
async fn two_pending_claims_bind_newest_first() {
    let mut h = Harness::new("conn-identity-two-claims").await;
    let holder: SocketAddr = "198.51.100.5:6001".parse().unwrap();
    // The older claim has the higher port, so neither map order nor address
    // order picks the newer one.
    let older: SocketAddr = "198.51.100.5:6003".parse().unwrap();
    let newer: SocketAddr = "198.51.100.5:6002".parse().unwrap();
    let peer = key();

    h.dial(holder, &peer).await;
    h.accept_expected(older, &peer).await;
    h.accept_expected(newer, &peer).await;
    h.transport_closed(holder).await;
    assert_eq!(h.mgr.connections[&newer].pub_key(), Some(&peer));
    assert_eq!(h.ring_key_at(newer).as_ref(), Some(&peer));
    assert_eq!(h.pending_on(older), Some(true), "the older claim waits");

    h.transport_closed(newer).await;
    assert_eq!(h.mgr.connections[&older].pub_key(), Some(&peer));
    assert_eq!(h.ring_addr_of(&peer), Some(older));
}

/// A claimed connection outside the ring holds a key, and a dial and a
/// matched claim for the key both wait behind it. When the dial closes, the
/// holder is unchanged, so the other claim keeps waiting; when the holder
/// closes, the remaining claim is bound and promoted.
#[tokio::test]
async fn a_pending_claim_waits_for_every_holder_of_its_key() {
    let mut h = Harness::new("conn-identity-moved-pending").await;
    let claimed: SocketAddr = "198.51.100.6:6001".parse().unwrap();
    let dialled: SocketAddr = "198.51.100.6:6002".parse().unwrap();
    let pending: SocketAddr = "198.51.100.6:6003".parse().unwrap();
    let peer = key();

    h.accept(claimed).await;
    h.connect_request(claimed, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.dial(dialled, &peer).await;
    h.accept_expected(pending, &peer).await;
    assert_eq!(h.pending_on(pending), Some(true));
    assert!(h.pending_on(dialled).is_some(), "the dial waits");
    assert_eq!(h.mgr.connections[&dialled].pub_key(), None);

    h.transport_closed(dialled).await;
    assert_eq!(
        h.pending_on(pending),
        Some(true),
        "the claimed connection still holds it"
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&claimed));

    h.transport_closed(claimed).await;
    assert_eq!(h.mgr.connections[&pending].pub_key(), Some(&peer));
    assert_eq!(h.ring_key_at(pending).as_ref(), Some(&peer));
    assert_eq!(h.ring_addr_of(&peer), Some(pending));
}

/// A claim made pending by an expected-inbound match keeps `promote` when a
/// ConnectRequest on the same connection is also held back, so binding it
/// later promotes the connection.
#[tokio::test]
async fn a_pending_claim_keeps_promote_across_a_later_request() {
    let mut h = Harness::new("conn-identity-promote-merge").await;
    let holder: SocketAddr = "198.51.100.9:6001".parse().unwrap();
    let claimant: SocketAddr = "198.51.100.9:6002".parse().unwrap();
    let peer = key();

    h.dial(holder, &peer).await;
    h.accept_expected(claimant, &peer).await;
    h.refused_connect_request(claimant, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(h.pending_on(claimant), Some(true));
    h.transport_closed(holder).await;
    assert_eq!(h.ring_key_at(claimant).as_ref(), Some(&peer));
}

/// A pending claim bound while the ring is at its cap is bound but not
/// promoted, like any promotion at the cap. Here the dial holding the key
/// is outside the ring, so its release frees no ring slot.
#[tokio::test]
async fn a_pending_claim_bound_at_the_cap_is_not_promoted() {
    let mut h = Harness::with_config("conn-identity-pending-at-cap", |config| {
        config.min_number_conn = Some(1);
        config.max_number_conn = Some(2);
    })
    .await;
    let holder: SocketAddr = "198.51.100.10:6001".parse().unwrap();
    let claimant: SocketAddr = "198.51.100.10:6003".parse().unwrap();
    let peer = key();

    h.dial(holder, &peer).await;
    h.mgr
        .bridge
        .op_manager
        .ring
        .connection_manager
        .prune_alive_connection(holder);
    for port in [6004, 6005] {
        let addr: SocketAddr = format!("198.51.100.10:{port}").parse().unwrap();
        h.dial(addr, &key()).await;
    }
    assert_eq!(h_count(&h), 2, "premise: at the cap, without the holder");
    h.accept_expected(claimant, &peer).await;
    assert_eq!(h.pending_on(claimant), Some(true));
    h.transport_closed(holder).await;

    assert_eq!(h.mgr.connections[&claimant].pub_key(), Some(&peer));
    assert_eq!(h.ring_key_at(claimant), None, "refused at the cap");
    assert_eq!(h_count(&h), 2);
}

/// A dialled holder closing on every teardown path binds and promotes the
/// claim it held back.
#[tokio::test]
async fn every_teardown_path_of_the_holder_binds_the_pending_claim() {
    let mut h = Harness::new("conn-identity-pending-teardown-paths").await;
    for (i, path) in Teardown::ALL.into_iter().enumerate() {
        let port = 6100 + 2 * i as u16;
        let holder: SocketAddr = format!("198.51.100.11:{port}").parse().unwrap();
        let claimant: SocketAddr = format!("198.51.100.11:{}", port + 1).parse().unwrap();
        let peer = key();
        h.dial(holder, &peer).await;
        h.accept_expected(claimant, &peer).await;
        path.run(&mut h, holder).await;
        assert_eq!(
            h.mgr.connections[&claimant].pub_key(),
            Some(&peer),
            "{path:?} did not bind the pending claim"
        );
        assert_eq!(
            h.ring_key_at(claimant).as_ref(),
            Some(&peer),
            "{path:?} did not promote"
        );
    }
}

/// A ConnectPeer for an unidentified connection that the ring refuses at
/// its cap leaves the connection unidentified.
#[tokio::test]
async fn connect_peer_refused_at_the_cap_leaves_the_connection_unbound() {
    let mut h = Harness::with_config("conn-identity-connect-peer-at-cap", |config| {
        config.min_number_conn = Some(1);
        config.max_number_conn = Some(2);
    })
    .await;
    for port in [6201, 6202] {
        let addr: SocketAddr = format!("198.51.100.12:{port}").parse().unwrap();
        h.dial(addr, &key()).await;
    }
    let remote: SocketAddr = "198.51.100.12:6203".parse().unwrap();
    let peer = key();
    h.accept(remote).await;
    assert!(
        !h.try_connect_peer(PeerKeyLocation::new(peer.clone(), remote))
            .await
    );
    assert_eq!(h.mgr.connections[&remote].pub_key(), None);
    assert!(!h.mgr.addr_by_pub_key.contains_key(&peer));
}

/// The retry deadline is armed only while a claim may be pending: `None`
/// with no claim kept, set once one is, and `None` again after a retry finds
/// none left.
#[tokio::test]
async fn the_retry_deadline_is_set_only_while_a_claim_may_be_pending() {
    let mut h = Harness::new("conn-identity-retry-deadline").await;
    let holder: SocketAddr = "198.51.100.20:6001".parse().unwrap();
    let claimant: SocketAddr = "198.51.100.20:6002".parse().unwrap();
    let peer = key();
    let retry = PendingClaimRetry::new(Instant::now(), Duration::from_secs(5));
    let deadline = |h: &Harness| retry.deadline(h.mgr.addr_by_pub_key.may_have_pending_claims());

    assert_eq!(deadline(&h), None);
    let clock = h.dial_with_receipt(holder, &peer).await;
    h.accept_expected(claimant, &peer).await;
    assert!(deadline(&h).is_some());
    clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
    let handshake = h.handshake.clone();
    h.mgr.retry_pending_claims(&handshake).await;
    assert_eq!(h.mgr.connections[&claimant].pub_key(), Some(&peer));
    assert_eq!(deadline(&h), None);
}

/// The retry is due once per interval: not before it, at it, and then not
/// again until a full interval later, so a loop that checks on every pass
/// does not retry on every pass.
#[test]
fn the_retry_is_due_once_per_interval() {
    let start = Instant::now();
    let interval = Duration::from_secs(5);
    let mut retry = PendingClaimRetry::new(start, interval);
    assert_eq!(retry.deadline(true), Some(start + interval));
    assert_eq!(retry.deadline(false), None);
    assert!(!retry.take_due(start + Duration::from_secs(4)));
    assert!(retry.take_due(start + interval));
    assert!(
        !retry.take_due(start + interval + Duration::from_millis(1)),
        "due again right after a retry"
    );
    assert_eq!(retry.deadline(true), Some(start + interval * 2));
    assert!(retry.take_due(start + interval * 2));

    // A pass that stopped with work left makes the next one due at once,
    // and the one after that a full interval later.
    let now = start + interval * 2 + Duration::from_millis(1);
    retry.run_again_now(now);
    assert_eq!(retry.deadline(true), Some(now));
    assert!(retry.take_due(now));
    assert_eq!(retry.deadline(true), Some(now + interval));
}

/// `next_wake` returns at the earlier deadline when no event arrives, which
/// is how the retry deadline wakes a quiet event loop.
#[tokio::test(start_paused = true)]
async fn next_wake_returns_at_the_earlier_deadline_without_events() {
    let mut never = futures::stream::pending::<()>();
    let start = Instant::now();
    let retry = start + Duration::from_secs(5);
    let backlog = start + Duration::from_secs(60);
    let wake = zombie_sweep::next_wake(
        &mut never,
        zombie_sweep::earliest(Some(backlog), Some(retry)),
    )
    .await;
    assert!(matches!(wake, zombie_sweep::LoopWake::TimerDue));
    assert_eq!(Instant::now(), retry);
    assert_eq!(zombie_sweep::earliest(None, Some(retry)), Some(retry));
    assert_eq!(zombie_sweep::earliest(Some(backlog), None), Some(backlog));
    assert_eq!(zombie_sweep::earliest(None, None), None);
}

/// The event loop's timer wake-up includes the pending-claim retry deadline,
/// and the loop runs the retry when it is due. Scoped to the event loop's
/// `next_wake` call so the pin cannot be satisfied elsewhere.
#[test]
fn the_event_loop_wakes_for_the_pending_claim_retry() {
    let source = include_str!("../../p2p_protoc.rs");
    let at = source
        .find("wake = zombie_sweep::next_wake(")
        .expect("the event loop's next_wake call");
    let call = &source[at..at + source[at..].find("=> {").expect("end of the call")];
    assert!(
        call.contains(".deadline(ctx.addr_by_pub_key.may_have_pending_claims())"),
        "the loop's timer must include the pending-claim retry deadline: {call}"
    );
    assert!(
        source.contains("pending_claim_retry.take_due(Instant::now())"),
        "the loop must check the retry on every pass"
    );
    // The retry runs in the condition, and its result gates an immediate
    // rerun at statement position inside the branch.
    let call = "&& ctx.retry_pending_claims(&handshake_cmd_sender).await\n";
    let at = source.find(call).expect("the loop must run the retry");
    let branch = &source[at..];
    let branch = &branch[..branch.find("\n            }\n").expect("end of the branch")];
    assert!(
        branch.lines().any(|line| line
            .trim()
            .starts_with("pending_claim_retry.run_again_now(Instant::now());")),
        "a pass with work left must make the next one due at once: {branch}"
    );
}

/// Several claims of one key wait on a dial that goes quiet: the retry
/// replaces the dial and binds and promotes only the newest claim. The
/// others keep waiting while it is heard from.
#[tokio::test]
async fn the_retry_binds_the_newest_claim_and_the_rest_wait() {
    let mut h = Harness::new("conn-identity-retry-one-per-key").await;
    let holder: SocketAddr = "198.51.100.21:6001".parse().unwrap();
    let oldest: SocketAddr = "198.51.100.21:6004".parse().unwrap();
    let middle: SocketAddr = "198.51.100.21:6003".parse().unwrap();
    let newest: SocketAddr = "198.51.100.21:6002".parse().unwrap();
    let peer = key();

    let clock = h.dial_with_receipt(holder, &peer).await;
    for claimant in [oldest, middle, newest] {
        h.accept_expected(claimant, &peer).await;
    }
    clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
    let handshake = h.handshake.clone();
    h.mgr.retry_pending_claims(&handshake).await;
    assert_eq!(h.mgr.connections[&newest].pub_key(), Some(&peer));
    assert_eq!(h.ring_addr_of(&peer), Some(newest));

    h.mgr.retry_pending_claims(&handshake).await;
    assert_eq!(h.pending_on(middle), Some(true));
    assert_eq!(h.pending_on(oldest), Some(true));
    assert_eq!(h.mgr.connections[&middle].pub_key(), None);
    assert_eq!(h.ring_addr_of(&peer), Some(newest));
}

/// Two live sessions under one key (one node leaving through two addresses,
/// or two nodes sharing a key): the second session's claim waits while the
/// first is heard from. It is not bound or promoted, its request is dropped,
/// and the first connection is not closed, however often the retry runs.
#[tokio::test]
async fn two_live_sessions_under_one_key_keep_one_connection() {
    let mut h = Harness::new("conn-identity-two-live-sessions").await;
    let first: SocketAddr = "198.51.100.24:6001".parse().unwrap();
    let second: SocketAddr = "198.51.100.24:6002".parse().unwrap();
    h.watch(&[second]);
    let peer = key();

    let clock = h.accept_with_receipt(first).await;
    h.connect_request(first, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.connect_peer(PeerKeyLocation::new(peer.clone(), first))
        .await;
    h.accept(second).await;
    h.refused_connect_request(second, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    let handshake = h.handshake.clone();
    for _ in 0..4 {
        clock.go_quiet(Duration::from_secs(20));
        clock.receive();
        h.mgr.retry_pending_claims(&handshake).await;
        h.refused_connect_request(second, PeerKeyLocation::with_unknown_addr(peer.clone()))
            .await;
    }
    assert!(
        h.mgr.connections.contains_key(&first),
        "the first is not closed"
    );
    assert_eq!(h.mgr.connections[&second].pub_key(), None);
    assert_eq!(h.pending_on(second), Some(false));
    assert_eq!(h.ring_key_at(second), None);
    assert_eq!(h.ring_addr_of(&peer), Some(first));
    assert!(seen(second).iter().all(|k| *k == ConnIdentity::ClaimHeld));
}

/// A joiner that reconnects to a gateway from a new address while its old
/// connection is still in the ring: the new connection's request waits
/// while the old one is heard from; once the old one has been quiet for
/// `HOLD_WINDOW`, the next request closes it, and the new connection is bound
/// and takes the old one's place in the ring, with the peer's state kept.
#[tokio::test]
async fn a_reconnecting_joiner_replaces_its_quiet_connection_on_a_gateway() {
    let mut h = Harness::new("conn-identity-gateway-reconnect").await;
    let old: SocketAddr = "198.51.100.27:6001".parse().unwrap();
    let new: SocketAddr = "198.51.100.27:6002".parse().unwrap();
    h.watch(&[old, new]);
    let peer = key();

    let clock = h.accept_with_receipt(old).await;
    h.connect_request(old, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.connect_peer(PeerKeyLocation::new(peer.clone(), old))
        .await;
    let hosted = seed_neighbour_hosting(&h, &peer);

    h.accept(new).await;
    h.refused_connect_request(new, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(h.ring_addr_of(&peer), Some(old));

    clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
    h.connect_request(new, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert!(
        !h.mgr.connections.contains_key(&old),
        "the quiet connection is closed"
    );
    assert_eq!(
        seen(old),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::SameConnectPeer,
            ConnIdentity::QuietHolderReplaced
        ]
    );
    assert_eq!(h.mgr.connections[&new].pub_key(), Some(&peer));
    assert_eq!(h.ring_addr_of(&peer), Some(new), "promoted in its place");
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&new));
    assert!(
        neighbour_hosts(&h, &peer, &hosted),
        "the peer's state is kept"
    );
}

/// An expected-inbound claim whose key a live claimed connection in the
/// ring holds waits: it is not bound or promoted, the holder is untouched,
/// and the reservation the connect op made for it is released.
#[tokio::test]
async fn an_expected_inbound_claim_waits_for_a_live_holder() {
    let mut h = Harness::new("conn-identity-matched-beside-ring").await;
    let in_ring: SocketAddr = "198.51.100.26:6001".parse().unwrap();
    let matched: SocketAddr = "198.51.100.26:6002".parse().unwrap();
    let peer = key();

    h.accept(in_ring).await;
    h.connect_request(in_ring, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.connect_peer(PeerKeyLocation::new(peer.clone(), in_ring))
        .await;
    let connection_manager = &h.mgr.bridge.op_manager.ring.connection_manager;
    assert!(connection_manager.should_accept(Location::from_address(&matched), matched));
    assert!(
        connection_manager.has_connection_or_pending(matched),
        "premise: reserved"
    );
    h.accept_expected(matched, &peer).await;
    assert_eq!(h.mgr.connections[&matched].pub_key(), None);
    assert_eq!(h.pending_on(matched), Some(true));
    assert_eq!(h.ring_key_at(matched), None);
    assert_eq!(h.ring_addr_of(&peer), Some(in_ring));
    assert!(
        !h.mgr
            .bridge
            .op_manager
            .ring
            .connection_manager
            .has_connection_or_pending(matched),
        "the reservation is released"
    );
}

/// The cleanup on the event loop's shutdown path decides per-peer cleanup
/// through the same helper as every other disconnect path.
#[test]
fn shutdown_cleanup_uses_the_shared_gating() {
    let source = include_str!("../../p2p_protoc.rs");
    let at = source
        .find("// Clean up all active connections")
        .expect("the shutdown cleanup");
    let block = &source[at..at + 6000.min(source.len() - at)];
    assert!(block.contains("ctx.forget_peer_state_unless_in_ring("));
    assert!(!block.contains("on_ring_connection_lost("));
}

/// A connection waiting for a key keeps that key: a ConnectRequest on it
/// naming another key is a mismatch and closes it, as for a bound one.
#[tokio::test]
async fn a_waiting_connection_keeps_its_key_against_a_connect_request() {
    let mut h = Harness::new("conn-identity-waiting-request").await;
    let holder: SocketAddr = "198.51.100.40:6001".parse().unwrap();
    let waiting: SocketAddr = "198.51.100.40:6002".parse().unwrap();
    h.watch(&[waiting]);
    let (peer, other) = (key(), key());

    h.dial(holder, &peer).await;
    h.accept_expected(waiting, &peer).await;
    assert_eq!(h.pending_on(waiting), Some(true));
    h.refused_connect_request(waiting, PeerKeyLocation::with_unknown_addr(other.clone()))
        .await;

    assert!(!h.mgr.connections.contains_key(&waiting), "closed");
    assert!(seen(waiting).contains(&ConnIdentity::ChangedConnectRequest));
    assert!(!h.mgr.addr_by_pub_key.contains_key(&other));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&holder));
}

/// A connection waiting for a key keeps that key: a ConnectPeer naming
/// another key for it is refused and promotes nothing.
#[tokio::test]
async fn a_waiting_connection_keeps_its_key_against_a_connect_peer() {
    let mut h = Harness::new("conn-identity-waiting-peer").await;
    let holder: SocketAddr = "198.51.100.41:6001".parse().unwrap();
    let waiting: SocketAddr = "198.51.100.41:6002".parse().unwrap();
    h.watch(&[waiting]);
    let (peer, other) = (key(), key());

    h.dial(holder, &peer).await;
    h.accept_expected(waiting, &peer).await;
    let ring_before = h_count(&h);
    assert!(
        !h.try_connect_peer(PeerKeyLocation::new(other.clone(), waiting))
            .await,
        "refused"
    );

    assert_eq!(h.ring_key_at(waiting), None);
    assert_eq!(h_count(&h), ring_before);
    assert_eq!(
        h.pending_on(waiting),
        Some(true),
        "still waiting for its key"
    );
    assert_eq!(h.mgr.connections[&waiting].pub_key(), None);
    assert!(seen(waiting).contains(&ConnIdentity::ChangedConnectPeer));
    assert_eq!(h.ring_addr_of(&other), None);
}

/// A dial held back by a live holder releases the reservation the connect op
/// made for it.
#[tokio::test]
async fn a_held_dial_releases_its_reservation() {
    let mut h = Harness::new("conn-identity-held-dial-reservation").await;
    let holder: SocketAddr = "198.51.100.42:6001".parse().unwrap();
    let dialled: SocketAddr = "198.51.100.42:6002".parse().unwrap();
    let peer = key();

    h.dial(holder, &peer).await;
    let connection_manager = &h.mgr.bridge.op_manager.ring.connection_manager;
    assert!(connection_manager.should_accept(Location::from_address(&dialled), dialled));
    assert_eq!(
        connection_manager.get_reserved_connections(),
        1,
        "premise: reserved"
    );
    h.dial(dialled, &peer).await;

    assert_eq!(h.pending_on(dialled), Some(true));
    assert_eq!(
        h.mgr
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_reserved_connections(),
        0
    );
}

/// A dial whose key is held by a quiet connection replaces it and is promoted
/// in its place, which takes up the reservation.
#[tokio::test]
async fn a_dial_replacing_a_quiet_holder_leaves_no_reservation() {
    let mut h = Harness::new("conn-identity-quiet-dial-reservation").await;
    let holder: SocketAddr = "198.51.100.43:6001".parse().unwrap();
    let dialled: SocketAddr = "198.51.100.43:6002".parse().unwrap();
    let peer = key();

    let clock = h.dial_with_receipt(holder, &peer).await;
    clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
    let connection_manager = &h.mgr.bridge.op_manager.ring.connection_manager;
    assert!(connection_manager.should_accept(Location::from_address(&dialled), dialled));
    h.dial(dialled, &peer).await;

    assert!(!h.mgr.connections.contains_key(&holder), "replaced");
    assert_eq!(h.ring_addr_of(&peer), Some(dialled));
    assert_eq!(
        h.mgr
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_reserved_connections(),
        0
    );
    // Promoted in the replaced holder's place on arrival, before its task
    // started: the key sent then is still delivered.
    assert_eq!(h.transport_keys(dialled).await, vec![peer]);
}

/// A transient dial left out of the ring for being transient, with no other
/// connection holding its key, keeps its reservation.
#[tokio::test]
async fn a_transient_dial_keeps_its_reservation() {
    // Not a gateway: a gateway promotes a transient connection anyway.
    let mut h = Harness::with_non_gateway_ring("conn-identity-transient-reservation").await;
    let dialled: SocketAddr = "198.51.100.44:6001".parse().unwrap();
    let peer = key();

    let connection_manager = &h.mgr.bridge.op_manager.ring.connection_manager;
    assert!(connection_manager.should_accept(Location::from_address(&dialled), dialled));
    h.establish(
        dialled,
        Some(PeerKeyLocation::new(peer.clone(), dialled)),
        true,
    )
    .await;

    assert_eq!(h.mgr.connections[&dialled].pub_key(), Some(&peer));
    assert_eq!(h.ring_key_at(dialled), None, "premise: not promoted");
    assert_eq!(
        h.mgr
            .bridge
            .op_manager
            .ring
            .connection_manager
            .get_reserved_connections(),
        1
    );
}

/// A ConnectRequest giving the connection's own address counts
/// `connect_request_known_addr` when it replaces a quiet holder, as on every
/// other outcome.
#[tokio::test]
async fn connect_request_replacing_a_quiet_holder_counts_known_addr() {
    let mut h = Harness::new("conn-identity-request-quiet-known").await;
    let old: SocketAddr = "198.51.100.48:6001".parse().unwrap();
    let new: SocketAddr = "198.51.100.48:6002".parse().unwrap();
    h.watch(&[new]);
    let peer = key();

    let clock = h.dial_with_receipt(old, &peer).await;
    h.accept(new).await;
    clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
    h.connect_request(new, PeerKeyLocation::new(peer.clone(), new))
        .await;

    assert!(!h.mgr.connections.contains_key(&old), "premise: replaced");
    assert!(seen(new).contains(&ConnIdentity::ConnectRequestKnownAddr));
}

/// A claim waiting behind a holder that is heard from is counted as a long
/// wait once it has waited `LONG_CLAIM_WAIT`, once only, and keeps waiting:
/// the holder is not touched.
#[tokio::test(start_paused = true)]
async fn a_claim_held_long_by_a_live_holder_is_counted_once() {
    let mut h = Harness::new("conn-identity-claim-held-long").await;
    let holder: SocketAddr = "198.51.100.47:6201".parse().unwrap();
    let waiting: SocketAddr = "198.51.100.47:6202".parse().unwrap();
    h.watch(&[holder, waiting]);
    let peer = key();
    let _clock = h.dial_with_receipt(holder, &peer).await;
    h.accept_expected(waiting, &peer).await;
    let handshake = h.handshake.clone();
    let counted = |addr| {
        seen(addr)
            .into_iter()
            .filter(|kind| *kind == ConnIdentity::ClaimHeldLong)
            .count()
    };

    tokio::time::advance(identity::LONG_CLAIM_WAIT - Duration::from_secs(1)).await;
    h.mgr.retry_pending_claims(&handshake).await;
    assert_eq!(counted(waiting), 0, "not before the wait");

    tokio::time::advance(Duration::from_secs(1)).await;
    h.mgr.retry_pending_claims(&handshake).await;
    assert_eq!(counted(waiting), 1);
    tokio::time::advance(Duration::from_secs(5)).await;
    h.mgr.retry_pending_claims(&handshake).await;
    assert_eq!(counted(waiting), 1, "once per claim");

    // The holder is checked on the bridge, not the ring: with paused time
    // moved this far, the harness's own ring maintenance (no live peers)
    // prunes its ring connections.
    assert_eq!(counted(holder), 0);
    assert_eq!(
        h.mgr.connections[&holder].pub_key(),
        Some(&peer),
        "the holder is kept"
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&holder));
    assert_eq!(h.pending_on(waiting), Some(true), "the claim keeps waiting");
}

/// A claim whose holder has gone quiet is not counted as a long wait: the
/// holder is replaced instead.
#[tokio::test(start_paused = true)]
async fn a_claim_whose_holder_went_quiet_is_not_counted_as_long() {
    let mut h = Harness::new("conn-identity-claim-quiet-not-long").await;
    let holder: SocketAddr = "198.51.100.47:6211".parse().unwrap();
    let waiting: SocketAddr = "198.51.100.47:6212".parse().unwrap();
    h.watch(&[waiting]);
    let peer = key();
    let clock = h.dial_with_receipt(holder, &peer).await;
    h.accept_expected(waiting, &peer).await;
    clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
    tokio::time::advance(identity::LONG_CLAIM_WAIT).await;
    let handshake = h.handshake.clone();
    h.mgr.retry_pending_claims(&handshake).await;

    assert!(!seen(waiting).contains(&ConnIdentity::ClaimHeldLong));
    assert_eq!(h.ring_addr_of(&peer), Some(waiting));
}

/// A connection that replaces a quiet holder on arrival is promoted while it
/// is being set up; the dashboard status keeps the location and key that
/// promotion recorded, rather than the bare entry the setup records for a
/// new connection.
#[test]
fn a_connection_promoted_on_arrival_keeps_its_recorded_location() {
    let _lock = crate::node::network_status::TEST_GLOBAL_STATE_LOCK
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    crate::node::network_status::init(0, Default::default(), "test".to_string());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test runtime")
        .block_on(async {
            let mut h = Harness::new("conn-identity-arrival-status").await;
            let old: SocketAddr = "198.51.100.47:6221".parse().unwrap();
            let new: SocketAddr = "198.51.100.47:6222".parse().unwrap();
            let peer = key();
            let clock = h.dial_with_receipt(old, &peer).await;
            clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
            h.dial(new, &peer).await;
            assert_eq!(
                h.ring_addr_of(&peer),
                Some(new),
                "premise: promoted on arrival"
            );

            let snapshot = crate::node::network_status::get_snapshot().expect("initialized");
            let entry = snapshot
                .peers
                .iter()
                .find(|p| p.address == new)
                .expect("the new connection is recorded");
            assert!(entry.location.is_some(), "location kept");
            assert_eq!(
                entry.peer_key_location.as_ref().map(|p| p.pub_key()),
                Some(&peer),
                "key kept"
            );
        });
}

/// The drop notice to a connection's task does not wait on a full channel:
/// it returns at once, and the notice is delivered once there is room.
#[tokio::test(start_paused = true)]
async fn a_drop_notice_to_a_full_channel_does_not_wait() {
    let (sender, mut receiver) = mpsc::channel(1);
    let addr: SocketAddr = "198.51.100.47:6231".parse().unwrap();
    let filler = Right(ConnEvent::NodeAction(NodeEvent::DropConnection(
        addr,
        crate::tracing::DisconnectReason::Explicit("filler".into()),
    )));
    sender.try_send(filler).expect("room for one");

    super::notify_connection_dropped(
        sender,
        addr,
        crate::tracing::DisconnectReason::Explicit("dropped".into()),
    );
    // Returned with the channel still full; now make room.
    assert!(receiver.recv().await.is_some(), "the filler");
    let delivered = tokio::time::timeout(Duration::from_millis(500), receiver.recv())
        .await
        .expect("delivered once there is room");
    assert!(matches!(
        delivered,
        Some(Right(ConnEvent::NodeAction(NodeEvent::DropConnection(at, _)))) if at == addr
    ));
}

/// Every explicit drop goes through the nonblocking notice: the event loop's
/// `drop_connection_by_addr` calls `notify_connection_dropped` and sends
/// nothing to the connection's task itself.
#[test]
fn drop_connection_by_addr_notifies_without_waiting() {
    let source = include_str!("../connection_lifecycle.rs");
    let start = source
        .find("    pub(super) async fn drop_connection_by_addr(")
        .expect("the function");
    let tests_at = source.find("#[cfg(test)]").unwrap_or(source.len());
    assert!(start < tests_at, "the signature is outside the test module");
    let end = start
        + source[start..]
            .find("\n    }\n")
            .expect("end of the function");
    let body = &source[start..end];
    assert!(
        body.lines().any(|line| line
            .trim()
            .starts_with("notify_connection_dropped(conn.sender")),
        "the drop notice must be sent through notify_connection_dropped"
    );
    assert!(
        !body.contains("conn.sender\n") && !body.contains("conn.sender.send("),
        "no send to the connection's task inside drop_connection_by_addr"
    );
}

/// A dialled connection's transport is given its key when the connection is
/// promoted, once: a later ConnectPeer for the connection, already in the
/// ring, sends nothing.
#[tokio::test]
async fn a_dial_gives_its_transport_the_key_once_at_promotion() {
    let mut h = Harness::new("transport-key-dial").await;
    let addr: SocketAddr = "203.0.113.210:6101".parse().unwrap();
    h.watch(&[addr]);
    let peer = key();

    h.dial(addr, &peer).await;
    assert_eq!(
        h.ring_key_at(addr).as_ref(),
        Some(&peer),
        "premise: promoted"
    );
    assert_eq!(h.transport_keys(addr).await, vec![peer.clone()]);

    h.connect_peer(PeerKeyLocation::new(peer.clone(), addr))
        .await;
    assert_eq!(
        h.transport_keys(addr).await,
        vec![peer.clone()],
        "no second key for a connection already in the ring"
    );
}

/// A connection bound to a key but not promoted gives its transport no key.
/// Here a transient dial on a node that is not a gateway, which stays out of
/// the ring until the connect op promotes it.
#[tokio::test]
async fn an_unpromoted_connection_gives_its_transport_no_key_until_promoted() {
    let mut h = Harness::with_non_gateway_ring("transport-key-transient").await;
    let addr: SocketAddr = "203.0.113.211:6102".parse().unwrap();
    h.watch(&[addr]);
    let peer = key();

    h.establish(addr, Some(PeerKeyLocation::new(peer.clone(), addr)), true)
        .await;
    assert_eq!(
        h.mgr.connections[&addr].pub_key(),
        Some(&peer),
        "premise: bound"
    );
    assert_eq!(h.ring_key_at(addr), None, "premise: not promoted");
    assert_eq!(
        h.transport_keys(addr).await,
        vec![],
        "bound is not promoted"
    );

    h.connect_peer(PeerKeyLocation::new(peer.clone(), addr))
        .await;
    assert_eq!(h.ring_key_at(addr).as_ref(), Some(&peer));
    assert_eq!(h.transport_keys(addr).await, vec![peer]);
}

/// An inbound connection that names itself in a ConnectRequest is bound to
/// that key but not promoted, so its transport is not given the key; the
/// ConnectPeer that promotes it gives it.
#[tokio::test]
async fn an_inbound_connection_learns_its_key_at_promotion_not_from_its_request() {
    let mut h = Harness::new("transport-key-inbound").await;
    let addr: SocketAddr = "203.0.113.212:6103".parse().unwrap();
    h.watch(&[addr]);
    let peer = key();

    h.accept(addr).await;
    h.connect_request(addr, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(
        h.mgr.connections[&addr].pub_key(),
        Some(&peer),
        "premise: bound"
    );
    assert_eq!(
        h.transport_keys(addr).await,
        vec![],
        "a request does not promote"
    );

    h.connect_peer(PeerKeyLocation::new(peer.clone(), addr))
        .await;
    assert_eq!(h.transport_keys(addr).await, vec![peer]);
}

/// A ConnectPeer for a connection with no identity promotes it and then
/// binds the named key; the transport is given the key once bound, and
/// nothing is counted as a mismatch.
#[tokio::test]
async fn a_connect_peer_on_an_unbound_connection_gives_its_transport_the_key() {
    let mut h = Harness::new("transport-key-connect-peer-unbound").await;
    let addr: SocketAddr = "203.0.113.217:6108".parse().unwrap();
    h.watch(&[addr]);
    let peer = key();

    h.accept(addr).await;
    assert_eq!(h.transport_keys(addr).await, vec![], "premise: unbound");
    h.connect_peer(PeerKeyLocation::new(peer.clone(), addr))
        .await;
    assert_eq!(h.ring_key_at(addr).as_ref(), Some(&peer));
    assert_eq!(h.transport_keys(addr).await, vec![peer]);
    assert_eq!(seen(addr), vec![ConnIdentity::SetConnectPeer]);
}

/// A matched expected-inbound claim held back by a dial of the same key
/// gives its transport no key while it waits, and the key once it is bound
/// and promoted after the dial closes.
#[tokio::test]
async fn a_pending_claim_gives_its_transport_the_key_when_promoted() {
    let mut h = Harness::new("transport-key-pending-claim").await;
    let dialled_at: SocketAddr = "203.0.113.213:6104".parse().unwrap();
    let claimed_at: SocketAddr = "203.0.113.214:6105".parse().unwrap();
    h.watch(&[dialled_at, claimed_at]);
    let peer = key();

    h.dial(dialled_at, &peer).await;
    h.accept_expected(claimed_at, &peer).await;
    assert_eq!(seen(claimed_at), vec![ConnIdentity::ClaimHeld], "premise");
    assert_eq!(h.transport_keys(claimed_at).await, vec![]);

    h.transport_closed(dialled_at).await;
    assert_eq!(h.ring_key_at(claimed_at).as_ref(), Some(&peer), "premise");
    assert_eq!(h.transport_keys(claimed_at).await, vec![peer]);
}

/// The key a transport is given is the one bound to its connection. A
/// promotion under any other key, or of a connection with none bound, gives
/// the transport nothing and is counted.
#[tokio::test]
async fn a_promotion_under_a_key_not_bound_is_refused_and_counted() {
    let mut h = Harness::new("transport-key-mismatch").await;
    let bound_at: SocketAddr = "203.0.113.215:6106".parse().unwrap();
    let unbound_at: SocketAddr = "203.0.113.216:6107".parse().unwrap();
    h.watch(&[bound_at, unbound_at]);
    let (peer, other) = (key(), key());

    h.dial(bound_at, &peer).await;
    assert_eq!(
        h.transport_keys(bound_at).await,
        vec![peer.clone()],
        "premise"
    );
    h.mgr.tell_transport_peer_identity(bound_at, &other);
    assert_eq!(
        h.transport_keys(bound_at).await,
        vec![peer.clone()],
        "the other key is not sent"
    );
    assert_eq!(
        seen(bound_at),
        vec![ConnIdentity::SetDialed, ConnIdentity::PromotionKeyMismatch]
    );
    assert_eq!(
        h.transport_seen(bound_at).await,
        vec![ConnIdentity::TransportKeySent],
        "only the promotion's key was sent"
    );

    h.accept(unbound_at).await;
    h.mgr.tell_transport_peer_identity(unbound_at, &other);
    assert_eq!(h.transport_keys(unbound_at).await, vec![]);
    assert_eq!(seen(unbound_at), vec![ConnIdentity::PromotionKeyMismatch]);
}

/// A node whose transport has no QUIC endpoint gives no transport a key: no
/// promotion path sends one, and nothing is counted.
#[tokio::test]
async fn quic_off_gives_no_transport_a_key() {
    let mut h = Harness::new("transport-key-quic-off").await;
    h.mgr.transport_has_quic = false;
    let dialled: SocketAddr = "203.0.113.218:6109".parse().unwrap();
    let requested: SocketAddr = "203.0.113.219:6110".parse().unwrap();
    let matched: SocketAddr = "203.0.113.220:6111".parse().unwrap();
    h.watch(&[dialled, requested, matched]);
    let (a, b, c) = (key(), key(), key());

    h.dial(dialled, &a).await;
    h.accept(requested).await;
    h.connect_peer(PeerKeyLocation::new(b.clone(), requested))
        .await;
    h.accept_expected(matched, &c).await;
    for (addr, peer) in [(dialled, &a), (requested, &b), (matched, &c)] {
        assert_eq!(
            h.ring_key_at(addr).as_ref(),
            Some(peer),
            "premise: promoted"
        );
        assert_eq!(h.transport_keys(addr).await, vec![], "{addr}");
        assert_eq!(h.transport_seen(addr).await, vec![], "{addr}");
    }
}

/// A key the node cannot hand to a connection's task at once (its channel
/// is full) is sent from a short task and counted when it gets through; one
/// still not handed over after the 1 s fallback is counted as undelivered.
#[tokio::test(start_paused = true)]
async fn a_key_for_a_full_channel_is_counted_sent_or_undelivered() {
    let counts = Some(super::DeliveryCounts {
        delivered: ConnIdentity::TransportKeySent,
        timed_out: ConnIdentity::TransportKeyUndelivered,
    });
    let filler = || {
        Right(ConnEvent::NodeAction(NodeEvent::DropConnection(
            "198.51.100.48:6232".parse().unwrap(),
            crate::tracing::DisconnectReason::Explicit("filler".into()),
        )))
    };
    for (addr, drain, expected) in [
        ("198.51.100.49:6233", true, ConnIdentity::TransportKeySent),
        (
            "198.51.100.50:6234",
            false,
            ConnIdentity::TransportKeyUndelivered,
        ),
    ] {
        let addr: SocketAddr = addr.parse().unwrap();
        TRANSPORT_METRICS.watch_conn_identity(addr);
        let (sender, mut receiver) = mpsc::channel(1);
        sender.try_send(filler()).expect("room for one");
        super::notify_connection_task(
            sender,
            Right(ConnEvent::SetPeerIdentity(key())),
            addr,
            "peer identity",
            counts,
        );
        assert_eq!(seen_all(addr), vec![], "nothing handed over yet");
        if drain {
            assert!(receiver.recv().await.is_some(), "the filler");
            let delivered = tokio::time::timeout(Duration::from_millis(500), receiver.recv())
                .await
                .expect("delivered once there is room");
            assert!(matches!(
                delivered,
                Some(Right(ConnEvent::SetPeerIdentity(_)))
            ));
        }
        // Past the 1 s fallback; also lets the sending task count.
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(seen_all(addr), vec![expected], "{addr}");
        drop(receiver);
    }
}

/// Every identity decision recorded for `addr`, unfiltered.
fn seen_all(addr: SocketAddr) -> Vec<ConnIdentity> {
    TRANSPORT_METRICS.conn_identity_from(addr)
}

/// A promotion of a connection the ring already holds adds nothing, so it
/// is not a promotion: it reports `AlreadyInRing` and gives the transport
/// no second key. Here the address is also still tracked as transient,
/// which makes `promote_existing_connection` ask the ring again.
#[tokio::test]
async fn promoting_a_connection_already_in_the_ring_sends_no_second_key() {
    let mut h = Harness::new("transport-key-already-in-ring").await;
    let addr: SocketAddr = "203.0.113.221:6112".parse().unwrap();
    h.watch(&[addr]);
    let peer = key();

    h.dial(addr, &peer).await;
    assert_eq!(h.transport_keys(addr).await, vec![peer.clone()], "premise");
    let connection_manager = &h.mgr.bridge.op_manager.ring.connection_manager;
    assert!(connection_manager.try_register_transient(addr, None));
    assert!(connection_manager.is_in_ring(addr), "premise: in the ring");

    let promotion = h.mgr.promote_existing_connection(addr, &peer, None).await;
    assert_eq!(promotion, Promotion::AlreadyInRing);
    assert_eq!(h.transport_keys(addr).await, vec![peer]);
    assert_eq!(
        h.transport_seen(addr).await,
        vec![ConnIdentity::TransportKeySent],
        "one key sent, at the dial's promotion"
    );
}

/// A native connection's key was proven in its QUIC handshake: the node binds
/// it as `Proven` (counted `SetProven`), links the peer under it and answers
/// the ConnectPeer.
#[tokio::test]
async fn a_native_connection_binds_its_proven_key() {
    use crate::operations::connect::native::DialTransport;
    let mut h = Harness::new("conn-identity-native-proven").await;
    let addr: SocketAddr = "203.0.113.230:6201".parse().unwrap();
    let peer = key();
    h.watch(&[addr]);
    let mut result = pending_connect_peer_over(
        &mut h,
        PeerKeyLocation::new(peer.clone(), addr),
        DialTransport::native_now(),
    )
    .await;
    h.handshake_event(HandshakeEvent::OutboundEstablished {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: PeerKeyLocation::new(peer.clone(), addr),
        connection: Box::new(IdleConnection::new(addr)),
        transient: false,
        transport: DialTransport::native_now(),
        origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    assert!(
        matches!(result.try_recv(), Ok(Ok(_))),
        "the dial is answered"
    );
    assert_eq!(seen(addr), vec![ConnIdentity::SetProven]);
    assert_eq!(h.ring_key_at(addr).as_ref(), Some(&peer));
    h.assert_own_key_absent();
}

/// A failed native setup: the connect op dials UDP next, so the address is
/// not marked failed and the peer gets no backoff, either of which would
/// refuse that dial. Only an outcome that says native does not work on the
/// path, seen as joiner, counts toward demotion: three joiner timeouts with
/// distinct peers demote the node; three refusals by its own punch budget do
/// not, nor do three timeouts as acceptor (a joiner chose this node, and
/// could fail on purpose).
#[tokio::test]
async fn a_failed_native_setup_leaves_the_udp_fallback_open_and_demotes_only_on_path_failures() {
    use crate::operations::connect::native::DialTransport;
    use crate::transport::NativeSetupOutcome;
    let mut h = Harness::new("native-failure").await;
    use crate::transport::NatAttemptOrigin;
    for (round, outcome, origin, demotions) in [
        (
            0u16,
            NativeSetupOutcome::OverBudgetDestination,
            NatAttemptOrigin::ConnectJoiner,
            0,
        ),
        (
            1,
            NativeSetupOutcome::TimedOut,
            NatAttemptOrigin::ConnectAcceptor,
            0,
        ),
        (
            2,
            NativeSetupOutcome::TimedOut,
            NatAttemptOrigin::ConnectJoiner,
            1,
        ),
    ] {
        for i in 0..3u16 {
            let addr: SocketAddr = format!("203.0.113.231:{}", 6210 + round * 10 + i)
                .parse()
                .unwrap();
            let peer = PeerKeyLocation::new(key(), addr);
            let mut result =
                pending_connect_peer_over(&mut h, peer.clone(), DialTransport::native_now()).await;
            h.handshake_event(HandshakeEvent::OutboundFailed {
                transaction: Transaction::new::<ConnectMsg>(),
                peer,
                // A transport-level error, which marks a UDP dial's address.
                error: ConnectionError::Timeout,
                transient: false,
                transport: DialTransport::native_now(),
                native_outcome: Some(outcome),
                origin,
            })
            .await;
            assert_eq!(
                result.try_recv(),
                Ok(Err(Some(outcome))),
                "the dial failed, and its requester learns how (10.2 item 16)"
            );
            assert!(
                !failed_addrs(&h).contains(&addr),
                "a native failure does not mark the address"
            );
            assert!(
                !h.state.peer_backoff.is_in_backoff(addr),
                "a native failure gives no backoff"
            );
        }
        assert_eq!(
            h.mgr
                .bridge
                .op_manager
                .ring
                .connection_manager
                .native_connect()
                .demotions(),
            demotions,
            "{outcome:?} as {origin:?}"
        );
    }
}

/// Review round 2: demotion is attributed by the same rule for successes as
/// for failures. A native success as acceptor does not reset the joiner
/// streak (its failures never count), so two joiner timeouts, an acceptor
/// success and a third joiner timeout demote the node; a success as joiner
/// in the same place resets the streak and the node is not demoted.
#[tokio::test]
async fn only_a_joiners_native_success_resets_its_demotion_streak() {
    use crate::operations::connect::native::DialTransport;
    use crate::transport::{NatAttemptOrigin, NativeSetupOutcome};
    for (case, success_origin, demotions) in [
        (0u16, NatAttemptOrigin::ConnectAcceptor, 1),
        (1, NatAttemptOrigin::ConnectJoiner, 0),
    ] {
        let mut h = Harness::new(&format!("native-success-origin-{case}")).await;
        let addr = |i: u16| -> SocketAddr {
            format!("203.0.113.233:{}", 6240 + case * 10 + i)
                .parse()
                .unwrap()
        };
        let fail = |i: u16| HandshakeEvent::OutboundFailed {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: PeerKeyLocation::new(key(), addr(i)),
            error: ConnectionError::FailedConnectOp,
            transient: false,
            transport: DialTransport::native_now(),
            native_outcome: Some(NativeSetupOutcome::TimedOut),
            origin: NatAttemptOrigin::ConnectJoiner,
        };
        h.handshake_event(fail(0)).await;
        h.handshake_event(fail(1)).await;
        let winner = PeerKeyLocation::new(key(), addr(2));
        let _result =
            pending_connect_peer_over(&mut h, winner.clone(), DialTransport::native_now()).await;
        h.handshake_event(HandshakeEvent::OutboundEstablished {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: winner,
            connection: Box::new(IdleConnection::new(addr(2))),
            transient: false,
            transport: DialTransport::native_now(),
            origin: success_origin,
        })
        .await;
        h.handshake_event(fail(3)).await;
        assert_eq!(
            h.mgr
                .bridge
                .op_manager
                .ring
                .connection_manager
                .native_connect()
                .demotions(),
            demotions,
            "a native success as {success_origin:?}"
        );
    }
}

/// With native connects off (production today) only UDP dials exist, and
/// they merge by address exactly as before the beside-dial change: one
/// handshake command, every requester answered by its outcome, nothing
/// beside. Both outcomes.
#[tokio::test]
async fn udp_only_dials_still_merge_by_address() {
    use crate::operations::connect::native::DialTransport;
    use crate::transport::NatAttemptOrigin;
    for (case, connects) in [(0u16, true), (1, false)] {
        let mut h = Harness::new(&format!("udp-only-merge-{case}")).await;
        let addr: SocketAddr = format!("203.0.113.235:{}", 6270 + case).parse().unwrap();
        let peer = PeerKeyLocation::new(key(), addr);
        let mut first = pending_connect_peer_over(&mut h, peer.clone(), DialTransport::Udp).await;
        let mut second = pending_connect_peer_over(&mut h, peer.clone(), DialTransport::Udp).await;
        let mut dialled = 0;
        while let Ok(command) = h.handshake_rx.try_recv() {
            if matches!(command, HandshakeCommand::Connect { .. }) {
                dialled += 1;
            }
        }
        assert_eq!(dialled, 1, "the second request joins the first dial");
        assert!(h.state.udp_beside_native.is_empty());
        assert!(h.state.native_pending.is_empty());
        let event = if connects {
            HandshakeEvent::OutboundEstablished {
                transaction: Transaction::new::<ConnectMsg>(),
                peer: peer.clone(),
                connection: Box::new(IdleConnection::new(addr)),
                transient: false,
                transport: DialTransport::Udp,
                origin: NatAttemptOrigin::ConnectJoiner,
            }
        } else {
            HandshakeEvent::OutboundFailed {
                transaction: Transaction::new::<ConnectMsg>(),
                peer: peer.clone(),
                error: ConnectionError::Timeout,
                transient: false,
                transport: DialTransport::Udp,
                native_outcome: None,
                origin: NatAttemptOrigin::ConnectJoiner,
            }
        };
        h.handshake_event(event).await;
        let expected = if connects {
            Ok((addr, None))
        } else {
            Err(None)
        };
        assert_eq!(first.try_recv(), Ok(expected));
        assert_eq!(second.try_recv(), Ok(expected));
    }
}

/// A UDP dial asked for while a native dial to the same address is pending
/// (another CONNECT's) starts at once BESIDE it, and a later UDP dial joins
/// it. The native dial's failure answers only its own callbacks; the UDP
/// dial's success answers everyone, since all of them asked for a connection
/// to this peer. Merged into the native dial, a UDP fallback failed with it;
/// held back until it ended, it punched late (found by
/// test_quic_native_blocked_pairs_connect_over_udp_behind_nats).
#[tokio::test]
async fn a_udp_dial_runs_beside_a_pending_native_dial() {
    use crate::operations::connect::native::DialTransport;
    use crate::transport::{NatAttemptOrigin, NativeSetupOutcome};
    for (case, native_fails_first) in [(0u16, true), (1, false)] {
        let mut h = Harness::new(&format!("udp-beside-native-{case}")).await;
        let addr: SocketAddr = format!("203.0.113.234:{}", 6250 + case).parse().unwrap();
        let peer = PeerKeyLocation::new(key(), addr);
        let mut native =
            pending_connect_peer_over(&mut h, peer.clone(), DialTransport::native_now()).await;
        let mut udp = pending_connect_peer_over(&mut h, peer.clone(), DialTransport::Udp).await;
        let mut joined = pending_connect_peer_over(&mut h, peer.clone(), DialTransport::Udp).await;
        let mut dialled = Vec::new();
        while let Ok(command) = h.handshake_rx.try_recv() {
            if let HandshakeCommand::Connect { transport, .. } = command {
                dialled.push(transport.is_native());
            }
        }
        assert_eq!(
            dialled,
            vec![true, false],
            "a native dial, then one UDP dial beside it; the third request joins the UDP dial"
        );
        for pending in [&mut native, &mut udp, &mut joined] {
            assert!(pending.try_recv().is_err(), "premise: all pending");
        }
        let established = || HandshakeEvent::OutboundEstablished {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: peer.clone(),
            connection: Box::new(IdleConnection::new(addr)),
            transient: false,
            transport: DialTransport::Udp,
            origin: NatAttemptOrigin::ConnectJoiner,
        };
        let native_failed = HandshakeEvent::OutboundFailed {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: peer.clone(),
            error: ConnectionError::FailedConnectOp,
            transient: false,
            transport: DialTransport::native_now(),
            native_outcome: Some(NativeSetupOutcome::TimedOut),
            origin: NatAttemptOrigin::ConnectJoiner,
        };
        if native_fails_first {
            h.handshake_event(native_failed).await;
            assert_eq!(
                native.try_recv(),
                Ok(Err(Some(crate::transport::NativeSetupOutcome::TimedOut))),
                "the native dial failed"
            );
            assert!(udp.try_recv().is_err(), "the UDP dial is still running");
            assert!(joined.try_recv().is_err(), "so is its joined request");
            let event = established();
            h.handshake_event(event).await;
            assert_eq!(udp.try_recv(), Ok(Ok((addr, None))));
            assert_eq!(joined.try_recv(), Ok(Ok((addr, None))));
        } else {
            let event = established();
            h.handshake_event(event).await;
            for pending in [&mut native, &mut udp, &mut joined] {
                assert_eq!(pending.try_recv(), Ok(Ok((addr, None))));
            }
            // The native dial's late failure leaves the connection.
            h.handshake_event(native_failed).await;
            assert!(h.mgr.connections.contains_key(&addr), "still connected");
        }
    }
}

/// The UDP dial beside a native one fails: only its callbacks are failed,
/// the native dial keeps running and its outcome still answers its own.
#[tokio::test]
async fn a_failed_udp_dial_beside_a_native_dial_leaves_the_native_dial() {
    use crate::operations::connect::native::DialTransport;
    use crate::transport::NatAttemptOrigin;
    let mut h = Harness::new("udp-beside-native-fails").await;
    let addr: SocketAddr = "203.0.113.234:6260".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    let mut native =
        pending_connect_peer_over(&mut h, peer.clone(), DialTransport::native_now()).await;
    let mut udp = pending_connect_peer_over(&mut h, peer.clone(), DialTransport::Udp).await;
    h.handshake_event(HandshakeEvent::OutboundFailed {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: peer.clone(),
        error: ConnectionError::Timeout,
        transient: false,
        transport: DialTransport::Udp,
        native_outcome: None,
        origin: NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    assert_eq!(udp.try_recv(), Ok(Err(None)), "the UDP dial failed");
    assert!(
        native.try_recv().is_err(),
        "the native dial is still pending"
    );
    h.handshake_event(HandshakeEvent::OutboundEstablished {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: peer.clone(),
        connection: Box::new(IdleConnection::new(addr)),
        transient: false,
        transport: DialTransport::native_now(),
        origin: NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    assert_eq!(
        native.try_recv(),
        Ok(Ok((addr, None))),
        "the native dial connected"
    );
}

/// Builders for the beside-dial tests: a native dial pending to `addr` and a
/// UDP dial beside it, and the four outcomes.
mod beside {
    use super::*;
    use crate::operations::connect::native::DialTransport;
    use crate::transport::{NatAttemptOrigin, NativeSetupOutcome};

    pub(super) type Pending = mpsc::Receiver<crate::message::ConnectResult>;

    /// A native dial to `addr`, then a UDP dial beside it.
    pub(super) async fn dials(
        h: &mut Harness,
        addr: SocketAddr,
    ) -> (PeerKeyLocation, Pending, Pending) {
        let peer = PeerKeyLocation::new(key(), addr);
        let native = pending_connect_peer_over(h, peer.clone(), DialTransport::native_now()).await;
        let udp = pending_connect_peer_over(h, peer.clone(), DialTransport::Udp).await;
        assert!(
            h.state.udp_beside_native.contains_key(&addr),
            "premise: the UDP dial is beside"
        );
        (peer, native, udp)
    }

    pub(super) fn established(peer: &PeerKeyLocation, native: bool) -> HandshakeEvent {
        let addr = peer.socket_addr().unwrap();
        HandshakeEvent::OutboundEstablished {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: peer.clone(),
            connection: Box::new(IdleConnection::new(addr)),
            transient: false,
            transport: if native {
                DialTransport::native_now()
            } else {
                DialTransport::Udp
            },
            origin: NatAttemptOrigin::ConnectJoiner,
        }
    }

    pub(super) fn failed(peer: &PeerKeyLocation, native: bool) -> HandshakeEvent {
        HandshakeEvent::OutboundFailed {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: peer.clone(),
            error: if native {
                ConnectionError::FailedConnectOp
            } else {
                ConnectionError::Timeout
            },
            transient: false,
            transport: if native {
                DialTransport::native_now()
            } else {
                DialTransport::Udp
            },
            native_outcome: native.then_some(NativeSetupOutcome::TimedOut),
            origin: NatAttemptOrigin::ConnectJoiner,
        }
    }

    /// The bookkeeping holds nothing for `addr` once both dials ended.
    pub(super) fn assert_clear(h: &Harness, addr: SocketAddr) {
        assert!(
            !h.state.native_pending.contains(&addr),
            "native_pending cleared"
        );
        assert!(
            !h.state.udp_beside_native.contains_key(&addr),
            "beside entry cleared"
        );
        assert!(
            !h.state.beside_udp_connected.contains(&addr),
            "beside_udp_connected cleared"
        );
    }
}

/// #6253 review item 1: the native dial connects, then the UDP dial beside it
/// fails. Its requesters are answered with the native connection (they asked
/// for a connection to this peer and have one), and the failure prunes
/// nothing, backs off nothing and leaves the peer in the ring.
#[tokio::test]
async fn native_success_then_a_failed_udp_dial_beside_it_keeps_the_connection() {
    let mut h = Harness::new("beside-native-ok-udp-fails").await;
    let addr: SocketAddr = "203.0.113.236:6280".parse().unwrap();
    let (peer, mut native, mut udp) = beside::dials(&mut h, addr).await;
    h.handshake_event(beside::established(&peer, true)).await;
    assert_eq!(native.try_recv(), Ok(Ok((addr, None))));
    assert_eq!(
        udp.try_recv(),
        Ok(Ok((addr, None))),
        "answered with the native connection"
    );
    let id = h.mgr.connections[&addr].connection_id;
    h.handshake_event(beside::failed(&peer, false)).await;
    assert_eq!(
        h.mgr.connections[&addr].connection_id, id,
        "the native connection stays"
    );
    assert_eq!(
        h.ring_key_at(addr).as_ref(),
        Some(peer.pub_key()),
        "still in the ring"
    );
    assert!(!h.state.peer_backoff.is_in_backoff(addr), "no backoff");
    assert!(
        !failed_addrs(&h).contains(&addr),
        "the address is not marked failed"
    );
    beside::assert_clear(&h, addr);
}

/// #6253 review item 2: both dials connect, the native one first. The first
/// connection (native) is kept and the UDP one is dropped without touching
/// the ring, and counted.
#[tokio::test]
async fn native_then_udp_both_connect_keeps_the_native_connection() {
    let mut h = Harness::new("beside-native-then-udp").await;
    let addr: SocketAddr = "203.0.113.236:6281".parse().unwrap();
    let before = both_connected_here();
    let survivors = survivor_here();
    let (peer, mut native, mut udp) = beside::dials(&mut h, addr).await;
    h.handshake_event(beside::established(&peer, true)).await;
    h.prove_native(addr);
    let id = h.mgr.connections[&addr].connection_id;
    let pair = PairState::of(&h, addr);
    h.handshake_event(beside::established(&peer, false)).await;
    assert_eq!(both_connected_here(), (before.0, before.1 + 1));
    assert_eq!(survivor_here(), (survivors.0 + 1, survivors.1));
    assert_eq!(PairState::of(&h, addr), pair, "nothing else changed");
    assert_eq!(native.try_recv(), Ok(Ok((addr, None))));
    assert_eq!(udp.try_recv(), Ok(Ok((addr, None))));
    assert_eq!(
        h.mgr.connections[&addr].connection_id, id,
        "the native connection stays"
    );
    assert_eq!(h.ring_key_at(addr).as_ref(), Some(peer.pub_key()));
    assert_eq!(h_count(&h), 1);
    beside::assert_clear(&h, addr);
}

/// Both dials connected on this thread: (kept UDP, kept native).
fn both_connected_here() -> (u64, u64) {
    let [udp, native] =
        crate::transport::metrics::BOTH_DIALS_CONNECTED_ON_THIS_THREAD.with(|c| c.get());
    (udp, native)
}

/// Gate 18: both dials connect, the UDP one first. The survivor rule keeps
/// the native connection: it is swapped in for the UDP one in place, without
/// promotion, prune (so no second DropConnection) or transient budget call,
/// and counted. What the ring and the connection manager hold for the peer
/// survives the swap: ring entry, ReadyState, connected-since, health record
/// and transient count (the in-place switch #6253 removed lost these).
#[tokio::test]
async fn udp_then_native_both_connect_swaps_in_the_native_connection() {
    let mut h = Harness::new("beside-udp-then-native").await;
    let addr: SocketAddr = "203.0.113.236:6282".parse().unwrap();
    let before = both_connected_here();
    let survivors = survivor_here();
    let (peer, mut native, mut udp) = beside::dials(&mut h, addr).await;
    h.handshake_event(beside::established(&peer, false)).await;
    assert_eq!(native.try_recv(), Ok(Ok((addr, None))));
    assert_eq!(udp.try_recv(), Ok(Ok((addr, None))));
    let udp_id = h.mgr.connections[&addr].connection_id;
    assert!(!h.mgr.connections[&addr].native, "premise: a UDP entry");
    let cm = &h.mgr.bridge.op_manager.ring.connection_manager;
    cm.mark_peer_ready(addr);
    cm.peer_health.lock().record_success(addr);
    let pair = PairState::of(&h, addr);
    assert!(pair.marked_ready && pair.connected_since.is_some() && pair.health == Some((1, 0)));
    while h.handshake_rx.try_recv().is_ok() {}
    h.handshake_event(beside::established(&peer, true)).await;
    assert_eq!(
        h.mgr.connections[&addr].connection_id, udp_id,
        "held beside the UDP entry until it carries a frame"
    );
    h.prove_native(addr);
    let entry = &h.mgr.connections[&addr];
    assert_ne!(
        entry.connection_id, udp_id,
        "the native connection took the entry"
    );
    assert!(entry.native);
    let mut drops = 0;
    while let Ok(command) = h.handshake_rx.try_recv() {
        if matches!(command, HandshakeCommand::DropConnection { addr: a } if a == addr) {
            drops += 1;
        }
    }
    // The arm drops the address's expectation once, as for any outbound
    // connection; a replacement would prune and send a second.
    assert_eq!(drops, 1, "nothing was pruned or replaced");
    assert_eq!(PairState::of(&h, addr), pair, "everything else survived");
    assert_eq!(both_connected_here(), (before.0 + 1, before.1));
    assert_eq!(survivor_here(), (survivors.0, survivors.1 + 1));
    beside::assert_clear(&h, addr);
    // The UDP listener's close carries the old id and is ignored.
    h.mgr
        .handle_transport_event(
            Some(ConnEvent::TransportClosed {
                remote_addr: addr,
                error: TransportError::ConnectionClosed(addr),
                connection_id: udp_id,
                reason: crate::tracing::DisconnectReason::Timeout,
            }),
            &mut h.state,
            &h.handshake,
        )
        .await
        .expect("handle_transport_event");
    assert!(
        h.mgr.connections[&addr].native,
        "the stale close is ignored"
    );
    assert_eq!(PairState::of(&h, addr), pair);
    // The old UDP listener itself ended: its sender was dropped, so it
    // reports its close, under the old id.
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match h._conn_events_rx.recv().await {
                Some(ConnEvent::TransportClosed { connection_id, .. })
                    if connection_id == udp_id =>
                {
                    return true;
                }
                Some(_) => continue,
                None => return false,
            }
        }
    })
    .await;
    assert_eq!(ended, Ok(true), "the old UDP listener ended");
}

/// What the node holds for a peer apart from its transport: what the
/// survivor rule must leave alone.
#[derive(Debug, PartialEq)]
struct PairState {
    ring_key: Option<TransportPublicKey>,
    in_ring: bool,
    transient: bool,
    transient_count: usize,
    marked_ready: bool,
    connected_since: Option<tokio::time::Instant>,
    health: Option<(u64, u64)>,
    bound_key: Option<TransportPublicKey>,
    created_at: Option<tokio::time::Instant>,
    connections: usize,
}

impl PairState {
    fn of(h: &Harness, addr: SocketAddr) -> Self {
        let cm = &h.mgr.bridge.op_manager.ring.connection_manager;
        h.assert_open_transports_match();
        let entry = h.mgr.connections.get(&addr);
        Self {
            ring_key: h.ring_key_at(addr),
            in_ring: cm.is_in_ring(addr),
            transient: cm.is_transient(addr),
            transient_count: cm.transient_count(),
            marked_ready: cm.is_marked_ready(addr),
            connected_since: cm.connected_since(addr),
            health: cm.peer_health.lock().counts(&addr),
            bound_key: entry.and_then(|e| e.pub_key().cloned()),
            created_at: entry.map(|e| e.created_at),
            connections: h_count(h),
        }
    }
}

/// Survivor-rule decisions on this thread: (UDP dropped, native swapped in).
fn survivor_here() -> (u64, u64) {
    let [dropped, swapped] = crate::transport::metrics::SURVIVOR_ON_THIS_THREAD.with(|c| c.get());
    (dropped, swapped)
}

/// #6253 review round 2 (M3): the native dial connects but its connection is
/// not kept (the transient budget is spent), so the requesters of the UDP
/// dial beside it are not told they are connected; they wait for their own
/// dial.
#[tokio::test]
async fn beside_requesters_are_answered_only_with_a_kept_connection() {
    use crate::operations::connect::native::DialTransport;
    use crate::transport::NatAttemptOrigin;
    let mut h = Harness::with_config("beside-native-not-kept", |config| {
        config.transient_budget = 0;
    })
    .await;
    let addr: SocketAddr = "203.0.113.236:6287".parse().unwrap();
    let (peer, _native, mut udp) = beside::dials(&mut h, addr).await;
    h.handshake_event(HandshakeEvent::OutboundEstablished {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: peer.clone(),
        connection: Box::new(IdleConnection::new(addr)),
        transient: true,
        transport: DialTransport::native_now(),
        origin: NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    assert!(!h.mgr.connections.contains_key(&addr), "premise: not kept");
    assert!(
        udp.try_recv().is_err(),
        "not answered: no connection exists"
    );
    assert!(
        h.state.udp_beside_native.contains_key(&addr),
        "its dial still runs"
    );
    // Gate 19: followed through, the UDP dial's own outcome answers them.
    h.handshake_event(beside::established(&peer, false)).await;
    assert_eq!(
        udp.try_recv(),
        Ok(Ok((addr, None))),
        "answered by its own dial"
    );
    assert!(
        !h.mgr.connections[&addr].native,
        "the UDP connection is kept"
    );
    assert_eq!(h.ring_key_at(addr).as_ref(), Some(peer.pub_key()));
    beside::assert_clear(&h, addr);
}

/// #6253 review item 3: the UDP dial beside a pending native one fails. No
/// backoff is recorded, so the native dial's own UDP fallback, which comes
/// after the native dial fails, is not refused.
#[tokio::test]
async fn a_failed_udp_dial_beside_a_native_dial_takes_no_backoff() {
    use crate::operations::connect::native::DialTransport;
    let mut h = Harness::new("beside-udp-fails-no-backoff").await;
    let addr: SocketAddr = "203.0.113.236:6283".parse().unwrap();
    let (peer, mut native, mut udp) = beside::dials(&mut h, addr).await;
    h.handshake_event(beside::failed(&peer, false)).await;
    assert_eq!(udp.try_recv(), Ok(Err(None)));
    assert!(!h.state.peer_backoff.is_in_backoff(addr), "no backoff");
    h.handshake_event(beside::failed(&peer, true)).await;
    assert_eq!(
        native.try_recv(),
        Ok(Err(Some(crate::transport::NativeSetupOutcome::TimedOut)))
    );
    beside::assert_clear(&h, addr);
    while h.handshake_rx.try_recv().is_ok() {}
    let mut fallback = pending_connect_peer_over(&mut h, peer.clone(), DialTransport::Udp).await;
    assert!(
        fallback.try_recv().is_err(),
        "the fallback is dialled, not refused"
    );
    assert!(
        matches!(
            h.handshake_rx.try_recv(),
            Ok(HandshakeCommand::Connect { .. })
        ),
        "the fallback's dial was sent"
    );
}

/// The native dial fails while the UDP dial beside it runs, then the UDP
/// dial fails too: the native failure leaves the in-transit slot to the UDP
/// dial, and the last failure takes the ordinary failure path (in-transit
/// release, failed address, diagnostics), backoff included (gate 19: the
/// beside dial skips the backoff only while the native dial is pending).
#[tokio::test]
async fn native_failure_then_udp_failure_beside_it() {
    let mut h = Harness::new("beside-native-fails-udp-fails").await;
    let addr: SocketAddr = "203.0.113.236:6284".parse().unwrap();
    let (peer, mut native, mut udp) = beside::dials(&mut h, addr).await;
    h.handshake_event(beside::failed(&peer, true)).await;
    assert_eq!(
        native.try_recv(),
        Ok(Err(Some(crate::transport::NativeSetupOutcome::TimedOut)))
    );
    assert!(udp.try_recv().is_err(), "the UDP dial is still running");
    h.handshake_event(beside::failed(&peer, false)).await;
    assert_eq!(udp.try_recv(), Ok(Err(None)));
    assert!(
        h.state.peer_backoff.is_in_backoff(addr),
        "the last dial's failure backs off"
    );
    assert!(
        failed_addrs(&h).contains(&addr),
        "the last dial's failure marks the address, as a UDP failure does"
    );
    beside::assert_clear(&h, addr);
}

/// #6253 review item 4: the handshake driver closes while only a UDP dial
/// beside a native one waits (the native dial's own requester was answered).
/// Its requesters get the failure and nothing is left.
#[tokio::test]
async fn driver_shutdown_fails_requesters_of_a_udp_dial_beside_a_native_one() {
    let mut h = Harness::new("beside-shutdown").await;
    let addr: SocketAddr = "203.0.113.236:6285".parse().unwrap();
    let (_peer, mut native, mut udp) = beside::dials(&mut h, addr).await;
    // Leave only the beside dial waiting.
    let native_callbacks = h.state.awaiting_connection.remove(&addr);
    h.state.awaiting_connection_txs.remove(&addr);
    drop(native_callbacks);
    assert!(native.try_recv().is_err());
    h.mgr
        .handle_handshake_stream_closed(&mut h.state)
        .await
        .expect("shutdown");
    assert_eq!(udp.try_recv(), Ok(Err(None)));
    beside::assert_clear(&h, addr);
}

/// The handshake command channel is closed when a UDP dial would start
/// beside a native one: its requester gets the failure and no beside entry is
/// left.
#[tokio::test]
async fn a_udp_dial_beside_a_native_one_that_cannot_be_sent_fails() {
    use crate::operations::connect::native::DialTransport;
    let mut h = Harness::new("beside-channel-closed").await;
    let addr: SocketAddr = "203.0.113.236:6286".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    let _native =
        pending_connect_peer_over(&mut h, peer.clone(), DialTransport::native_now()).await;
    drop(std::mem::replace(&mut h.handshake_rx, mpsc::channel(1).1));
    let mut udp = pending_connect_peer_over(&mut h, peer, DialTransport::Udp).await;
    assert_eq!(udp.try_recv(), Ok(Err(None)));
    assert!(!h.state.udp_beside_native.contains_key(&addr));
}

/// The server of a native setup waits longer than its client, so the pair
/// can connect by the client's UDP fallback before the server's setup ends.
/// That late failure must leave the connection alone: it stays in the
/// connection map and in the ring, and the address is not marked failed.
#[tokio::test]
async fn a_late_native_failure_keeps_the_connection_made_meanwhile() {
    use crate::operations::connect::native::DialTransport;
    use crate::transport::NativeSetupOutcome;
    let mut h = Harness::new("native-late-failure").await;
    let addr: SocketAddr = "203.0.113.232:6230".parse().unwrap();
    let peer = key();
    let mut result = pending_connect_peer(&mut h, PeerKeyLocation::new(peer.clone(), addr)).await;
    h.handshake_event(HandshakeEvent::OutboundEstablished {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: PeerKeyLocation::new(peer.clone(), addr),
        connection: Box::new(IdleConnection::new(addr)),
        transient: false,
        transport: DialTransport::Udp,
        origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    assert!(matches!(result.try_recv(), Ok(Ok(_))), "premise: connected");
    assert_eq!(
        h.ring_key_at(addr).as_ref(),
        Some(&peer),
        "premise: in the ring"
    );

    h.handshake_event(HandshakeEvent::OutboundFailed {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: PeerKeyLocation::new(peer.clone(), addr),
        error: ConnectionError::FailedConnectOp,
        transient: false,
        transport: DialTransport::native_now(),
        native_outcome: Some(NativeSetupOutcome::TimedOut),
        origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    assert!(h.mgr.connections.contains_key(&addr), "still connected");
    assert_eq!(
        h.ring_key_at(addr).as_ref(),
        Some(&peer),
        "still in the ring"
    );
    assert!(!failed_addrs(&h).contains(&addr));
    assert_eq!(h_count(&h), 1);
}

/// How a connection reaches the handler, for the survivor-rule tests.
#[derive(Debug, Clone, Copy)]
enum Arrival {
    /// A native dial's outcome (`OutboundEstablished`, native).
    Native,
    /// A UDP dial's outcome (`OutboundEstablished`, UDP).
    OutboundUdp,
    /// An inbound UDP connection matched to an expectation that carried the
    /// peer's key.
    InboundUdpKeyed,
    /// An inbound UDP connection with no expectation, so no key.
    InboundUdpUnkeyed,
}

impl Arrival {
    fn event(self, peer: &PeerKeyLocation, transient: bool) -> HandshakeEvent {
        use crate::operations::connect::native::DialTransport;
        let addr = peer.socket_addr().unwrap();
        let connection: Box<dyn PeerConnectionApi> = Box::new(IdleConnection::new(addr));
        let outbound = |transport| HandshakeEvent::OutboundEstablished {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: peer.clone(),
            connection,
            transient,
            transport,
            origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
        };
        match self {
            Self::Native => outbound(DialTransport::native_now()),
            Self::OutboundUdp => outbound(DialTransport::Udp),
            Self::InboundUdpKeyed | Self::InboundUdpUnkeyed => HandshakeEvent::InboundConnection {
                transaction: None,
                matched: match self {
                    Self::InboundUdpKeyed => {
                        HandshakeInboundMatch::Expected(Some(peer.pub_key().clone()))
                    }
                    Self::Native | Self::OutboundUdp | Self::InboundUdpUnkeyed => {
                        HandshakeInboundMatch::Unexpected
                    }
                },
                connection: Box::new(IdleConnection::new(addr)),
                transient,
            },
        }
    }
}

/// Gate 18: one survivor rule for every order and direction. With a native
/// and a UDP connection to one peer the node keeps the native one: a UDP
/// connection arriving second (dialled, or inbound with or without a key) is
/// dropped; a native one arriving second is swapped in for the UDP entry in
/// place. Either way the ring, the connection manager's records and the
/// transient count are untouched, with the connection promoted or transient,
/// and the decision is counted. None of these is the beside pair, so the
/// both-dials counter does not move. An unkeyed inbound UDP entry is bound to
/// no key, so a native connection arriving after it is not the same peer by
/// key: it takes the ordinary path, replacing the entry, and is kept native.
#[tokio::test]
async fn survivor_rule_keeps_native_in_each_order_and_direction() {
    let cases = [
        (Arrival::Native, Arrival::OutboundUdp),
        (Arrival::Native, Arrival::InboundUdpKeyed),
        (Arrival::Native, Arrival::InboundUdpUnkeyed),
        (Arrival::OutboundUdp, Arrival::Native),
        (Arrival::InboundUdpKeyed, Arrival::Native),
        (Arrival::InboundUdpUnkeyed, Arrival::Native),
    ];
    let mut port = 6300u16;
    for transient in [false, true] {
        for (first, second) in cases {
            port += 1;
            let case = format!("{first:?} then {second:?}, transient {transient}");
            let id = format!("survivor-{port}");
            // A non-gateway ring keeps a transient connection out of the ring.
            let mut h = if transient {
                Harness::with_non_gateway_ring(&id).await
            } else {
                Harness::new(&id).await
            };
            let addr: SocketAddr = format!("203.0.113.240:{port}").parse().unwrap();
            let peer = PeerKeyLocation::new(key(), addr);
            h.handshake_event(first.event(&peer, transient)).await;
            if matches!(first, Arrival::Native) {
                h.prove_native(addr);
            }
            let first_id = h.mgr.connections[&addr].connection_id;
            assert_eq!(
                h.mgr.connections[&addr].native,
                matches!(first, Arrival::Native),
                "{case}: premise"
            );
            // An unkeyed inbound connection is held as transient until it
            // is named, whatever the flag.
            if !matches!(first, Arrival::InboundUdpUnkeyed) {
                assert_eq!(
                    h.mgr
                        .bridge
                        .op_manager
                        .ring
                        .connection_manager
                        .is_transient(addr),
                    transient,
                    "{case}: premise"
                );
            }
            let pair = PairState::of(&h, addr);
            let (survivors, both) = (survivor_here(), both_connected_here());
            h.handshake_event(second.event(&peer, transient)).await;
            if matches!(second, Arrival::Native) {
                h.prove_native(addr);
            }
            let entry = &h.mgr.connections[&addr];
            assert!(entry.native, "{case}: the native connection is kept");
            if matches!(first, Arrival::InboundUdpUnkeyed) {
                assert_ne!(entry.connection_id, first_id, "{case}: replaced");
                assert_eq!(entry.pub_key(), Some(peer.pub_key()), "{case}: bound");
                assert_eq!(h.mgr.connections.len(), 1, "{case}: one entry");
                if !transient {
                    assert_eq!(
                        h.ring_key_at(addr).as_ref(),
                        Some(peer.pub_key()),
                        "{case}: ring"
                    );
                }
                assert_eq!(
                    survivor_here(),
                    survivors,
                    "{case}: not a survivor decision"
                );
                continue;
            }
            let swapped = matches!(second, Arrival::Native);
            assert_eq!(
                entry.connection_id != first_id,
                swapped,
                "{case}: connection id"
            );
            assert_eq!(
                PairState::of(&h, addr),
                pair,
                "{case}: nothing else changed"
            );
            let expected = if swapped {
                (survivors.0, survivors.1 + 1)
            } else {
                (survivors.0 + 1, survivors.1)
            };
            assert_eq!(survivor_here(), expected, "{case}: counted");
            assert_eq!(both_connected_here(), both, "{case}: not the beside pair");
        }
    }
}

/// Two UDP connections to one address keep the older behaviour: the later
/// replaces the earlier, and the survivor rule counts nothing.
#[tokio::test]
async fn two_udp_connections_to_one_address_still_replace() {
    let mut h = Harness::new("survivor-udp-udp").await;
    let addr: SocketAddr = "203.0.113.241:6320".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    let first_id = h.mgr.connections[&addr].connection_id;
    let survivors = survivor_here();
    h.handshake_event(Arrival::InboundUdpKeyed.event(&peer, false))
        .await;
    assert_ne!(h.mgr.connections[&addr].connection_id, first_id, "replaced");
    assert!(!h.mgr.connections[&addr].native);
    assert_eq!(survivor_here(), survivors);
}

/// Gate 19: a native dial landing on an entry bound to another key (the peer
/// came back under a new key, e.g. after a suspend) does not keep the stale
/// entry or swap into it: the ordinary path replaces it, and the peer is in
/// the ring under the new key.
#[tokio::test]
async fn a_native_dial_onto_a_stale_key_replaces_the_entry() {
    let mut h = Harness::new("survivor-stale-key").await;
    let addr: SocketAddr = "203.0.113.241:6321".parse().unwrap();
    let old = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&old, false))
        .await;
    let old_id = h.mgr.connections[&addr].connection_id;
    let survivors = survivor_here();
    let new = PeerKeyLocation::new(key(), addr);
    let mut result = pending_connect_peer_over(
        &mut h,
        new.clone(),
        crate::operations::connect::native::DialTransport::native_now(),
    )
    .await;
    // The ConnectPeer for the new key meets the stale entry and is refused;
    // the native dial in flight (another CONNECT's) still lands.
    assert_eq!(
        result.try_recv(),
        Ok(Err(None)),
        "premise: refused on the stale entry"
    );
    h.handshake_event(Arrival::Native.event(&new, false)).await;
    let entry = &h.mgr.connections[&addr];
    assert_ne!(entry.connection_id, old_id, "the stale entry is replaced");
    assert!(entry.native);
    assert_eq!(entry.pub_key(), Some(new.pub_key()));
    assert_eq!(h.ring_key_at(addr).as_ref(), Some(new.pub_key()));
    assert_eq!(h_count(&h), 1);
    assert_eq!(survivor_here(), survivors, "not a survivor decision");
}

/// Gate 19: a working native setup as joiner resets the demotion streak
/// even when its connection is not the one kept (a native entry was already
/// there) or is swapped in: two joiner timeouts, the success, a third
/// timeout, and the node is not demoted.
#[tokio::test]
async fn a_native_success_resets_the_streak_whatever_is_kept() {
    use crate::operations::connect::native::DialTransport;
    use crate::transport::{NatAttemptOrigin, NativeSetupOutcome};
    for (case, existing) in [(0u16, Arrival::Native), (1, Arrival::OutboundUdp)] {
        let mut h = Harness::new(&format!("survivor-streak-{case}")).await;
        let addr = |i: u16| -> SocketAddr {
            format!("203.0.113.242:{}", 6330 + case * 10 + i)
                .parse()
                .unwrap()
        };
        let fail = |i: u16| HandshakeEvent::OutboundFailed {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: PeerKeyLocation::new(key(), addr(i)),
            error: ConnectionError::FailedConnectOp,
            transient: false,
            transport: DialTransport::native_now(),
            native_outcome: Some(NativeSetupOutcome::TimedOut),
            origin: NatAttemptOrigin::ConnectJoiner,
        };
        h.handshake_event(fail(0)).await;
        h.handshake_event(fail(1)).await;
        let winner = PeerKeyLocation::new(key(), addr(2));
        h.handshake_event(existing.event(&winner, false)).await;
        h.handshake_event(Arrival::Native.event(&winner, false))
            .await;
        h.prove_native(addr(2));
        assert!(h.mgr.connections[&addr(2)].native, "{existing:?}: premise");
        h.handshake_event(fail(3)).await;
        assert_eq!(
            h.mgr
                .bridge
                .op_manager
                .ring
                .connection_manager
                .native_connect()
                .demotions(),
            0,
            "{existing:?}: the success reset the streak"
        );
    }
}

/// Gate 19: the both-dials counter counts only the beside pair. A UDP dial
/// beside a native one connects while the address holds a UDP connection
/// from elsewhere (an inbound one): that is two UDP connections, so the
/// later replaces the earlier and nothing is counted (#6253 counted it as
/// "kept native"). When the native dial then connects it is swapped in, and
/// that is the pair: counted UDP first.
#[tokio::test]
async fn both_dials_counts_only_the_beside_pair() {
    let mut h = Harness::new("survivor-beside-only").await;
    let addr: SocketAddr = "203.0.113.242:6350".parse().unwrap();
    let before = both_connected_here();
    let (peer, mut native, mut udp) = beside::dials(&mut h, addr).await;
    h.handshake_event(Arrival::InboundUdpKeyed.event(&peer, false))
        .await;
    assert_eq!(
        native.try_recv(),
        Ok(Ok((addr, None))),
        "answered by the inbound"
    );
    h.handshake_event(beside::established(&peer, false)).await;
    assert_eq!(udp.try_recv(), Ok(Ok((addr, None))));
    assert_eq!(both_connected_here(), before, "not the pair yet");
    assert!(!h.mgr.connections[&addr].native);
    h.handshake_event(beside::established(&peer, true)).await;
    h.prove_native(addr);
    assert!(h.mgr.connections[&addr].native, "swapped in");
    assert_eq!(both_connected_here(), (before.0 + 1, before.1));
    beside::assert_clear(&h, addr);
}

/// Gate 18, both ends: the two nodes of a pair see the same two connections
/// in different orders and directions, and each keeps the native one, so
/// they agree without exchanging anything. The #6253 round-3 case: the
/// joiner's native dial connects before its UDP dial beside it, while the
/// acceptor sees that UDP dial arrive inbound before its own native setup
/// ends. Keeping the first connection at each end, the joiner kept native
/// and the acceptor kept UDP, and each closed the connection the other kept.
/// Each scenario lists the arrivals at the joiner, then at the acceptor.
#[tokio::test]
async fn both_ends_apply_the_survivor_rule_to_the_same_facts() {
    let scenarios = [
        (
            [Arrival::Native, Arrival::OutboundUdp],
            [Arrival::InboundUdpKeyed, Arrival::Native],
        ),
        (
            [Arrival::OutboundUdp, Arrival::Native],
            [Arrival::Native, Arrival::InboundUdpKeyed],
        ),
        (
            [Arrival::Native, Arrival::OutboundUdp],
            [Arrival::InboundUdpUnkeyed, Arrival::Native],
        ),
        (
            [Arrival::OutboundUdp, Arrival::Native],
            [Arrival::Native, Arrival::InboundUdpUnkeyed],
        ),
    ];
    for (i, (at_joiner, at_acceptor)) in scenarios.into_iter().enumerate() {
        let i = i as u16;
        let mut joiner = Harness::new(&format!("survivor-ends-j{i}")).await;
        let mut acceptor = Harness::new(&format!("survivor-ends-a{i}")).await;
        let joiner_addr: SocketAddr = format!("203.0.113.244:{}", 6370 + i).parse().unwrap();
        let acceptor_addr: SocketAddr = format!("203.0.113.245:{}", 6370 + i).parse().unwrap();
        let acceptor_seen = PeerKeyLocation::new(acceptor.own_key(), acceptor_addr);
        let joiner_seen = PeerKeyLocation::new(joiner.own_key(), joiner_addr);
        // Each end's native connection carries the peer's probe at once.
        for arrival in at_joiner {
            joiner
                .handshake_event(arrival.event(&acceptor_seen, false))
                .await;
            if matches!(arrival, Arrival::Native) {
                joiner.prove_native(acceptor_addr);
            }
        }
        for arrival in at_acceptor {
            acceptor
                .handshake_event(arrival.event(&joiner_seen, false))
                .await;
            if matches!(arrival, Arrival::Native) {
                acceptor.prove_native(joiner_addr);
            }
        }
        let kept = (
            joiner.mgr.connections[&acceptor_addr].native,
            acceptor.mgr.connections[&joiner_addr].native,
        );
        assert_eq!(
            kept,
            (true, true),
            "{at_joiner:?} at the joiner, {at_acceptor:?} at the acceptor: both keep native"
        );
    }
}

/// The survivor rule looks a claimed key up by key, not only at the
/// connection's address: an inbound UDP connection matched to an expectation
/// for a peer whose key a native connection holds at another address (its
/// UDP traffic seen from another port) is dropped too, and the native entry
/// stays as it was.
#[tokio::test]
async fn a_keyed_udp_connection_from_another_address_is_dropped_against_native() {
    let mut h = Harness::new("survivor-key-elsewhere").await;
    let native_addr: SocketAddr = "203.0.113.246:6380".parse().unwrap();
    let udp_addr: SocketAddr = "203.0.113.246:6381".parse().unwrap();
    let peer_key = key();
    let native_peer = PeerKeyLocation::new(peer_key.clone(), native_addr);
    h.handshake_event(Arrival::Native.event(&native_peer, false))
        .await;
    let pair = PairState::of(&h, native_addr);
    let survivors = survivor_here();
    h.handshake_event(
        Arrival::InboundUdpKeyed.event(&PeerKeyLocation::new(peer_key, udp_addr), false),
    )
    .await;
    assert!(!h.mgr.connections.contains_key(&udp_addr), "dropped");
    assert_eq!(PairState::of(&h, native_addr), pair);
    assert_eq!(survivor_here(), (survivors.0 + 1, survivors.1));
}

/// A native connection with a receipt stamp on `clock`, established by a
/// native dial to `peer`.
fn native_with_receipt(peer: &PeerKeyLocation, clock: &ReceiptClock) -> HandshakeEvent {
    let addr = peer.socket_addr().unwrap();
    HandshakeEvent::OutboundEstablished {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: peer.clone(),
        connection: Box::new(IdleConnection {
            addr,
            receipt: Some(clock.stamp()),
        }),
        transient: false,
        transport: crate::operations::connect::native::DialTransport::native_now(),
        origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
    }
}

/// #6255 review item 1: native wins only over a live native entry. Once the
/// native connection has not been heard from within the hold window (the
/// peer restarted, or its NAT rebound, and the native connection will only
/// end at the QUIC idle timeout), a UDP connection from the peer takes the
/// ordinary path and is kept: dialled under the same key, inbound under the
/// same key from a new address, and inbound without a key (a peer back
/// under a new key). While the native entry is live, each is dropped.
#[tokio::test]
async fn native_wins_only_over_a_live_native_entry() {
    for (case, udp, live) in [
        (0u16, Arrival::OutboundUdp, false),
        (1, Arrival::InboundUdpKeyed, false),
        (2, Arrival::InboundUdpUnkeyed, false),
        (3, Arrival::OutboundUdp, true),
        (4, Arrival::InboundUdpKeyed, true),
        (5, Arrival::InboundUdpUnkeyed, true),
    ] {
        let mut h = Harness::new(&format!("survivor-live-{case}")).await;
        let native_addr: SocketAddr = format!("203.0.113.247:{}", 6400 + case * 2)
            .parse()
            .unwrap();
        // The NAT-rebind case arrives from a new address; the others at the
        // native connection's own.
        let udp_addr: SocketAddr = if matches!(udp, Arrival::InboundUdpKeyed) {
            format!("203.0.113.247:{}", 6401 + case * 2)
                .parse()
                .unwrap()
        } else {
            native_addr
        };
        let peer_key = key();
        let clock = ReceiptClock::default();
        h.handshake_event(native_with_receipt(
            &PeerKeyLocation::new(peer_key.clone(), native_addr),
            &clock,
        ))
        .await;
        h.prove_native(native_addr);
        if !live {
            clock.go_quiet(identity::HOLD_WINDOW + Duration::from_secs(1));
        }
        let survivors = survivor_here();
        h.handshake_event(udp.event(&PeerKeyLocation::new(peer_key.clone(), udp_addr), false))
            .await;
        let kept_udp = h
            .mgr
            .connections
            .get(&udp_addr)
            .is_some_and(|entry| !entry.native);
        assert_eq!(kept_udp, !live, "{udp:?}, live {live}: UDP kept");
        let dropped = if live { 1 } else { 0 };
        assert_eq!(
            survivor_here(),
            (survivors.0 + dropped, survivors.1),
            "{udp:?}, live {live}: counted only when dropped"
        );
    }
}

/// #6255 review item 2: a UDP dial dropped because a native connection at
/// another address holds the peer answers its requesters with the holder's
/// address, and releases the reservation it made at its own.
#[tokio::test]
async fn a_dropped_udp_dial_answers_with_the_holders_address() {
    let mut h = Harness::new("survivor-holder-addr").await;
    let holder: SocketAddr = "203.0.113.248:6420".parse().unwrap();
    let dialled: SocketAddr = "203.0.113.248:6421".parse().unwrap();
    let peer_key = key();
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    assert!(cm.should_accept(Location::from_address(&dialled), dialled));
    let mut result =
        pending_connect_peer(&mut h, PeerKeyLocation::new(peer_key.clone(), dialled)).await;
    h.handshake_event(
        Arrival::Native.event(&PeerKeyLocation::new(peer_key.clone(), holder), false),
    )
    .await;
    assert!(
        cm.has_connection_or_pending(dialled),
        "premise: reserved at the dialled address"
    );
    h.handshake_event(Arrival::OutboundUdp.event(&PeerKeyLocation::new(peer_key, dialled), false))
        .await;
    assert_eq!(
        result.try_recv(),
        Ok(Ok((holder, None))),
        "the holder's address"
    );
    assert!(!h.mgr.connections.contains_key(&dialled), "dropped");
    assert!(
        !cm.has_connection_or_pending(dialled),
        "the reservation at the dialled address is released"
    );
    assert!(cm.is_in_ring(holder), "the holder stays in the ring");
}

/// #6255 review L5: a swap with no entry to swap into is a caller bug; it
/// asserts in debug builds.
#[cfg(debug_assertions)]
#[tokio::test]
#[should_panic(expected = "swap_udp_in_beside_native without an entry")]
async fn a_swap_without_an_entry_asserts() {
    let mut h = Harness::new("survivor-swap-no-entry").await;
    let addr: SocketAddr = "203.0.113.249:6440".parse().unwrap();
    h.use_addr(addr);
    h.mgr
        .swap_udp_in_beside_native(addr, Box::new(IdleConnection::new(addr)))
        .await
        .ok();
}

/// #6255 re-review L2: a dropped UDP dial does not release the reservation
/// at its address while a native dial to that address is pending, since
/// that dial holds the reservation too.
#[tokio::test]
async fn a_dropped_udp_dial_keeps_the_reservation_a_pending_native_dial_holds() {
    use crate::operations::connect::native::DialTransport;
    let mut h = Harness::new("survivor-holder-native-pending").await;
    let holder: SocketAddr = "203.0.113.248:6422".parse().unwrap();
    let dialled: SocketAddr = "203.0.113.248:6423".parse().unwrap();
    let peer_key = key();
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    assert!(cm.should_accept(Location::from_address(&dialled), dialled));
    let dialled_peer = PeerKeyLocation::new(peer_key.clone(), dialled);
    let _native =
        pending_connect_peer_over(&mut h, dialled_peer.clone(), DialTransport::native_now()).await;
    let mut udp = pending_connect_peer_over(&mut h, dialled_peer.clone(), DialTransport::Udp).await;
    assert!(
        h.state.native_pending.contains(&dialled),
        "premise: native dial pending"
    );
    h.handshake_event(Arrival::Native.event(&PeerKeyLocation::new(peer_key, holder), false))
        .await;
    h.handshake_event(Arrival::OutboundUdp.event(&dialled_peer, false))
        .await;
    assert_eq!(
        udp.try_recv(),
        Ok(Ok((holder, None))),
        "the holder's address"
    );
    assert!(
        cm.has_connection_or_pending(dialled),
        "the pending native dial keeps the reservation"
    );
}

/// Delivers a `TransportClosed` for connection `connection_id` at `addr`.
async fn close_connection(h: &mut Harness, addr: SocketAddr, connection_id: u64) {
    h.mgr
        .handle_transport_event(
            Some(ConnEvent::TransportClosed {
                remote_addr: addr,
                error: TransportError::ConnectionClosed(addr),
                connection_id,
                reason: crate::tracing::DisconnectReason::Timeout,
            }),
            &mut h.state,
            &h.handshake,
        )
        .await
        .expect("handle_transport_event");
}

/// Option (b), UDP first: a native connection that arrives for a UDP entry
/// is held beside it, not swapped in, until it carries a frame. Without one
/// it is dropped after `HELD_NATIVE_WINDOW` and the UDP entry never moved,
/// which is the one-sided setup (the peer never held the native connection).
#[tokio::test]
async fn a_native_connection_arriving_second_waits_for_a_frame() {
    let mut h = Harness::new("hold-udp-then-native").await;
    let addr: SocketAddr = "203.0.113.230:6500".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    let udp_id = h.mgr.connections[&addr].connection_id;
    let pair = PairState::of(&h, addr);
    let survivors = survivor_here();
    h.handshake_event(Arrival::Native.event(&peer, false)).await;
    let entry = &h.mgr.connections[&addr];
    assert_eq!(entry.connection_id, udp_id, "the UDP entry stays");
    assert!(!entry.native);
    let held = h.mgr.held_native.get(&addr).expect("held beside the entry");
    assert_ne!(held.connection_id, udp_id);
    assert_eq!(PairState::of(&h, addr), pair, "nothing else changed");
    assert_eq!(survivor_here(), survivors, "no survivor decision yet");
    let since = held.since;
    h.mgr
        .expire_held_native(since + HELD_NATIVE_WINDOW - Duration::from_millis(1));
    assert!(h.mgr.held_native.contains_key(&addr), "still waiting");
    h.mgr.expire_held_native(since + HELD_NATIVE_WINDOW);
    assert!(h.mgr.held_native.is_empty(), "dropped without a frame");
    let entry = &h.mgr.connections[&addr];
    assert_eq!(entry.connection_id, udp_id, "UDP kept");
    assert!(!entry.native);
    assert_eq!(PairState::of(&h, addr), pair, "the ring never moved");
    assert_eq!(survivor_here(), survivors, "no swap was counted");
}

/// Option (b), native first: a UDP connection that arrives for a native
/// entry that has not carried a frame becomes the entry, and the native
/// connection is held beside it. The ring is untouched. When the held native
/// connection then carries a frame it takes the entry back, counted as a
/// swap. Dialled, keyed inbound and unkeyed inbound UDP connections: the
/// unkeyed one is bound to no key and takes the ordinary path.
#[tokio::test]
async fn a_udp_connection_arriving_second_is_kept_until_native_carries_a_frame() {
    for (case, udp) in [(0u16, Arrival::OutboundUdp), (1, Arrival::InboundUdpKeyed)] {
        let mut h = Harness::new(&format!("hold-native-then-udp-{case}")).await;
        let addr: SocketAddr = format!("203.0.113.231:{}", 6510 + case).parse().unwrap();
        let peer = PeerKeyLocation::new(key(), addr);
        h.handshake_event(Arrival::Native.event(&peer, false)).await;
        let native_id = h.mgr.connections[&addr].connection_id;
        assert!(
            !h.mgr.connections[&addr].frame_seen,
            "premise: no frame yet"
        );
        let pair = PairState::of(&h, addr);
        let survivors = survivor_here();
        h.handshake_event(udp.event(&peer, false)).await;
        let entry = &h.mgr.connections[&addr];
        assert!(!entry.native, "{udp:?}: UDP is the entry");
        assert_ne!(entry.connection_id, native_id);
        assert_eq!(
            h.mgr.held_native.get(&addr).map(|held| held.connection_id),
            Some(native_id),
            "{udp:?}: the native connection is held"
        );
        assert_eq!(PairState::of(&h, addr), pair, "{udp:?}: ring untouched");
        assert_eq!(survivor_here(), survivors, "{udp:?}: not decided yet");
        h.prove_native(addr);
        let entry = &h.mgr.connections[&addr];
        assert!(entry.native, "{udp:?}: the native connection took over");
        assert_eq!(entry.connection_id, native_id);
        assert!(h.mgr.held_native.is_empty());
        assert_eq!(PairState::of(&h, addr), pair, "{udp:?}: ring untouched");
        assert_eq!(survivor_here(), (survivors.0, survivors.1 + 1), "{udp:?}");
    }
}

/// Option (b), native first and then UDP, with no frame: the pair keeps
/// working over UDP and the native connection expires.
#[tokio::test]
async fn a_native_entry_that_never_carries_a_frame_loses_to_udp() {
    let mut h = Harness::new("hold-native-never-frames").await;
    let addr: SocketAddr = "203.0.113.231:6520".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::Native.event(&peer, false)).await;
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    let udp_id = h.mgr.connections[&addr].connection_id;
    let since = h.mgr.held_native[&addr].since;
    h.mgr.expire_held_native(since + HELD_NATIVE_WINDOW);
    assert!(h.mgr.held_native.is_empty());
    let entry = &h.mgr.connections[&addr];
    assert!(!entry.native, "UDP stays");
    assert_eq!(entry.connection_id, udp_id);
}

/// A held native connection that closes is forgotten and the UDP entry
/// stays: the peer never held it, so its closing says nothing about UDP.
#[tokio::test]
async fn a_held_native_connection_that_closes_leaves_the_udp_entry() {
    let mut h = Harness::new("hold-closes").await;
    let addr: SocketAddr = "203.0.113.232:6530".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    let udp_id = h.mgr.connections[&addr].connection_id;
    h.handshake_event(Arrival::Native.event(&peer, false)).await;
    let held_id = h.mgr.held_native[&addr].connection_id;
    let pair = PairState::of(&h, addr);
    close_connection(&mut h, addr, held_id).await;
    assert!(h.mgr.held_native.is_empty(), "forgotten");
    assert_eq!(h.mgr.connections[&addr].connection_id, udp_id);
    assert_eq!(PairState::of(&h, addr), pair, "the peer is still connected");
}

/// A native connection whose `recv` ends, with a chosen [`CloseReason`], when
/// told to: what a real `NativePeerConnection` does when quinn ends its path
/// (the idle timeout, the peer's close, a loss), which
/// `transport::peer_connection::native_connection::tests` pins against a
/// real QUIC connection. The node's real `peer_connection_listener` runs over
/// it, so what these tests cover is the node's side of a dead native
/// connection (gate 21).
struct DyingNative {
    inner: IdleConnection,
    end: tokio::sync::oneshot::Receiver<crate::transport::CloseReason>,
    reason: Option<crate::transport::CloseReason>,
}

impl DyingNative {
    fn new(
        addr: SocketAddr,
    ) -> (
        Self,
        tokio::sync::oneshot::Sender<crate::transport::CloseReason>,
    ) {
        let (kill, end) = tokio::sync::oneshot::channel();
        (
            Self {
                inner: IdleConnection::new(addr),
                end,
                reason: None,
            },
            kill,
        )
    }
}

impl PeerConnectionApi for DyingNative {
    fn remote_addr(&self) -> SocketAddr {
        self.inner.remote_addr()
    }

    fn remote_version(&self) -> Option<(u8, u8, u16)> {
        None
    }

    fn close_reason(&self) -> Option<crate::transport::CloseReason> {
        self.reason
    }

    fn send_bytes(
        &mut self,
        data: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        self.inner.send_bytes(data)
    }

    fn recv(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, TransportError>> + Send + '_>> {
        let addr = self.inner.addr;
        Box::pin(async move {
            // Cancel-safe: the receiver is polled in place, never consumed.
            let reason = (&mut self.end).await;
            self.reason = reason.ok();
            Err(TransportError::ConnectionClosed(addr))
        })
    }

    fn set_inbound_stream_registry(
        &mut self,
        registry: Arc<dyn crate::transport::InboundStreamRegistry>,
    ) {
        self.inner.set_inbound_stream_registry(registry);
    }

    fn set_peer_identity(&mut self, key: TransportPublicKey) {
        self.inner.set_peer_identity(key);
    }

    fn send_stream_data(
        &mut self,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        completion_tx: Option<tokio::sync::oneshot::Sender<crate::transport::StreamSendOutcome>>,
        progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        self.inner
            .send_stream_data(stream_id, data, metadata, completion_tx, progress)
    }

    fn pipe_stream_data(
        &mut self,
        outbound_stream_id: StreamId,
        inbound_handle: crate::transport::IncomingStream,
        metadata: Option<bytes::Bytes>,
        progress: Option<Arc<dyn crate::transport::StreamProgressObserver>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + '_>> {
        self.inner
            .pipe_stream_data(outbound_stream_id, inbound_handle, metadata, progress)
    }
}

/// Every way a native path can end, as `recv` reports it.
fn native_end_reasons() -> Vec<crate::transport::CloseReason> {
    use crate::transport::{CloseReason, QuicPathEnd};
    vec![
        CloseReason::IdleTimeout,
        CloseReason::QuicPath(QuicPathEnd::ClosedByPeer),
        CloseReason::QuicPath(QuicPathEnd::ClosedLocally),
        CloseReason::QuicPath(QuicPathEnd::Lost),
        CloseReason::QuicPath(QuicPathEnd::NotAcknowledging),
        CloseReason::QuicPath(QuicPathEnd::Stalled),
    ]
}

/// Delivers the next `TransportClosed` the connections' listeners report.
async fn next_transport_closed(h: &mut Harness) -> ConnEvent {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match h._conn_events_rx.recv().await {
                Some(event @ ConnEvent::TransportClosed { .. }) => return event,
                Some(_) => continue,
                None => panic!("the listeners' event channel closed"),
            }
        }
    })
    .await
    .expect("the listener reported the dead native connection")
}

/// Gate 21: a native connection whose path ends, for any reason, leaves no
/// entry at this end. The listener reports the end, the entry is removed, the
/// peer is pruned from the ring and every map keyed by it is cleaned. Both
/// ends of a native connection are built by one path (`OutboundEstablished`
/// with a native transport, whichever end dialled), so this is each end.
#[tokio::test]
async fn a_native_connection_that_dies_leaves_no_entry() {
    let mut h = Harness::new("native-dies").await;
    for (i, reason) in native_end_reasons().into_iter().enumerate() {
        let addr: SocketAddr = format!("203.0.113.236:{}", 6600 + i).parse().unwrap();
        let peer = PeerKeyLocation::new(key(), addr);
        let (conn, kill) = DyingNative::new(addr);
        h.handshake_event(HandshakeEvent::OutboundEstablished {
            transaction: Transaction::new::<ConnectMsg>(),
            peer: peer.clone(),
            connection: Box::new(conn),
            transient: false,
            transport: crate::operations::connect::native::DialTransport::native_now(),
            origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
        })
        .await;
        h.prove_native(addr);
        let case = format!("{reason:?}");
        assert!(h.mgr.connections[&addr].native, "{case}: premise");
        assert_eq!(
            h.ring_key_at(addr).as_ref(),
            Some(peer.pub_key()),
            "{case}: premise, in the ring"
        );

        kill.send(reason).expect("the connection is listening");
        let closed = next_transport_closed(&mut h).await;
        // The reason the handler passes to `prune_connection` is the event's.
        let ConnEvent::TransportClosed {
            reason: reported, ..
        } = &closed
        else {
            panic!("{case}: not a close");
        };
        assert_eq!(
            reported.to_string(),
            disconnect_reason_for_close(Some(reason)).to_string(),
            "{case}: the disconnect reason"
        );
        h.mgr
            .handle_transport_event(Some(closed), &mut h.state, &h.handshake)
            .await
            .expect("handle_transport_event");

        assert!(!h.mgr.connections.contains_key(&addr), "{case}: entry");
        assert!(h.mgr.held_native.is_empty(), "{case}: nothing held");
        assert!(
            !h.mgr.addr_by_pub_key.contains_key(peer.pub_key()),
            "{case}: key map"
        );
        assert_eq!(h.ring_key_at(addr), None, "{case}: pruned from the ring");
        assert!(
            !h.mgr
                .bridge
                .op_manager
                .ring
                .connection_manager
                .has_connection_or_pending(addr),
            "{case}: no connection or reservation"
        );
        h.assert_open_transports_match();
    }
}

/// Gate 21: a native connection held beside the UDP entry whose path dies is
/// forgotten, by its own listener's report, and the UDP entry stays.
#[tokio::test]
async fn a_held_native_connection_that_dies_is_forgotten() {
    let mut h = Harness::new("native-held-dies").await;
    let addr: SocketAddr = "203.0.113.236:6620".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    let udp_id = h.mgr.connections[&addr].connection_id;
    let (conn, kill) = DyingNative::new(addr);
    h.handshake_event(HandshakeEvent::OutboundEstablished {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: peer.clone(),
        connection: Box::new(conn),
        transient: false,
        transport: crate::operations::connect::native::DialTransport::native_now(),
        origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    assert!(h.mgr.held_native.contains_key(&addr), "premise: held");
    kill.send(crate::transport::CloseReason::IdleTimeout)
        .expect("the connection is listening");
    let closed = next_transport_closed(&mut h).await;
    h.mgr
        .handle_transport_event(Some(closed), &mut h.state, &h.handshake)
        .await
        .expect("handle_transport_event");
    assert!(h.mgr.held_native.is_empty(), "forgotten");
    assert_eq!(h.mgr.connections[&addr].connection_id, udp_id);
}

/// A frame reported by a connection that is neither the entry nor the held
/// one (a replaced connection's listener) changes nothing, and a held
/// native connection is dropped once its UDP entry is removed, whatever the
/// window says (so it cannot be promoted onto a later entry).
#[tokio::test]
async fn a_stray_first_frame_changes_nothing_and_removal_drops_the_hold() {
    let mut h = Harness::new("hold-stray").await;
    let addr: SocketAddr = "203.0.113.232:6531".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    h.handshake_event(Arrival::Native.event(&peer, false)).await;
    let held_id = h.mgr.held_native[&addr].connection_id;
    let survivors = survivor_here();
    h.mgr.on_native_first_frame(addr, held_id + 1000);
    assert!(h.mgr.held_native.contains_key(&addr), "still held");
    assert!(!h.mgr.connections[&addr].native);
    assert_eq!(survivor_here(), survivors);
    h.mgr.remove_connection(addr);
    assert!(h.mgr.held_native.is_empty(), "removed with its entry");
    // A new UDP entry for the same peer and address is not promoted onto by
    // a frame from the old session's native connection.
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    h.mgr.on_native_first_frame(addr, held_id);
    assert!(!h.mgr.connections[&addr].native);
    assert_eq!(survivor_here(), survivors);
}

/// The held connection's deadline is what the event loop wakes for, so
/// expiry runs with no other event; a frame that arrives after the deadline
/// does not promote.
#[tokio::test(start_paused = true)]
async fn the_hold_has_a_wake_deadline_and_a_late_frame_does_not_promote() {
    let mut h = Harness::new("hold-deadline").await;
    let addr: SocketAddr = "203.0.113.234:6550".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    assert_eq!(h.mgr.held_native_deadline(), None);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    h.handshake_event(Arrival::Native.event(&peer, false)).await;
    let since = h.mgr.held_native[&addr].since;
    assert_eq!(
        h.mgr.held_native_deadline(),
        Some(since + HELD_NATIVE_WINDOW)
    );
    tokio::time::advance(HELD_NATIVE_WINDOW + Duration::from_millis(1)).await;
    let held_id = h.mgr.held_native[&addr].connection_id;
    let survivors = survivor_here();
    h.mgr.on_native_first_frame(addr, held_id);
    assert!(h.mgr.held_native.is_empty(), "expired, not promoted");
    assert!(!h.mgr.connections[&addr].native, "UDP stays");
    assert_eq!(survivor_here(), survivors);
    assert_eq!(h.mgr.held_native_deadline(), None);
    // The loop's wake deadline includes it.
    const LOOP: &str = include_str!("../../p2p_protoc.rs");
    assert!(
        LOOP.contains("ctx.held_native_deadline()"),
        "the event loop wakes for the held-native deadline"
    );
}

/// The peer promoted its native connection on our probe and closed UDP; our
/// UDP entry's close arrives (in either order against the native frame) while
/// a native connection is held: it becomes the entry, the ring is untouched.
#[tokio::test]
async fn a_closed_udp_entry_hands_over_to_the_held_native_connection() {
    let mut h = Harness::new("hold-udp-closes").await;
    let addr: SocketAddr = "203.0.113.235:6560".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    let udp_id = h.mgr.connections[&addr].connection_id;
    h.handshake_event(Arrival::Native.event(&peer, false)).await;
    let held_id = h.mgr.held_native[&addr].connection_id;
    let pair = PairState::of(&h, addr);
    close_connection(&mut h, addr, udp_id).await;
    let entry = &h.mgr.connections[&addr];
    assert!(entry.native, "the held native connection is the entry");
    assert_eq!(entry.connection_id, held_id);
    assert!(h.mgr.held_native.is_empty());
    assert_eq!(PairState::of(&h, addr), pair, "the peer stays connected");
    // If it is dead too, its own close removes the entry as usual.
    close_connection(&mut h, addr, held_id).await;
    assert!(!h.mgr.connections.contains_key(&addr));
}

/// At most one native connection is held per address: a second native
/// connection for it replaces the first (a re-dial means the old one is
/// likely stale).
#[tokio::test]
async fn a_newer_native_connection_replaces_the_held_one() {
    let mut h = Harness::new("hold-one").await;
    let addr: SocketAddr = "203.0.113.233:6540".parse().unwrap();
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, false))
        .await;
    h.handshake_event(Arrival::Native.event(&peer, false)).await;
    let first = h.mgr.held_native[&addr].connection_id;
    h.handshake_event(Arrival::Native.event(&peer, false)).await;
    assert_eq!(h.mgr.held_native.len(), 1);
    assert_ne!(h.mgr.held_native[&addr].connection_id, first, "the newer");
}

/// Gate 16: only a native setup's own end is carried to its requesters. A
/// native dial that fails locally before any setup runs (here the handshake
/// channel is closed) and a UDP dial's failure both answer `Err(None)`.
#[tokio::test]
async fn local_and_udp_failures_carry_no_native_outcome() {
    use crate::operations::connect::native::DialTransport;
    let mut h = Harness::new("native-outcome-local").await;
    let native_addr: SocketAddr = "203.0.113.250:6450".parse().unwrap();
    let udp_addr: SocketAddr = "203.0.113.250:6451".parse().unwrap();
    let mut udp = pending_connect_peer_over(
        &mut h,
        PeerKeyLocation::new(key(), udp_addr),
        DialTransport::Udp,
    )
    .await;
    h.handshake_event(HandshakeEvent::OutboundFailed {
        transaction: Transaction::new::<ConnectMsg>(),
        peer: PeerKeyLocation::new(key(), udp_addr),
        error: ConnectionError::Timeout,
        transient: false,
        transport: DialTransport::Udp,
        native_outcome: None,
        origin: crate::transport::NatAttemptOrigin::ConnectJoiner,
    })
    .await;
    assert_eq!(udp.try_recv(), Ok(Err(None)), "a UDP failure");
    drop(std::mem::replace(&mut h.handshake_rx, mpsc::channel(1).1));
    let mut native = pending_connect_peer_over(
        &mut h,
        PeerKeyLocation::new(key(), native_addr),
        DialTransport::native_now(),
    )
    .await;
    assert_eq!(
        native.try_recv(),
        Ok(Err(None)),
        "a local failure of a native dial"
    );
}

/// An inbound connection nobody expected, as the handshake driver reports
/// it: not transient, and without a peer identity.
async fn unexpected_inbound(h: &mut Harness, addr: SocketAddr) {
    h.handshake_event(HandshakeEvent::InboundConnection {
        transaction: None,
        matched: HandshakeInboundMatch::Unexpected,
        connection: Box::new(IdleConnection::new(addr)),
        transient: false,
    })
    .await;
}

fn transient_count(h: &Harness) -> usize {
    h.mgr
        .bridge
        .op_manager
        .ring
        .connection_manager
        .transient_count()
}

/// At the transient budget, an unexpected inbound connection closes the
/// oldest connection still waiting outside the ring and takes its slot, so a
/// host that fills the budget has to keep opening connections to keep it
/// full, rather than holding it and refusing every joiner.
#[tokio::test]
async fn unexpected_inbound_at_the_budget_evicts_the_oldest_unpromoted() {
    let mut h = Harness::with_config("transient-budget-evict-oldest", |config| {
        config.transient_budget = 2;
    })
    .await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let addr = |n: u8| SocketAddr::from(([198, 51, 100, 216], 7300 + u16::from(n)));
    unexpected_inbound(&mut h, addr(1)).await;
    unexpected_inbound(&mut h, addr(2)).await;
    assert_eq!(cm.transient_count(), 2, "premise: the budget is spent");

    unexpected_inbound(&mut h, addr(3)).await;
    assert!(
        !h.mgr.connections.contains_key(&addr(1)),
        "the oldest is closed"
    );
    assert!(!cm.is_transient(addr(1)));
    for kept in [addr(2), addr(3)] {
        assert!(h.mgr.connections.contains_key(&kept));
        assert!(cm.is_transient(kept));
    }
    assert_eq!(cm.transient_count(), 2);
    assert_eq!(cm.transient_evictions(), 1);

    unexpected_inbound(&mut h, addr(4)).await;
    assert!(
        !h.mgr.connections.contains_key(&addr(2)),
        "then the next oldest"
    );
    assert!(h.mgr.connections.contains_key(&addr(3)));
    assert!(h.mgr.connections.contains_key(&addr(4)));
    assert_eq!(cm.transient_evictions(), 2);
    h.assert_open_transports_match();
}

/// An unexpected inbound connection the budget refuses is closed, not kept
/// open and uncounted. Here every slot is held by a registration no open
/// connection owns, so there is nothing to evict.
#[tokio::test]
async fn unexpected_inbound_refused_by_the_budget_is_dropped() {
    let mut h = Harness::with_config("transient-budget-unexpected-inbound", |config| {
        config.transient_budget = 2;
    })
    .await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    for port in [7301, 7302] {
        assert!(cm.try_register_transient(SocketAddr::from(([198, 51, 100, 210], port)), None));
    }
    let addr = SocketAddr::from(([198, 51, 100, 210], 7303));
    unexpected_inbound(&mut h, addr).await;
    assert!(
        !h.mgr.connections.contains_key(&addr),
        "refused by the budget: dropped"
    );
    assert!(!cm.is_transient(addr));
    assert_eq!(h.ring_key_at(addr), None, "not in the ring");
    assert_eq!(cm.transient_count(), 2);
    assert_eq!(cm.transient_evictions(), 0);
    h.assert_open_transports_match();
}

/// The budget counts the connections open outside the ring, not those
/// younger than `transient_ttl`: an unpromoted connection keeps its slot past
/// the TTL (neither its expiry timer nor the periodic cleanup gives it back
/// while the transport is open), and gives it back when it is promoted.
#[tokio::test(start_paused = true)]
async fn an_unpromoted_connection_keeps_its_budget_slot_past_the_ttl() {
    let mut h = Harness::with_config("transient-budget-slot-held", |config| {
        config.transient_budget = 1;
    })
    .await;
    let first = SocketAddr::from(([198, 51, 100, 211], 7311));
    let second = SocketAddr::from(([198, 51, 100, 211], 7312));
    let third = SocketAddr::from(([198, 51, 100, 211], 7313));
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();

    unexpected_inbound(&mut h, first).await;
    assert!(h.mgr.connections.contains_key(&first));
    assert_eq!(cm.transient_count(), 1, "premise: the budget is spent");

    // Past the TTL: the expiry timer has run, and so has a cleanup.
    tokio::time::sleep(cm.transient_ttl() + Duration::from_secs(1)).await;
    assert_eq!(cm.cleanup_expired_transients(), 0);
    assert!(
        h.mgr.connections.contains_key(&first),
        "premise: still open"
    );
    assert!(cm.is_transient(first), "an open connection keeps its slot");
    // The slot is still spent, so the next connection has to evict it.
    unexpected_inbound(&mut h, second).await;
    assert!(h.mgr.connections.contains_key(&second));
    assert!(!h.mgr.connections.contains_key(&first));
    assert_eq!(cm.transient_evictions(), 1);

    // Promoting it gives the slot back.
    let peer = key();
    h.connect_peer(PeerKeyLocation::new(peer.clone(), second))
        .await;
    assert_eq!(h.ring_key_at(second).as_ref(), Some(&peer));
    assert_eq!(cm.transient_count(), 0);
    unexpected_inbound(&mut h, third).await;
    assert!(h.mgr.connections.contains_key(&third));
    assert_eq!(cm.transient_evictions(), 1, "a free slot needs no eviction");
}

/// Every path that closes an unpromoted connection gives its budget slot
/// back at once, so a held slot is never stranded after its connection.
#[tokio::test]
async fn closing_an_unpromoted_connection_gives_its_budget_slot_back() {
    for (i, teardown) in Teardown::ALL.into_iter().enumerate() {
        let mut h = Harness::with_config(&format!("transient-budget-close-{i}"), |config| {
            config.transient_budget = 1;
        })
        .await;
        let port = 7320 + 2 * i as u16;
        let held = SocketAddr::from(([198, 51, 100, 212], port));
        let next = SocketAddr::from(([198, 51, 100, 212], port + 1));
        unexpected_inbound(&mut h, held).await;
        assert!(h.mgr.connections.contains_key(&held));
        assert_eq!(transient_count(&h), 1, "{teardown:?}: premise");

        teardown.run(&mut h, held).await;
        assert_eq!(transient_count(&h), 0, "{teardown:?}: slot given back");

        unexpected_inbound(&mut h, next).await;
        assert!(
            h.mgr.connections.contains_key(&next),
            "{teardown:?}: the slot is free for a new connection"
        );
    }
}

/// A transient connection that takes the budget's last slot is kept: the
/// check before the insert asks for the slot this connection already holds,
/// rather than comparing the count, which already includes it, to the budget.
#[tokio::test]
async fn a_transient_connection_may_take_the_last_budget_slot() {
    let mut h = Harness::with_non_gateway_ring("transient-budget-last-slot").await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let budget = cm.transient_budget();
    for n in 0..budget - 1 {
        let filler = SocketAddr::from(([10, 99, (n >> 8) as u8, n as u8], 1));
        assert!(cm.try_register_transient(filler, None));
    }
    let last = SocketAddr::from(([198, 51, 100, 213], 7330));
    let peer = PeerKeyLocation::new(key(), last);
    // `establish` asserts the connection is kept.
    h.establish(last, Some(peer), true).await;
    assert!(cm.is_transient(last));
    assert_eq!(cm.transient_count(), budget);
}

/// One address cannot hold the whole transient budget: past its share its
/// unexpected inbound connections are closed and counted, a second address
/// is still admitted, and a connection the address promotes gives its share
/// slot back.
#[tokio::test]
async fn one_address_saturating_its_transient_share_leaves_others_admitted() {
    let mut h = Harness::with_config("transient-budget-source-share", |config| {
        config.transient_budget = 64;
    })
    .await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let share = cm.transient_source_share();
    assert!(
        share < cm.transient_budget(),
        "premise: the share binds first"
    );
    let a = |port: u16| SocketAddr::from(([198, 51, 100, 214], port));

    for port in 0..share as u16 {
        unexpected_inbound(&mut h, a(7400 + port)).await;
        assert!(h.mgr.connections.contains_key(&a(7400 + port)));
    }
    let over = a(7400 + share as u16);
    unexpected_inbound(&mut h, over).await;
    assert!(
        !h.mgr.connections.contains_key(&over),
        "past its share: closed"
    );
    assert_eq!(cm.transient_source_refusals(), 1, "and counted");
    assert_eq!(cm.transient_count(), share);
    h.assert_open_transports_match();

    let other = SocketAddr::from(([198, 51, 100, 215], 7400));
    unexpected_inbound(&mut h, other).await;
    assert!(
        h.mgr.connections.contains_key(&other),
        "a second address is admitted"
    );

    // Promoting one of the first address's connections frees a share slot.
    h.connect_peer(PeerKeyLocation::new(key(), a(7400))).await;
    let again = a(7400 + share as u16 + 1);
    unexpected_inbound(&mut h, again).await;
    assert!(h.mgr.connections.contains_key(&again));
}

/// A ConnectPeer the ring refuses at its cap leaves the connection holding
/// its transient slot: the slot is given back only when the ring takes it.
#[tokio::test]
async fn a_promotion_refused_at_the_cap_keeps_the_transient_slot() {
    let mut h = Harness::with_config("transient-budget-refused-promotion", |config| {
        config.min_number_conn = Some(1);
        config.max_number_conn = Some(2);
    })
    .await;
    for port in [7341, 7342] {
        h.dial(SocketAddr::from(([198, 51, 100, 217], port)), &key())
            .await;
    }
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let remote = SocketAddr::from(([198, 51, 100, 217], 7343));
    unexpected_inbound(&mut h, remote).await;
    assert_eq!(cm.transient_count(), 1, "premise");

    assert!(
        !h.try_connect_peer(PeerKeyLocation::new(key(), remote))
            .await
    );
    assert!(h.mgr.connections.contains_key(&remote), "kept open");
    assert!(cm.is_transient(remote), "and counted");
    assert_eq!(cm.transient_count(), 1);
}

/// A refused promotion of a connection that holds no transient slot, when
/// none can be had, closes the connection rather than keep it uncounted.
#[tokio::test]
async fn a_refused_promotion_without_a_slot_closes_the_connection() {
    let mut h = Harness::with_config("transient-budget-refused-uncounted", |config| {
        config.min_number_conn = Some(1);
        config.max_number_conn = Some(2);
        config.transient_budget = 1;
    })
    .await;
    for port in [7351, 7352] {
        h.dial(SocketAddr::from(([198, 51, 100, 218], port)), &key())
            .await;
    }
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let remote = SocketAddr::from(([198, 51, 100, 218], 7353));
    unexpected_inbound(&mut h, remote).await;
    // An open connection outside the ring with no slot, and none free.
    assert!(cm.drop_transient(remote).is_some());
    assert!(cm.try_register_transient(SocketAddr::from(([198, 51, 100, 219], 1)), None));

    assert!(
        !h.try_connect_peer(PeerKeyLocation::new(key(), remote))
            .await
    );
    assert!(!h.mgr.connections.contains_key(&remote), "closed");
    h.assert_open_transports_match();
}

/// A transient connection kept outside the ring on a node that is not a
/// gateway (a dial marked transient) holds its slot past the TTL too.
#[tokio::test(start_paused = true)]
async fn an_unpromoted_transient_dial_holds_its_slot_past_the_ttl() {
    let mut h = Harness::with_non_gateway_ring("transient-budget-dial-held").await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let addr = SocketAddr::from(([198, 51, 100, 220], 7360));
    h.establish(addr, Some(PeerKeyLocation::new(key(), addr)), true)
        .await;
    assert_eq!(h.ring_key_at(addr), None, "premise: not promoted");
    tokio::time::sleep(cm.transient_ttl() + Duration::from_secs(1)).await;
    assert_eq!(cm.cleanup_expired_transients(), 0);
    assert!(cm.is_transient(addr));
}

/// End to end, on the simulated network: every node outside a NAT listens on
/// `::1`, so a gateway sees all its joiners come from one source. Runs
/// `nodes` joiners against one gateway with transient budget `budget` and
/// returns the gateway's evictions and the nodes that ended with no ring
/// connection.
fn join_behind_one_address(
    name: &'static str,
    seed: u64,
    nodes: usize,
    budget: usize,
) -> (u64, Vec<crate::node::testing_impl::NodeLabel>) {
    use crate::node::testing_impl::{NodeLabel, SimNetwork};
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut sim = rt.block_on(SimNetwork::new(name, 1, nodes, 7, 3, 10, 2, seed));
    sim.with_start_backoff(Duration::ZERO);
    sim.with_transient_budget(budget);
    let result = sim.run_controlled_simulation(
        seed,
        Vec::new(),
        Duration::from_secs(600),
        Duration::from_secs(300),
    );
    assert!(
        result.turmoil_result.is_ok(),
        "[{name}] simulation must complete: {:?}",
        result.turmoil_result.err()
    );
    let gateway = NodeLabel::gateway(name, 0);
    let cm = &result
        .node_rings
        .get(&gateway)
        .expect("the gateway started")
        .connection_manager;
    let mut unconnected: Vec<NodeLabel> = result
        .node_rings
        .iter()
        .filter(|(label, ring)| {
            !label.is_gateway() && ring.connection_manager.connection_count() == 0
        })
        .map(|(label, _)| label.clone())
        .collect();
    unconnected.sort_by_key(|label| label.to_string());
    assert_eq!(
        result.node_rings.len(),
        nodes + 1,
        "[{name}] every node started"
    );
    (cm.transient_evictions(), unconnected)
}

/// Many joiners from one address against a gateway with a small transient
/// budget (8, below the per-source share): a joiner at the budget evicts the
/// oldest unpromoted connection, and every joiner still joins the ring.
///
/// The share itself is not reached here: the gateway's start-up admission
/// ramp (5 handshakes a second) keeps far fewer than 16 joiners unpromoted at
/// once. `one_address_saturating_its_transient_share_leaves_others_admitted`
/// covers it through the bridge's real handlers.
#[test]
fn joiners_behind_one_address_get_past_transient_eviction() {
    let (evicted, unconnected) =
        join_behind_one_address("transient-evict-one-address", 0x7A45_0002, 24, 8);
    assert!(
        evicted > 0,
        "the budget was never reached; the test would be vacuous"
    );
    assert!(unconnected.is_empty(), "never joined: {unconnected:?}");
}

/// A non-gateway node (so a transient dial stays out of the ring) with
/// transient budget `budget`.
async fn non_gateway_with_budget(id: &str, budget: usize) -> Harness {
    Harness::build(id, |config| config.transient_budget = budget, false).await
}

/// Closing the evicted connection can release a key that a pending claim on
/// the arriving connection names, which binds the claim and promotes the
/// arrival into the ring. The arrival is then not transient: it takes no
/// slot, and is not left both in the ring and in the transient map.
#[tokio::test]
async fn an_arrival_promoted_by_its_eviction_takes_no_transient_slot() {
    let mut h = non_gateway_with_budget("transient-evict-promotes-arrival", 1).await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let victim = SocketAddr::from(([198, 51, 100, 221], 7370));
    let arrival = SocketAddr::from(([198, 51, 100, 222], 7371));
    let peer = key();
    // An unpromoted transient dial holds the only slot and the key.
    h.handshake_event(
        Arrival::OutboundUdp.event(&PeerKeyLocation::new(peer.clone(), victim), true),
    )
    .await;
    assert!(
        cm.is_transient(victim),
        "premise: the victim holds the slot"
    );
    assert_eq!(h.ring_key_at(victim), None, "premise: not in the ring");

    // An inbound connection matched to the same key waits on that claim.
    h.handshake_event(
        Arrival::InboundUdpKeyed.event(&PeerKeyLocation::new(peer.clone(), arrival), false),
    )
    .await;

    assert!(
        !h.mgr.connections.contains_key(&victim),
        "the victim is evicted"
    );
    assert_eq!(cm.transient_evictions(), 1);
    assert_eq!(
        h.ring_key_at(arrival).as_ref(),
        Some(&peer),
        "premise: the released claim promoted the arrival"
    );
    assert!(!cm.is_transient(arrival), "a ring connection holds no slot");
    assert_eq!(cm.transient_count(), 0);
}

/// At the budget the victim is the oldest held entry of the source holding
/// the most slots, so a host that fills the budget mostly evicts its own
/// connections: here every arrival evicts one of the attacker's, oldest
/// first, and every honest joiner, though older, keeps its connection.
#[tokio::test]
async fn eviction_takes_from_the_source_holding_the_most_slots() {
    let mut h = Harness::with_config("transient-evict-largest-source", |config| {
        config.transient_budget = 32;
    })
    .await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    assert_eq!(cm.transient_source_share(), 16, "premise");
    let honest: Vec<SocketAddr> = (1..=17u8)
        .map(|n| SocketAddr::from(([198, 51, 101, n], 7380)))
        .collect();
    let attacker = |port: u16| SocketAddr::from(([198, 51, 102, 1], port));
    for addr in &honest {
        unexpected_inbound(&mut h, *addr).await;
    }
    for port in 0..15 {
        unexpected_inbound(&mut h, attacker(7400 + port)).await;
    }
    assert_eq!(cm.transient_count(), 32, "premise: the budget is spent");

    for n in 0..5u8 {
        let arrival = SocketAddr::from(([198, 51, 103, n + 1], 7390));
        unexpected_inbound(&mut h, arrival).await;
        assert!(h.mgr.connections.contains_key(&arrival), "admitted");
        let evicted = attacker(7400 + u16::from(n));
        assert!(
            !h.mgr.connections.contains_key(&evicted),
            "the attacker's oldest goes first ({evicted})"
        );
    }
    assert_eq!(cm.transient_evictions(), 5);
    for addr in &honest {
        assert!(h.mgr.connections.contains_key(addr), "honest {addr} kept");
    }
}

/// Eviction spares only a UDP entry with a native connection held beside it
/// (the hold lasts `HELD_NATIVE_WINDOW`), and takes a QUIC-native entry like
/// a UDP one, oldest first. So legs a few sources hold cannot make the pool
/// non-evictable: here the native entry is older than the plain one and goes
/// first, then the plain one, then the arrivals; the held pair, older than all
/// of them, is never the victim.
#[tokio::test]
async fn eviction_takes_native_entries_and_spares_only_a_held_pair() {
    let mut h = non_gateway_with_budget("transient-evict-native-legs", 3).await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let pair = SocketAddr::from(([198, 51, 100, 223], 7375));
    let native = SocketAddr::from(([198, 51, 100, 238], 7386));
    let plain = SocketAddr::from(([198, 51, 100, 239], 7387));
    let pair_peer = PeerKeyLocation::new(key(), pair);
    h.handshake_event(Arrival::OutboundUdp.event(&pair_peer, true))
        .await;
    h.handshake_event(Arrival::Native.event(&pair_peer, true))
        .await;
    h.handshake_event(Arrival::Native.event(&PeerKeyLocation::new(key(), native), true))
        .await;
    unexpected_inbound(&mut h, plain).await;
    assert!(
        h.mgr.held_native.contains_key(&pair),
        "premise: held beside"
    );
    assert!(h.mgr.connections[&native].native, "premise: a native entry");
    assert_eq!(cm.transient_count(), 3, "premise: the budget is spent");

    let first = SocketAddr::from(([198, 51, 100, 224], 7376));
    unexpected_inbound(&mut h, first).await;
    assert!(
        !h.mgr.connections.contains_key(&native),
        "the older native entry is evicted"
    );
    assert!(
        h.mgr.connections.contains_key(&plain),
        "the plain one stays"
    );
    assert!(h.mgr.connections.contains_key(&first), "arrival admitted");

    let second = SocketAddr::from(([198, 51, 100, 240], 7388));
    unexpected_inbound(&mut h, second).await;
    assert!(!h.mgr.connections.contains_key(&plain));
    let third = SocketAddr::from(([198, 51, 100, 241], 7389));
    unexpected_inbound(&mut h, third).await;
    assert!(!h.mgr.connections.contains_key(&first));
    assert!(h.mgr.connections.contains_key(&second));
    assert!(h.mgr.connections.contains_key(&third));
    assert!(h.mgr.connections.contains_key(&pair), "the pair stays");
    assert!(
        h.mgr.held_native.contains_key(&pair),
        "with its held native connection"
    );
    assert_eq!(cm.transient_evictions(), 3);
}

/// Callbacks queued for a UDP dial beside a native one are failed when the
/// transient budget closes that address by eviction, instead of waiting out
/// their own timeout; the entry is removed from `udp_beside_native` too.
#[tokio::test]
async fn an_evicted_address_fails_its_udp_beside_native_callbacks() {
    let mut h = non_gateway_with_budget("transient-evict-fails-beside", 1).await;
    let victim = SocketAddr::from(([198, 51, 100, 245], 7401));
    unexpected_inbound(&mut h, victim).await;
    let (callback, mut waiter) = mpsc::channel::<crate::message::ConnectResult>(1);
    h.state
        .udp_beside_native
        .entry(victim)
        .or_default()
        .push((Transaction::new::<ConnectMsg>(), Box::new(callback)));

    let arrival = SocketAddr::from(([198, 51, 100, 246], 7402));
    unexpected_inbound(&mut h, arrival).await;
    assert!(!h.mgr.connections.contains_key(&victim), "premise: evicted");
    assert!(h.mgr.connections.contains_key(&arrival), "arrival admitted");
    assert!(
        matches!(waiter.try_recv(), Ok(Err(_))),
        "the beside waiter is told"
    );
    assert!(!h.state.udp_beside_native.contains_key(&victim));
}

/// A connection the transient budget closes tells whoever still waits for a
/// connection to its address that it failed, instead of leaving them to their
/// timeout: here the close of a refused promotion that cannot be counted.
#[tokio::test]
async fn a_budget_close_fails_the_waiters_for_its_address() {
    let mut h = Harness::with_config("transient-budget-close-fails-waiters", |config| {
        config.min_number_conn = Some(1);
        config.max_number_conn = Some(2);
        config.transient_budget = 1;
    })
    .await;
    for port in [7395, 7396] {
        h.dial(SocketAddr::from(([198, 51, 100, 243], port)), &key())
            .await;
    }
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let remote = SocketAddr::from(([198, 51, 100, 243], 7397));
    unexpected_inbound(&mut h, remote).await;
    assert!(cm.drop_transient(remote).is_some());
    assert!(cm.try_register_transient(SocketAddr::from(([198, 51, 100, 244], 1)), None));
    let (callback, mut waiter) = mpsc::channel::<crate::message::ConnectResult>(1);
    h.state
        .awaiting_connection
        .entry(remote)
        .or_default()
        .push(Box::new(callback));
    let (beside_callback, mut beside_waiter) = mpsc::channel::<crate::message::ConnectResult>(1);
    h.state
        .udp_beside_native
        .entry(remote)
        .or_default()
        .push((Transaction::new::<ConnectMsg>(), Box::new(beside_callback)));

    assert!(
        !h.try_connect_peer(PeerKeyLocation::new(key(), remote))
            .await
    );
    assert!(!h.mgr.connections.contains_key(&remote), "premise: closed");
    assert!(
        matches!(waiter.try_recv(), Ok(Err(_))),
        "the waiter is told"
    );
    assert!(!h.state.awaiting_connection.contains_key(&remote));
    assert!(
        matches!(beside_waiter.try_recv(), Ok(Err(_))),
        "the beside waiter is told"
    );
    assert!(!h.state.udp_beside_native.contains_key(&remote));
}

/// The refused arrival's own address: with the budget spent and no victim to
/// evict, the arrival is dropped and the UDP dials queued beside a native one
/// for its address are failed, not left to their timeout.
#[tokio::test]
async fn a_refused_arrival_fails_its_udp_beside_native_callbacks() {
    let mut h = non_gateway_with_budget("transient-refused-fails-beside", 2).await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    for port in [7411, 7412] {
        assert!(cm.try_register_transient(SocketAddr::from(([198, 51, 100, 247], port)), None));
    }
    let addr = SocketAddr::from(([198, 51, 100, 248], 7413));
    let (callback, mut waiter) = mpsc::channel::<crate::message::ConnectResult>(1);
    h.state
        .udp_beside_native
        .entry(addr)
        .or_default()
        .push((Transaction::new::<ConnectMsg>(), Box::new(callback)));
    unexpected_inbound(&mut h, addr).await;
    assert!(!h.mgr.connections.contains_key(&addr), "premise: refused");
    assert!(
        matches!(waiter.try_recv(), Ok(Err(_))),
        "the beside waiter is told"
    );
    assert!(!h.state.udp_beside_native.contains_key(&addr));
}

/// (b) The same when the entry is closed because a promotion was refused at
/// the cap and no slot can be had for it: the held native connection goes
/// with it.
#[tokio::test]
async fn a_close_for_an_uncounted_refused_promotion_drops_the_held_native() {
    let mut h = Harness::build(
        "transient-refused-held-native",
        |config| {
            config.min_number_conn = Some(1);
            config.max_number_conn = Some(2);
            config.transient_budget = 1;
        },
        false,
    )
    .await;
    for port in [7377, 7378] {
        h.dial(SocketAddr::from(([198, 51, 100, 225], port)), &key())
            .await;
    }
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let addr = SocketAddr::from(([198, 51, 100, 226], 7379));
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, true))
        .await;
    h.handshake_event(Arrival::Native.event(&peer, true)).await;
    assert!(
        h.mgr.held_native.contains_key(&addr),
        "premise: held beside"
    );
    // The entry loses its slot and none is free: uncounted.
    assert!(cm.drop_transient(addr).is_some());
    assert!(cm.try_register_transient(SocketAddr::from(([198, 51, 100, 227], 1)), None));
    assert!(
        !h.try_connect_peer(peer.clone()).await,
        "refused at the cap"
    );
    assert!(!h.mgr.connections.contains_key(&addr), "closed");
    assert!(
        h.mgr.held_native.is_empty(),
        "with its held native connection"
    );
}

/// (c) Holding a native connection beside an entry takes no transient slot
/// and is never an eviction candidate of its own.
#[tokio::test]
async fn a_held_native_connection_takes_no_transient_slot() {
    let mut h = non_gateway_with_budget("transient-held-native-uncounted", 8).await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let addr = SocketAddr::from(([198, 51, 100, 231], 7383));
    let peer = PeerKeyLocation::new(key(), addr);
    h.handshake_event(Arrival::OutboundUdp.event(&peer, true))
        .await;
    let before = (cm.transient_count(), cm.oldest_held_transient(|_| false));
    h.handshake_event(Arrival::Native.event(&peer, true)).await;
    assert!(
        h.mgr.held_native.contains_key(&addr),
        "premise: held beside"
    );
    assert_eq!(
        (cm.transient_count(), cm.oldest_held_transient(|_| false)),
        before
    );
    assert_eq!(before, (1, Some(addr)), "only the entry's own slot");
}

/// (d) The address keeps exactly one slot when the held native connection
/// is swapped in on its first frame, and when it takes over after the UDP
/// entry closes.
#[tokio::test]
async fn a_swapped_in_native_connection_keeps_the_addresss_one_slot() {
    let mut h = non_gateway_with_budget("transient-held-native-swap", 8).await;
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let swapped = SocketAddr::from(([198, 51, 100, 232], 7384));
    let handed = SocketAddr::from(([198, 51, 100, 233], 7385));
    for addr in [swapped, handed] {
        let peer = PeerKeyLocation::new(key(), addr);
        h.handshake_event(Arrival::OutboundUdp.event(&peer, true))
            .await;
        h.handshake_event(Arrival::Native.event(&peer, true)).await;
    }
    assert_eq!(cm.transient_count(), 2, "premise: one slot each");

    let held_id = h.mgr.held_native[&swapped].connection_id;
    h.mgr.on_native_first_frame(swapped, held_id);
    assert!(h.mgr.connections[&swapped].native, "swapped in");
    assert!(cm.is_transient(swapped));

    let udp_id = h.mgr.connections[&handed].connection_id;
    close_connection(&mut h, handed, udp_id).await;
    assert!(h.mgr.connections[&handed].native, "handed over");
    assert!(cm.is_transient(handed));
    assert_eq!(cm.transient_count(), 2, "still one slot per address");
}

/// When a transient connection replaces one at the same address, the old
/// connection's slot is released and this one takes it again. If another
/// registration takes the freed slot first (here: releasing the old
/// connection's key binds a pending claim whose promotion is refused at the
/// cap, and the claimant is registered), the new connection is dropped, and
/// whoever waits for it is told it failed rather than left waiting.
#[tokio::test]
async fn a_replacement_that_cannot_retake_its_slot_fails_its_waiters() {
    let mut h = Harness::build(
        "transient-replacement-fails-waiters",
        |config| {
            config.min_number_conn = Some(1);
            config.max_number_conn = Some(2);
            config.transient_budget = 2;
        },
        false,
    )
    .await;
    for port in [7391, 7392] {
        h.dial(SocketAddr::from(([198, 51, 100, 234], port)), &key())
            .await;
    }
    let cm = h.mgr.bridge.op_manager.ring.connection_manager.clone();
    let addr = SocketAddr::from(([198, 51, 100, 235], 7393));
    let claimant = SocketAddr::from(([198, 51, 100, 236], 7394));
    let peer = key();
    // The old connection: an unpromoted transient dial holding `peer`.
    h.handshake_event(Arrival::OutboundUdp.event(&PeerKeyLocation::new(peer.clone(), addr), true))
        .await;
    // A connection whose claim of `peer` waits on it, with no slot of its own.
    h.handshake_event(
        Arrival::InboundUdpKeyed.event(&PeerKeyLocation::new(peer.clone(), claimant), false),
    )
    .await;
    assert!(cm.drop_transient(claimant).is_some(), "premise");
    assert!(cm.try_register_transient(SocketAddr::from(([198, 51, 100, 237], 1)), None));
    assert_eq!(cm.transient_count(), 2, "premise: the budget is spent");

    let (callback, mut result) = mpsc::channel::<crate::message::ConnectResult>(1);
    h.state
        .awaiting_connection
        .entry(addr)
        .or_default()
        .push(Box::new(callback));
    h.handshake_event(Arrival::OutboundUdp.event(&PeerKeyLocation::new(key(), addr), true))
        .await;

    assert!(
        cm.is_transient(claimant),
        "premise: the claimant took the slot"
    );
    assert!(
        !h.mgr.connections.contains_key(&addr),
        "the replacement is dropped"
    );
    assert!(
        matches!(result.try_recv(), Ok(Err(_))),
        "its waiter is told it failed"
    );
    assert!(!h.state.awaiting_connection.contains_key(&addr));
}
