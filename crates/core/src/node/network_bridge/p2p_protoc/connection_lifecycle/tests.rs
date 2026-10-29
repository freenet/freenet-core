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
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
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
        )
        .await
        .expect("build P2pConnManager");
        let (conn_events_tx, conn_events_rx) = mpsc::channel(64);
        mgr.conn_event_tx = Some(conn_events_tx);
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
        let (callback, mut result_rx) = mpsc::channel::<Result<(SocketAddr, Option<usize>), ()>>(1);
        self.mgr
            .handle_connect_peer(
                peer,
                Box::new(callback),
                Transaction::new::<ConnectMsg>(),
                &self.handshake,
                &mut self.state,
                true,
                crate::transport::NatAttemptOrigin::ConnectAcceptor,
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
    }

    /// A handshake event, handled by the event loop's handler.
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

fn seen(addr: SocketAddr) -> Vec<ConnIdentity> {
    TRANSPORT_METRICS.conn_identity_from(addr)
}

fn is_set(kind: ConnIdentity) -> bool {
    match kind {
        ConnIdentity::SetDialed
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
        | ConnIdentity::ClaimHeldByDial
        | ConnIdentity::QuietDialReplaced
        | ConnIdentity::OwnKey
        | ConnIdentity::DuplicateKey
        | ConnIdentity::TeardownKeyElsewhere
        | ConnIdentity::TeardownUnidentified
        | ConnIdentity::TeardownKeyUnmapped => false,
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

/// A peer that reappears at another address while its first (claimed)
/// connection is still open gets a second live connection for its key
/// (`duplicate_key`), and the key maps to the second, which the connect op
/// then promotes. When the first connection later closes, its cleanup is for
/// a key mapped to the second (`teardown_key_elsewhere`): it leaves the
/// mapping, and the peer's state, since the peer is still in the ring. The
/// second's close then removes both, and counts nothing.
#[tokio::test]
async fn second_connection_for_a_key_keeps_the_key_through_the_first_ones_close() {
    let mut h = Harness::new("conn-identity-duplicate").await;
    let old: SocketAddr = "203.0.113.70:4701".parse().unwrap();
    let new: SocketAddr = "203.0.113.70:4702".parse().unwrap();
    h.watch(&[old, new]);
    let peer_key = key();

    h.accept(old).await;
    h.connect_request(old, PeerKeyLocation::with_unknown_addr(peer_key.clone()))
        .await;
    h.accept(new).await;
    h.connect_request(new, PeerKeyLocation::with_unknown_addr(peer_key.clone()))
        .await;
    assert_eq!(
        seen(new),
        vec![ConnIdentity::DuplicateKey, ConnIdentity::SetConnectRequest]
    );
    // The connect op accepts the peer on the newer connection.
    h.connect_peer(PeerKeyLocation::new(peer_key.clone(), new))
        .await;
    assert_eq!(h.ring_addr_of(&peer_key), Some(new));

    let hosted = seed_neighbour_hosting(&h, &peer_key);

    h.transport_closed(old).await;
    assert_eq!(
        seen(old),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::TeardownKeyElsewhere
        ]
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer_key), Some(&new));
    assert!(
        neighbour_hosts(&h, &peer_key, &hosted),
        "the peer is still in the ring, so its state is kept"
    );

    h.transport_closed(new).await;
    assert_eq!(
        seen(new),
        vec![
            ConnIdentity::DuplicateKey,
            ConnIdentity::SetConnectRequest,
            ConnIdentity::SameConnectPeer
        ]
    );
    assert!(!h.mgr.addr_by_pub_key.contains_key(&peer_key));
    assert_eq!(h.ring_addr_of(&peer_key), None);
    assert!(
        !neighbour_hosts(&h, &peer_key, &hosted),
        "the last connection for the key took the peer's state with it"
    );

    // Closing the connection that does hold the mapping counts nothing.
    h.dial(old, &key()).await;
    h.transport_closed(old).await;
    assert_eq!(
        seen(old),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::TeardownKeyElsewhere,
            ConnIdentity::SetDialed,
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
) -> mpsc::Receiver<Result<(SocketAddr, Option<usize>), ()>> {
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
    async fn connect_peer(
        &self,
        h: &mut Harness,
    ) -> mpsc::Receiver<Result<(SocketAddr, Option<usize>), ()>> {
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
        matches!(result.try_recv(), Ok(Err(()))),
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

    assert_eq!(seen(peer_addr), vec![ConnIdentity::ClaimHeldByDial]);
    assert_eq!(h.mgr.connections[&peer_addr].pub_key(), None);
    assert_eq!(h.ring_key_at(peer_addr), None);
    assert_eq!(h.mgr.addr_by_pub_key.get(&rig.peer_key), Some(&dialled_at));

    h.transport_closed(dialled_at).await;
    assert_eq!(
        seen(peer_addr),
        vec![
            ConnIdentity::ClaimHeldByDial,
            ConnIdentity::PendingClaimBound
        ]
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

    rig.expect_peer(&h).await;
    let mut result = rig.connect_peer(&mut h).await;
    assert_eq!(rig.handle_next_event(&mut h).await, "outbound_established");
    assert!(matches!(result.try_recv(), Ok(Ok(_))), "the dial succeeded");
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

    assert_eq!(seen(claimed_at), vec![ConnIdentity::ClaimHeldByDial]);
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
        vec![
            ConnIdentity::ClaimHeldByDial,
            ConnIdentity::PendingClaimBound
        ]
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

/// A ConnectRequest claim held back by a dial is bound when the dial closes,
/// so the restarted peer's later requests are passed on; it is not promoted,
/// since a request never promotes.
#[tokio::test]
async fn connect_request_claim_held_by_a_dial_is_bound_when_the_dial_closes() {
    let mut h = Harness::new("conn-identity-request-held-released").await;
    let dialled_at: SocketAddr = "203.0.113.183:5804".parse().unwrap();
    let claimed_at: SocketAddr = "203.0.113.184:5805".parse().unwrap();
    h.watch(&[claimed_at]);
    let peer = key();

    h.dial(dialled_at, &peer).await;
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
            ConnIdentity::ClaimHeldByDial,
            ConnIdentity::PendingClaimBound,
            ConnIdentity::SameConnectRequest,
        ]
    );
}

/// Bug shape: a stale claimed connection closing must not remove the key
/// mapping of a dialled connection that now holds the key, nor the peer's
/// state, and a later claim elsewhere is still held back by the dial.
#[tokio::test]
async fn closing_a_superseded_claim_keeps_the_dialled_holder() {
    let mut h = Harness::new("conn-identity-stale-claim-close").await;
    let claim_x: SocketAddr = "203.0.113.185:5806".parse().unwrap();
    let dial_1: SocketAddr = "203.0.113.186:5807".parse().unwrap();
    let claim_y: SocketAddr = "203.0.113.187:5808".parse().unwrap();
    h.watch(&[claim_y]);
    let peer = key();

    h.accept_expected(claim_x, &peer).await;
    h.dial(dial_1, &peer).await;
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dial_1));
    let hosted = seed_neighbour_hosting(&h, &peer);

    h.transport_closed(claim_x).await;
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dial_1));
    assert!(
        neighbour_hosts(&h, &peer, &hosted),
        "the peer's state is kept"
    );
    assert_eq!(h.ring_key_at(dial_1).as_ref(), Some(&peer));
    assert_eq!(h.ring_addr_of(&peer), Some(dial_1));

    h.accept_expected(claim_y, &peer).await;
    assert_eq!(seen(claim_y), vec![ConnIdentity::ClaimHeldByDial]);
    assert_eq!(h.mgr.connections[&claim_y].pub_key(), None);
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dial_1));
}

/// The same through an explicit drop of the stale claim rather than a
/// transport close.
#[tokio::test]
async fn dropping_a_superseded_claim_keeps_the_dialled_holder() {
    let mut h = Harness::new("conn-identity-stale-claim-drop").await;
    let claim_x: SocketAddr = "203.0.113.188:5809".parse().unwrap();
    let dial_1: SocketAddr = "203.0.113.189:5810".parse().unwrap();
    let peer = key();

    h.accept_expected(claim_x, &peer).await;
    h.dial(dial_1, &peer).await;
    let hosted = seed_neighbour_hosting(&h, &peer);
    let handshake = h.handshake.clone();
    assert!(
        h.mgr
            .drop_connection_by_addr(
                claim_x,
                &handshake,
                crate::tracing::DisconnectReason::Pruned
            )
            .await
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dial_1));
    assert_eq!(h.ring_addr_of(&peer), Some(dial_1));
    assert!(neighbour_hosts(&h, &peer, &hosted));
}

/// Two dials of one key at two addresses: the second binds, counts
/// `duplicate_key`, and takes the key's mapping. When it closes, the mapping
/// returns to the first, which is still open.
#[tokio::test]
async fn second_dial_of_a_key_binds_and_takes_the_mapping() {
    let mut h = Harness::new("conn-identity-dial-dial").await;
    let first: SocketAddr = "203.0.113.190:5811".parse().unwrap();
    let second: SocketAddr = "203.0.113.191:5812".parse().unwrap();
    h.watch(&[first, second]);
    let peer = key();

    h.dial(first, &peer).await;
    h.dial(second, &peer).await;
    assert_eq!(seen(first), vec![ConnIdentity::SetDialed]);
    assert_eq!(
        seen(second),
        vec![ConnIdentity::DuplicateKey, ConnIdentity::SetDialed]
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&second));

    let hosted = seed_neighbour_hosting(&h, &peer);
    h.transport_closed(second).await;
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&first));
    assert_eq!(h.ring_addr_of(&peer), Some(first));
    assert!(neighbour_hosts(&h, &peer, &hosted));
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

    assert_eq!(seen(claimed_at), vec![ConnIdentity::ClaimHeldByDial]);
    assert!(h.mgr.connections.contains_key(&claimed_at), "left open");
    assert_eq!(h.mgr.connections[&claimed_at].pub_key(), None);
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dialled_at));
}

/// The other order: a dial of a key that a claimed connection holds still
/// binds, and the key's address moves to the dialled connection
/// (`duplicate_key`). The claimed connection keeps its own binding.
#[tokio::test]
async fn dial_of_a_key_held_by_a_claim_binds() {
    let mut h = Harness::new("conn-identity-dial-after-claim").await;
    let claimed_at: SocketAddr = "203.0.113.200:6001".parse().unwrap();
    let dialled_at: SocketAddr = "203.0.113.201:6002".parse().unwrap();
    h.watch(&[claimed_at, dialled_at]);
    let peer = key();

    h.accept_expected(claimed_at, &peer).await;
    h.dial(dialled_at, &peer).await;

    assert_eq!(seen(claimed_at), vec![ConnIdentity::ExpectedInboundBound]);
    assert_eq!(
        seen(dialled_at),
        vec![ConnIdentity::DuplicateKey, ConnIdentity::SetDialed]
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dialled_at));
    assert_eq!(h.mgr.connections[&claimed_at].pub_key(), Some(&peer));
    assert_eq!(bound_source(&h, dialled_at), Some(IdentitySource::Dialled));
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
    assert_eq!(seen(new), vec![ConnIdentity::ClaimHeldByDial]);
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
        vec![ConnIdentity::SetDialed, ConnIdentity::QuietDialReplaced]
    );
    assert_eq!(
        seen(new),
        vec![
            ConnIdentity::ClaimHeldByDial,
            ConnIdentity::PendingClaimBound
        ]
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
        vec![
            ConnIdentity::ClaimHeldByDial,
            ConnIdentity::PendingClaimBound
        ]
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

/// The dial closes while another connection, outside the ring, is bound to
/// the key by its own ConnectRequest, and a matched claim is pending: the key
/// moves to the claimed connection, the pending claim is bound and promoted,
/// and the peer's state is kept since the peer stays in the ring.
#[tokio::test]
async fn a_pending_claim_is_bound_when_the_key_moves_to_a_claimed_holder() {
    let mut h = Harness::new("conn-identity-moved-pending").await;
    let claimed: SocketAddr = "198.51.100.6:6001".parse().unwrap();
    let holder: SocketAddr = "198.51.100.6:6002".parse().unwrap();
    let pending: SocketAddr = "198.51.100.6:6003".parse().unwrap();
    let peer = key();

    h.accept(claimed).await;
    h.connect_request(claimed, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.dial(holder, &peer).await;
    let hosted = seed_neighbour_hosting(&h, &peer);
    h.accept_expected(pending, &peer).await;
    assert_eq!(h.pending_on(pending), Some(true));

    h.transport_closed(holder).await;
    assert_eq!(h.mgr.connections[&pending].pub_key(), Some(&peer));
    assert_eq!(h.ring_key_at(pending).as_ref(), Some(&peer));
    assert_eq!(h.ring_addr_of(&peer), Some(pending));
    assert!(neighbour_hosts(&h, &peer, &hosted));
}

/// The dial closes and the key moves to a connection outside the ring with
/// nothing pending: the peer has no ring connection left, so its state is
/// forgotten, as on a release.
#[tokio::test]
async fn moving_the_key_out_of_the_ring_forgets_the_peer_state() {
    let mut h = Harness::new("conn-identity-moved-out-of-ring").await;
    let claimed: SocketAddr = "198.51.100.7:6001".parse().unwrap();
    let holder: SocketAddr = "198.51.100.7:6002".parse().unwrap();
    let peer = key();

    h.accept(claimed).await;
    h.connect_request(claimed, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.dial(holder, &peer).await;
    let hosted = seed_neighbour_hosting(&h, &peer);
    h.transport_closed(holder).await;

    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&claimed));
    assert_eq!(h.ring_addr_of(&peer), None);
    assert!(!neighbour_hosts(&h, &peer, &hosted));
}

/// The dial closes and the key moves to another dialled connection that is
/// in the ring: the peer's state is kept, and a pending claim stays pending
/// while that dial holds the key.
#[tokio::test]
async fn moving_the_key_to_an_in_ring_dial_keeps_the_peer_state() {
    let mut h = Harness::new("conn-identity-moved-in-ring").await;
    let first: SocketAddr = "198.51.100.8:6001".parse().unwrap();
    let second: SocketAddr = "198.51.100.8:6002".parse().unwrap();
    let pending: SocketAddr = "198.51.100.8:6003".parse().unwrap();
    let peer = key();

    h.dial(first, &peer).await;
    h.dial(second, &peer).await;
    let hosted = seed_neighbour_hosting(&h, &peer);
    h.accept_expected(pending, &peer).await;
    h.transport_closed(second).await;

    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&first));
    assert_eq!(h.ring_addr_of(&peer), Some(first));
    assert!(neighbour_hosts(&h, &peer, &hosted));
    assert_eq!(h.pending_on(pending), Some(true));
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
    assert!(
        source.contains("ctx.retry_pending_claims(&handshake_cmd_sender).await;"),
        "the loop must run the retry"
    );
}

/// Several claims of one key wait on a dial that goes quiet: the retry
/// replaces the dial and binds and promotes only the newest claim. Later
/// passes bind one more claim each, outside the ring, since the newest keeps
/// the key in the ring.
#[tokio::test]
async fn the_retry_binds_one_claim_per_key_per_pass() {
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
    assert_eq!(h.pending_on(middle), Some(true));
    assert_eq!(h.pending_on(oldest), Some(true));
    assert_eq!(h.ring_addr_of(&peer), Some(newest));

    h.mgr.retry_pending_claims(&handshake).await;
    assert_eq!(h.mgr.connections[&middle].pub_key(), Some(&peer));
    assert_eq!(
        h.ring_key_at(middle),
        None,
        "not promoted beside the newest"
    );
    assert_eq!(h.pending_on(oldest), Some(true));
    assert_eq!(h.ring_addr_of(&peer), Some(newest));
}

/// A peer's own ConnectRequest on a new connection binds beside a connection
/// already bound to its key by a claim (a peer reconnecting while its old
/// connection is still open): claims are not held back by claims, and a
/// request never promotes, so the key stays in the ring at one address.
#[tokio::test]
async fn a_connect_request_binds_beside_a_claimed_holder() {
    let mut h = Harness::new("conn-identity-request-beside-claim").await;
    let old: SocketAddr = "198.51.100.24:6001".parse().unwrap();
    let new: SocketAddr = "198.51.100.24:6002".parse().unwrap();
    h.watch(&[new]);
    let peer = key();

    h.accept(old).await;
    h.connect_request(old, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.connect_peer(PeerKeyLocation::new(peer.clone(), old))
        .await;
    h.accept(new).await;
    h.connect_request(new, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(
        seen(new),
        vec![ConnIdentity::DuplicateKey, ConnIdentity::SetConnectRequest]
    );
    assert_eq!(h.mgr.connections[&new].pub_key(), Some(&peer));
    assert_eq!(h.ring_key_at(new), None);
    assert_eq!(h.ring_addr_of(&peer), Some(old));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&old));
}

/// A dial holding a key closes while a claimed connection keeps the key in
/// the ring and another claim is pending: the pending claim is bound, but not
/// promoted, and the peer's state is kept.
#[tokio::test]
async fn a_pending_claim_bound_beside_an_in_ring_claim_is_not_promoted() {
    let mut h = Harness::new("conn-identity-release-beside-claim").await;
    let claimed: SocketAddr = "198.51.100.25:6001".parse().unwrap();
    let dialled: SocketAddr = "198.51.100.25:6002".parse().unwrap();
    let pending: SocketAddr = "198.51.100.25:6003".parse().unwrap();
    let peer = key();

    h.accept(claimed).await;
    h.connect_request(claimed, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.connect_peer(PeerKeyLocation::new(peer.clone(), claimed))
        .await;
    h.dial(dialled, &peer).await;
    h.accept_expected(pending, &peer).await;
    assert_eq!(h.pending_on(pending), Some(true));
    let hosted = seed_neighbour_hosting(&h, &peer);

    h.transport_closed(dialled).await;
    assert_eq!(h.mgr.connections[&pending].pub_key(), Some(&peer));
    assert_eq!(h.ring_key_at(pending), None);
    assert_eq!(h.ring_addr_of(&peer), Some(claimed));
    assert!(neighbour_hosts(&h, &peer, &hosted));
}

/// An expected-inbound claim bound while another connection keeps its key
/// in the ring is not promoted on establishment.
#[tokio::test]
async fn an_expected_inbound_claim_is_not_promoted_beside_the_key_in_the_ring() {
    let mut h = Harness::new("conn-identity-matched-beside-ring").await;
    let in_ring: SocketAddr = "198.51.100.26:6001".parse().unwrap();
    let matched: SocketAddr = "198.51.100.26:6002".parse().unwrap();
    let peer = key();

    h.accept(in_ring).await;
    h.connect_request(in_ring, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.connect_peer(PeerKeyLocation::new(peer.clone(), in_ring))
        .await;
    h.accept_expected(matched, &peer).await;
    assert_eq!(h.mgr.connections[&matched].pub_key(), Some(&peer));
    assert_eq!(h.ring_key_at(matched), None);
    assert_eq!(h.ring_addr_of(&peer), Some(in_ring));
}

/// Two connections claim one key, and the one in the ring closes while the
/// other is outside it. Per-peer cleanup follows ring membership, whichever
/// connection the index pointed at: here the index points at the connection
/// outside the ring (it bound while neither was in the ring), so the closing
/// connection does not own it, and the peer's state must still go.
#[tokio::test]
async fn cleanup_follows_the_ring_when_the_index_points_outside_it() {
    let mut h = Harness::new("conn-identity-cleanup-index-outside").await;
    let in_ring: SocketAddr = "198.51.100.22:6001".parse().unwrap();
    let outside: SocketAddr = "198.51.100.22:6002".parse().unwrap();
    let peer = key();

    h.accept(in_ring).await;
    h.connect_request(in_ring, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.accept(outside).await;
    h.connect_request(outside, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&outside));
    // Promoted directly: a ConnectPeer resolves its target by key, so it
    // would promote the connection the index points at.
    assert_eq!(
        h.mgr
            .promote_existing_connection(in_ring, &peer, None)
            .await,
        Promotion::InRing
    );
    assert_eq!(h.ring_addr_of(&peer), Some(in_ring));
    let hosted = seed_neighbour_hosting(&h, &peer);

    h.transport_closed(in_ring).await;
    assert_eq!(h.ring_addr_of(&peer), None);
    assert!(
        !neighbour_hosts(&h, &peer, &hosted),
        "no connection for the peer is left in the ring"
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&outside));
}

/// The other binding order: the connection in the ring binds and is
/// promoted first, so a later claim outside the ring does not take the index
/// from it; closing the in-ring connection moves the index and forgets the
/// peer's state, and closing the outside one instead would keep it.
#[tokio::test]
async fn a_claim_outside_the_ring_does_not_take_the_index_from_one_in_it() {
    let mut h = Harness::new("conn-identity-index-stays-in-ring").await;
    let in_ring: SocketAddr = "198.51.100.23:6001".parse().unwrap();
    let outside: SocketAddr = "198.51.100.23:6002".parse().unwrap();
    let peer = key();

    h.accept(in_ring).await;
    h.connect_request(in_ring, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.connect_peer(PeerKeyLocation::new(peer.clone(), in_ring))
        .await;
    h.accept(outside).await;
    h.connect_request(outside, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&in_ring));
    let hosted = seed_neighbour_hosting(&h, &peer);

    h.transport_closed(outside).await;
    assert!(
        neighbour_hosts(&h, &peer, &hosted),
        "the peer stays in the ring through the other connection"
    );
    h.accept(outside).await;
    h.connect_request(outside, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.transport_closed(in_ring).await;
    assert!(!neighbour_hosts(&h, &peer, &hosted));
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&outside));
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
