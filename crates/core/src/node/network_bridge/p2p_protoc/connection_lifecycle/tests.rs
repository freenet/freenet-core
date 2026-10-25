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

/// A connection that never delivers anything and accepts every send.
struct IdleConnection {
    addr: SocketAddr,
}

impl PeerConnectionApi for IdleConnection {
    fn remote_addr(&self) -> SocketAddr {
        self.addr
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
    _handshake_rx: mpsc::Receiver<HandshakeCommand>,
    _conn_events_rx: mpsc::Receiver<ConnEvent>,
}

impl Harness {
    async fn new(id: &str) -> Self {
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
            mgr,
            state: EventListenerState::new(Arc::new(ExpectedInboundTracker::empty_for_test())),
            handshake: HandshakeCommandSender::for_test(handshake_tx),
            _handshake_rx: handshake_rx,
            _conn_events_rx: conn_events_rx,
        }
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
        self.mgr
            .handle_successful_connection(
                peer,
                Box::new(IdleConnection { addr }),
                &mut self.state,
                None,
                transient,
                &self.handshake,
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

    /// A ConnectRequest from the connection at `remote` naming `joiner`.
    async fn connect_request(&mut self, remote: SocketAddr, joiner: PeerKeyLocation) {
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
        assert!(
            matches!(result, EventResult::Event(_)),
            "message is passed on"
        );
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
    /// (`NodeEvent::ExpectPeerConnection`), as the handshake driver reports
    /// it: not transient, and with no peer identity.
    async fn accept_expected(&mut self, addr: SocketAddr) {
        self.mgr
            .handle_handshake_action(
                HandshakeEvent::InboundConnection {
                    transaction: None,
                    expected: true,
                    connection: Box::new(IdleConnection { addr }),
                    transient: false,
                },
                &mut self.state,
                &self.handshake,
            )
            .await
            .expect("handle_handshake_action");
        assert!(self.mgr.connections.contains_key(&addr), "connection kept");
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
                .all(|entry| entry.pub_key.as_ref() != Some(&own)),
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

fn watch(addrs: &[SocketAddr]) {
    for addr in addrs {
        TRANSPORT_METRICS.watch_conn_identity(*addr);
    }
}

fn seen(addr: SocketAddr) -> Vec<ConnIdentity> {
    TRANSPORT_METRICS.conn_identity_from(addr)
}

fn is_set(kind: ConnIdentity) -> bool {
    match kind {
        ConnIdentity::SetDialed
        | ConnIdentity::ExpectedInbound
        | ConnIdentity::SetConnectRequest
        | ConnIdentity::SetConnectPeer
        | ConnIdentity::SameConnectRequest
        | ConnIdentity::SameConnectPeer => true,
        ConnIdentity::ChangedConnectRequest
        | ConnIdentity::ConnectRequestKnownAddr
        | ConnIdentity::ChangedConnectPeer
        | ConnIdentity::ChangedRingPromotion
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
    watch(&[dialed, joiner]);
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

/// A second ConnectRequest naming another key replaces the connection's key
/// and counts `changed_connect_request`.
#[tokio::test]
async fn connect_request_naming_another_key_counts_changed() {
    let mut h = Harness::new("conn-identity-changed-request").await;
    let remote: SocketAddr = "203.0.113.20:4201".parse().unwrap();
    watch(&[remote]);
    let (first, second) = (key(), key());

    h.accept(remote).await;
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(first))
        .await;
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(second.clone()))
        .await;

    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::ChangedConnectRequest
        ]
    );
    assert_eq!(h.mgr.connections[&remote].pub_key.as_ref(), Some(&second));
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
    watch(&[remote]);

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
    watch(&[remote]);
    let peer_key = key();

    h.accept(remote).await;
    h.connect_peer(PeerKeyLocation::new(peer_key.clone(), remote))
        .await;

    assert_eq!(seen(remote), vec![ConnIdentity::SetConnectPeer]);
    assert_eq!(h.mgr.connections[&remote].pub_key.as_ref(), Some(&peer_key));
}

/// A ConnectPeer naming a key other than the connection's promotes the
/// connection into the ring under the ConnectPeer key while the connection
/// keeps its own: both decisions are counted.
#[tokio::test]
async fn connect_peer_naming_another_key_counts_changed_and_ring_promotion() {
    let mut h = Harness::new("conn-identity-changed-connect-peer").await;
    let remote: SocketAddr = "203.0.113.50:4501".parse().unwrap();
    watch(&[remote]);
    let (recorded, other) = (key(), key());

    h.accept(remote).await;
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(recorded.clone()))
        .await;
    h.connect_peer(PeerKeyLocation::new(other.clone(), remote))
        .await;

    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::ChangedRingPromotion,
            ConnIdentity::ChangedConnectPeer,
        ]
    );
    assert_eq!(
        h.mgr.connections[&remote].pub_key.as_ref(),
        Some(&recorded),
        "the connection keeps its key"
    );
    let ring_peer = h
        .mgr
        .bridge
        .op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(remote)
        .expect("promoted into the ring");
    assert_eq!(
        ring_peer.pub_key(),
        &other,
        "the ring took the ConnectPeer key"
    );
}

/// An inbound connection matched to an expected-inbound registration starts
/// without a peer identity, exactly like an unexpected one: no key on the
/// connection, nothing in the key-to-address map, nothing in the ring. Only
/// the match is counted.
#[tokio::test]
async fn expected_inbound_starts_without_a_peer_identity() {
    let mut h = Harness::new("conn-identity-expected-no-key").await;
    let remote: SocketAddr = "203.0.113.60:4601".parse().unwrap();
    watch(&[remote]);
    h.accept_expected(remote).await;

    assert_eq!(seen(remote), vec![ConnIdentity::ExpectedInbound]);
    assert_eq!(h.mgr.connections[&remote].pub_key, None);
    assert!(h.mgr.addr_by_pub_key.is_empty(), "nothing is mapped");
    assert_eq!(h.ring_key_at(remote), None, "not in the ring");
    h.assert_own_key_absent();

    // Its close is the close of an unidentified connection.
    h.transport_closed(remote).await;
    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::ExpectedInbound,
            ConnIdentity::TeardownUnidentified
        ]
    );
}

/// A peer that reappears at another address while its first connection is
/// still open gets a second live connection for its key (`duplicate_key`).
/// When the first connection later closes, its cleanup is for a key now
/// recorded for the second (`teardown_key_elsewhere`), and that removes the
/// mapping, so the second's later close counts `teardown_key_unmapped`.
#[tokio::test]
async fn second_connection_for_a_key_counts_duplicate_then_both_teardowns() {
    let mut h = Harness::new("conn-identity-duplicate").await;
    let old: SocketAddr = "203.0.113.70:4701".parse().unwrap();
    let new: SocketAddr = "203.0.113.70:4702".parse().unwrap();
    watch(&[old, new]);
    let peer_key = key();

    h.dial(old, &peer_key).await;
    h.accept(new).await;
    h.connect_request(new, PeerKeyLocation::with_unknown_addr(peer_key.clone()))
        .await;
    assert_eq!(
        seen(new),
        vec![ConnIdentity::SetConnectRequest, ConnIdentity::DuplicateKey]
    );

    h.transport_closed(old).await;
    assert_eq!(
        seen(old),
        vec![ConnIdentity::SetDialed, ConnIdentity::TeardownKeyElsewhere]
    );

    // The older connection's cleanup removed the key's mapping, so the
    // newer one's close finds it unmapped: a different kind, not a second
    // `teardown_key_elsewhere`.
    h.transport_closed(new).await;
    assert_eq!(
        seen(new),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::DuplicateKey,
            ConnIdentity::TeardownKeyUnmapped,
        ]
    );

    // Closing the connection that does hold the mapping counts nothing.
    h.dial(old, &key()).await;
    h.transport_closed(old).await;
    assert_eq!(
        seen(old),
        vec![
            ConnIdentity::SetDialed,
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
    watch(&[remote]);

    h.accept(remote).await;
    let handshake = h.handshake.clone();
    assert!(
        h.mgr
            .drop_connection_by_addr(remote, &handshake, crate::tracing::DisconnectReason::Pruned,)
            .await
    );

    assert_eq!(seen(remote), vec![ConnIdentity::TeardownUnidentified]);
}

/// The NAT path of a connect op registers an expected inbound connection
/// and then sends ConnectPeer with the peer's key; the peer may also send a
/// ConnectRequest, before or after. Whichever names the peer first gives
/// the connection its identity, the ConnectPeer promotes it into the ring
/// under the peer's key, and at no point is anything recorded or promoted
/// under this node's own key. Several such connections can be open at once.
#[tokio::test]
async fn expected_inbound_then_named_peer_is_identified_by_the_peer_key() {
    let mut h = Harness::new("conn-identity-expected-named").await;
    let by_connect_peer: SocketAddr = "203.0.113.90:4901".parse().unwrap();
    let by_request: SocketAddr = "203.0.113.91:4902".parse().unwrap();
    watch(&[by_connect_peer, by_request]);
    let (first, second) = (key(), key());

    h.accept_expected(by_connect_peer).await;
    h.accept_expected(by_request).await;
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
            ConnIdentity::ExpectedInbound,
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
            ConnIdentity::ExpectedInbound,
            ConnIdentity::SetConnectRequest,
            ConnIdentity::SameConnectPeer,
        ]
    );

    for (addr, peer_key) in [(by_connect_peer, &first), (by_request, &second)] {
        assert_eq!(h.mgr.connections[&addr].pub_key.as_ref(), Some(peer_key));
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
    watch(&[remote]);

    h.accept(remote).await;
    let own = h.own_key();
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(own))
        .await;

    assert_eq!(
        seen(remote),
        vec![ConnIdentity::SetConnectRequest, ConnIdentity::OwnKey]
    );
}

/// A ConnectPeer naming this node's own key counts `own_key` too.
#[tokio::test]
async fn connect_peer_naming_own_key_counts_own_key() {
    let mut h = Harness::new("conn-identity-own-key-connect-peer").await;
    let remote: SocketAddr = "203.0.113.110:5101".parse().unwrap();
    watch(&[remote]);

    h.accept(remote).await;
    let own = h.own_key();
    h.connect_peer(PeerKeyLocation::new(own, remote)).await;

    assert_eq!(
        seen(remote),
        vec![ConnIdentity::SetConnectPeer, ConnIdentity::OwnKey]
    );
}

/// The realistic NAT order: expected inbound, ConnectPeer naming the peer,
/// then the peer's own ConnectRequest. That is one first identity
/// (`set_connect_peer`) and one confirmation (`same_connect_request`); a
/// later relayed request naming another peer is a change, counted as one,
/// exactly as on any other connection.
#[tokio::test]
async fn expected_inbound_connect_peer_then_requests_counts_one_first_identity() {
    let mut h = Harness::new("conn-identity-expected-order").await;
    let in_order: SocketAddr = "203.0.113.130:5301".parse().unwrap();
    watch(&[in_order]);
    let (peer, third_party) = (key(), key());

    h.accept_expected(in_order).await;
    h.connect_peer(PeerKeyLocation::new(peer.clone(), in_order))
        .await;
    h.connect_request(in_order, PeerKeyLocation::with_unknown_addr(peer.clone()))
        .await;
    h.connect_request(
        in_order,
        PeerKeyLocation::with_unknown_addr(third_party.clone()),
    )
    .await;
    assert_eq!(
        seen(in_order),
        vec![
            ConnIdentity::ExpectedInbound,
            ConnIdentity::SetConnectPeer,
            ConnIdentity::SameConnectRequest,
            ConnIdentity::ChangedConnectRequest,
        ]
    );
    h.assert_own_key_absent();
}

/// An open expected-inbound connection holds no key, so it does not make a
/// later claim of this node's own key on another connection a duplicate:
/// that claim counts `own_key` alone, and a later claim of another key is a
/// change.
#[tokio::test]
async fn own_key_claim_beside_an_expected_inbound_connection_is_not_a_duplicate() {
    let mut h = Harness::new("conn-identity-own-key-beside-expected").await;
    let remote: SocketAddr = "203.0.113.140:5401".parse().unwrap();
    let expected: SocketAddr = "203.0.113.141:5402".parse().unwrap();
    watch(&[remote]);

    h.accept_expected(expected).await;
    h.accept(remote).await;
    let own = h.own_key();
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(own))
        .await;
    h.connect_request(remote, PeerKeyLocation::with_unknown_addr(key()))
        .await;

    assert_eq!(
        seen(remote),
        vec![
            ConnIdentity::SetConnectRequest,
            ConnIdentity::OwnKey,
            ConnIdentity::ChangedConnectRequest,
        ]
    );
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

/// End to end through the real handshake driver and a real (mock-socket)
/// transport: this node, a gateway, is told by a connect op to expect a
/// peer at its address; the peer's handshake reaches us before we dial it,
/// so the transport accepts it as an inbound connection and the driver
/// matches it to the expectation; then the connect op's ConnectPeer names
/// the peer. The connection must start with no identity, end with the
/// peer's real key in the connection entry, the key-to-address map and the
/// ring, and never hold this node's key.
///
/// The simulation never takes this path (its hole punches complete on the
/// dialling side), so this is the test that covers it. The watched
/// `expected_inbound` record is counted only when the driver reported the
/// connection as matched, so it proves the matched path ran.
#[tokio::test]
async fn expected_inbound_matched_by_the_handshake_driver_gets_the_peer_key() {
    use crate::transport::mock_transport::{
        PacketDropPolicy, create_mock_gateway, create_mock_peer,
    };
    use futures::StreamExt;

    let mut h = Harness::new("conn-identity-expected-driver").await;
    let channels: crate::transport::mock_transport::Channels = Default::default();
    let (gw_key, (gw_outbound, gw_accepted), gw_addr) =
        create_mock_gateway(PacketDropPolicy::ReceiveAll, channels.clone())
            .await
            .expect("mock gateway");
    let (peer_key, peer_outbound, peer_addr) =
        create_mock_peer(PacketDropPolicy::ReceiveAll, channels)
            .await
            .expect("mock peer");
    watch(&[peer_addr]);

    let (mut driver, commands) = HandshakeHandler::new(
        Box::new(MockAcceptor(gw_accepted)),
        Arc::new(gw_outbound),
        h.mgr.bridge.op_manager.ring.connection_manager.clone(),
        h.mgr.bridge.op_manager.ring.router.clone(),
        None,
        true,
        None,
    );

    // The connect op expects the peer, through the event loop's handler.
    P2pConnManager::handle_expect_peer_connection(peer_addr, &h.state, &commands);
    // Let the driver take the registration before the peer's handshake.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    // The peer dials us; we have not dialled it, so we accept it inbound.
    let dial = tokio::spawn(async move {
        let pending = OutboundConnector::connect(
            &peer_outbound,
            gw_key,
            gw_addr,
            crate::transport::NatAttemptOrigin::ConnectJoiner,
        )
        .await;
        tokio::time::timeout(Duration::from_secs(10), pending).await
    });
    let event = tokio::time::timeout(Duration::from_secs(10), driver.next())
        .await
        .expect("the driver reports the connection")
        .expect("the driver is running");
    h.mgr
        .handle_handshake_action(event, &mut h.state, &commands)
        .await
        .expect("handle_handshake_action");
    assert!(
        dial.await.expect("dial task").is_ok(),
        "the peer's dial completes"
    );

    assert_eq!(
        seen(peer_addr),
        vec![ConnIdentity::ExpectedInbound],
        "the driver matched the connection to the expectation"
    );
    assert_eq!(h.mgr.connections[&peer_addr].pub_key, None);
    assert_eq!(h.ring_key_at(peer_addr), None, "not in the ring yet");
    h.assert_own_key_absent();

    // The connect op names the peer.
    assert!(
        h.try_connect_peer(PeerKeyLocation::new(peer_key.clone(), peer_addr))
            .await,
        "ConnectPeer reuses the inbound connection"
    );
    assert_eq!(
        seen(peer_addr),
        vec![ConnIdentity::ExpectedInbound, ConnIdentity::SetConnectPeer]
    );
    assert_eq!(
        h.mgr.connections[&peer_addr].pub_key.as_ref(),
        Some(&peer_key)
    );
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer_key), Some(&peer_addr));
    assert_eq!(
        h.ring_key_at(peer_addr).as_ref(),
        Some(&peer_key),
        "promoted under the peer's key"
    );
    h.assert_own_key_absent();
}
