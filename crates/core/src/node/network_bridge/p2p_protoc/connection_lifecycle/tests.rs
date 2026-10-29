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
    handshake_rx: mpsc::Receiver<HandshakeCommand>,
    _conn_events_rx: mpsc::Receiver<ConnEvent>,
}

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
            mgr,
            state: EventListenerState::new(Arc::new(ExpectedInboundTracker::empty_for_test())),
            handshake: HandshakeCommandSender::for_test(handshake_tx),
            handshake_rx,
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
            connection: Box::new(IdleConnection { addr }),
            transient: false,
        })
        .await;
        assert!(self.mgr.connections.contains_key(&addr), "connection kept");
    }

    /// A handshake event, handled by the event loop's handler.
    async fn handshake_event(&mut self, event: HandshakeEvent) {
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

/// A second ConnectRequest naming another key does not change the
/// connection's key: it counts `changed_connect_request`, the request is
/// dropped, and the connection is closed. Nothing is recorded under the new
/// key, and the first key's mapping goes with the connection.
#[tokio::test]
async fn connect_request_naming_another_key_closes_the_connection() {
    let mut h = Harness::new("conn-identity-changed-request").await;
    let remote: SocketAddr = "203.0.113.20:4201".parse().unwrap();
    watch(&[remote]);
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
    assert_eq!(h.mgr.connections[&remote].pub_key(), Some(&peer_key));
}

/// A ConnectPeer naming a key other than the connection's is refused: the
/// connect op is told it failed, the connection keeps its key and is not
/// promoted into the ring, and nothing is recorded under the other key.
#[tokio::test]
async fn connect_peer_naming_another_key_is_refused() {
    let mut h = Harness::new("conn-identity-changed-connect-peer").await;
    let remote: SocketAddr = "203.0.113.50:4501".parse().unwrap();
    watch(&[remote]);
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
    watch(&[remote]);
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
/// (`duplicate_key`), and the key maps to the second. When the first
/// connection later closes, its cleanup is for a key mapped to the second
/// (`teardown_key_elsewhere`): it leaves the mapping and the peer's state
/// alone. The second's close then removes both, and counts nothing.
#[tokio::test]
async fn second_connection_for_a_key_keeps_the_key_through_the_first_ones_close() {
    let mut h = Harness::new("conn-identity-duplicate").await;
    let old: SocketAddr = "203.0.113.70:4701".parse().unwrap();
    let new: SocketAddr = "203.0.113.70:4702".parse().unwrap();
    watch(&[old, new]);
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
        "the peer is still connected, so its state is kept"
    );

    h.transport_closed(new).await;
    assert_eq!(
        seen(new),
        vec![ConnIdentity::DuplicateKey, ConnIdentity::SetConnectRequest]
    );
    assert!(!h.mgr.addr_by_pub_key.contains_key(&peer_key));
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
    watch(&[by_connect_peer, by_request]);
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
    watch(&[remote]);

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
    watch(&[remote]);

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
    watch(&[in_order]);
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
    watch(&[remote]);

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
    watch(&[remote]);
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
        let connection = Box::new(IdleConnection { addr });
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
    watch(&[rig.peer_addr]);

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
    watch(&[rig.peer_addr]);
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
    watch(&[rig.peer_addr]);
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
    watch(&[peer_addr]);

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
        connection: Box::new(IdleConnection { addr }),
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
    let remote: SocketAddr = "203.0.113.150:5501".parse().unwrap();
    watch(&[remote]);
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
    watch(&[remote]);
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
    let remote: SocketAddr = "203.0.113.170:5701".parse().unwrap();
    watch(&[remote]);
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
    watch(&[dialled_at, claimed_at]);
    let peer = key();

    h.dial(dialled_at, &peer).await;
    h.accept_expected(claimed_at, &peer).await;

    assert_eq!(seen(claimed_at), vec![ConnIdentity::ClaimHeldByDial]);
    assert_eq!(h.mgr.connections[&claimed_at].pub_key(), None);
    assert!(h.mgr.connections[&claimed_at].identity.pending().is_some());
    assert_eq!(h.ring_key_at(claimed_at), None, "not promoted");
    assert_eq!(h.mgr.addr_by_pub_key.get(&peer), Some(&dialled_at));
    assert_eq!(h.ring_key_at(dialled_at).as_ref(), Some(&peer));

    h.transport_closed(dialled_at).await;
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
    watch(&[claimed_at]);
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
    watch(&[claim_y]);
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
    watch(&[first, second]);
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
    watch(&[claimed_at]);
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
    watch(&[claimed_at, dialled_at]);
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
    watch(&[remote_addr]);

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
    watch(&[remote_addr]);

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

/// Identity telemetry is watched per remote address, process-wide, so two
/// tests in this file using one address see each other's records when they
/// run in one process (plain `cargo test`). Each address literal here must
/// be unique.
#[test]
fn test_addresses_are_unique() {
    let source = include_str!("tests.rs");
    let mut seen = std::collections::BTreeSet::new();
    let mut duplicates = Vec::new();
    for prefix in ["\"203.0.113.", "\"198.51.100."] {
        for (at, _) in source.match_indices(prefix) {
            let rest = &source[at + 1..];
            let Some(end) = rest.find('"') else { continue };
            let literal = &rest[..end];
            if literal.contains(':') && !seen.insert(literal.to_string()) {
                duplicates.push(literal.to_string());
            }
        }
    }
    assert!(seen.len() > 20, "premise: the address literals were found");
    assert!(
        duplicates.is_empty(),
        "address literals used twice: {duplicates:?}"
    );
}
