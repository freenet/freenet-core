use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use crate::config::{GlobalExecutor, GlobalRng, PCK_VERSION};
use crate::transport::crypto::TransportSecretKey;
use crate::transport::packet_data::UnknownEncryption;
use crate::transport::symmetric_message::OutboundConnection;
use crate::util::backoff::ExponentialBackoff;
use aes_gcm::{Aes128Gcm, KeyInit};
use dashmap::DashSet;
use futures::{
    Future, FutureExt, TryFutureExt,
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use recv_errors::{RecvErrorAction, RecvErrorLog, RecvErrorPause, RecvErrorTracker};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    task_local,
};
use tracing::{Instrument, span};
use version_cmp::PROTOC_VERSION;

use super::{
    Socket, SocketRead, TransportError, UdpError, UdpErrorKind,
    congestion_control::{CongestionControl, CongestionControlConfig},
    crypto::{TransportKeypair, TransportPublicKey},
    demux::DemuxSocket,
    global_bandwidth::GlobalBandwidthManager,
    packet_data::{
        MAX_PACKET_SIZE, MAX_RECV_PACKET_SIZE as RECV_BUF_SIZE, PacketData, SymmetricAES,
    },
    peer_connection::{InboundPacket, PeerConnection, RemoteConnection},
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
    token_bucket::TokenBucket,
};
use crate::simulation::{RealTime, TimeSource, VirtualTime};
use crate::transport::in_memory_socket::get_socket_virtual_time;
use crate::transport::metrics::NatAttemptOrigin;
use stray_intro::RecentTraversalFailures;

// Constants for interval increase
const INITIAL_INTERVAL: Duration = Duration::from_millis(50);

/// Capacity of the inbound packet channel from connection_handler to PeerConnection.
///
/// At FixedRate 10 Mbps (~892 packets/sec), 2048 packets provides ~2.3 seconds
/// of buffer headroom, up from ~1.1 seconds at capacity 1000.
const INBOUND_CHANNEL_CAPACITY: usize = 2048;

/// Minimum interval between asymmetric decryption attempts for the same address.
/// Prevents CPU exhaustion from attackers sending intro-sized packets.
/// Note: X25519 decryption is ~100x faster than RSA-2048, so this is very conservative.
const ASYM_DECRYPTION_RATE_LIMIT: Duration = Duration::from_secs(1);

/// Interval for cleaning up expired rate limit entries.
/// This prevents unbounded growth of the last_asym_attempt HashMap.
const RATE_LIMIT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Duration to keep connections in RecentlyClosed state.
/// Allows in-flight packets to drain without triggering asymmetric decryption.
const RECENTLY_CLOSED_DURATION: Duration = Duration::from_secs(2);

/// Gateway connection acceptance rate limiting after restart.
///
/// When a gateway restarts, all connected peers detect their connections are dead
/// and attempt to reconnect simultaneously ("thundering herd"). This overwhelms
/// the inbound channel buffers for inter-gateway links, triggering packet drops
/// that can cascade into a non-recovering overflow loop.
///
/// The rate limiter gradually increases the connection acceptance rate:
/// - Phase 1 (0-30s):   max 5 new connections/second
/// - Phase 2 (30s-2m):  max 20 new connections/second
/// - Phase 3 (>2m):     unlimited
const GW_RAMP_PHASE1_DURATION: Duration = Duration::from_secs(30);
const GW_RAMP_PHASE2_DURATION: Duration = Duration::from_secs(120);
const GW_RAMP_PHASE1_RATE: u64 = 5;
const GW_RAMP_PHASE2_RATE: u64 = 20;

/// Duration to keep outdated peer entries before cleanup.
/// Peers with incompatible protocol versions are ignored for this duration.
const OUTDATED_PEER_EXPIRY: Duration = Duration::from_secs(600);

/// Window used to sample the congestion controller's current rate when
/// initializing a token bucket. Must stay in sync with `MIN_BUCKET_CAPACITY_BYTES`:
/// the bucket holds exactly one window's worth of tokens at the initial rate
/// (`initial_rate / 10` == `initial_rate * TOKEN_BUCKET_WINDOW`).
const TOKEN_BUCKET_WINDOW: Duration = Duration::from_millis(100);

/// Minimum token bucket capacity in bytes (8 KiB floor).
///
/// Prevents the bucket from becoming so small that a single max-size packet
/// (~1200 bytes) cannot be admitted, stalling connections at very low rates.
/// 8 KiB (~6–7 max-size packets) provides enough headroom for burst handling
/// without allowing large uncontrolled bursts.
const MIN_BUCKET_CAPACITY_BYTES: usize = 8192;

pub type SerializedMessage = Vec<u8>;

type GatewayConnectionFuture<S, T> = BoxFuture<
    'static,
    Result<
        (
            RemoteConnection<S, T>,
            InboundRemoteConnection,
            PacketData<SymmetricAES>,
        ),
        TransportError,
    >,
>;

type TraverseNatFuture<S, T> =
    BoxFuture<'static, Result<(RemoteConnection<S, T>, InboundRemoteConnection), TransportError>>;

/// Maximum retries for socket binding in case of transient port conflicts.
/// This handles race conditions in test environments where ports are released
/// and rebound quickly.
const SOCKET_BIND_MAX_RETRIES: usize = 5;

/// Backoff configuration for socket bind retries.
/// Base delay of 50ms, max delay of 1s (exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms).
const SOCKET_BIND_BACKOFF: ExponentialBackoff =
    ExponentialBackoff::new(Duration::from_millis(50), Duration::from_secs(1));

/// Create a pair of connection handlers for managing transport connections.
///
/// This is the main entry point for establishing UDP connections with NAT traversal.
/// Returns an outbound handler (for initiating connections) and inbound handler
/// (for receiving connection notifications).
///
/// # Arguments
/// * `keypair` - X25519 keypair for this peer
/// * `listen_host` - IP address to bind to (default `::` for dual-stack on all interfaces)
/// * `listen_port` - UDP port to bind to
/// * `is_gateway` - Whether this peer acts as a gateway
/// * `bandwidth_limit` - Optional per-connection bandwidth limit in bytes/sec
/// * `global_bandwidth` - Optional shared bandwidth manager
/// * `ledbat_min_ssthresh` - Optional minimum slow-start threshold for LEDBAT
/// * `congestion_config` - Optional congestion control config (defaults to FixedRate)
#[allow(clippy::too_many_arguments)]
pub async fn create_connection_handler<S: Socket>(
    keypair: TransportKeypair,
    listen_host: IpAddr,
    listen_port: u16,
    is_gateway: bool,
    bandwidth_limit: Option<usize>,
    global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
    ledbat_min_ssthresh: Option<usize>,
    congestion_config: Option<CongestionControlConfig>,
    ack_version_floor_override: Option<(u8, u8, u16)>,
) -> Result<
    (
        OutboundConnectionHandler<DemuxSocket<S>>,
        InboundConnectionHandler<DemuxSocket<S>>,
        tokio::task::JoinHandle<TransportError>,
    ),
    TransportError,
> {
    bind_udp_listener::<S>(
        keypair,
        listen_host,
        listen_port,
        is_gateway,
        UdpTransportConfig {
            bandwidth_limit,
            global_bandwidth,
            ledbat_min_ssthresh,
            congestion_config,
            ack_version_floor_override,
            // `create_connection_handler` is the test and tool entry point;
            // the node opens the transport through `open_udp_transport`.
            quic: None,
        },
    )
    .await
}

/// The UDP transport's tuning knobs, passed by name so that two knobs of the
/// same type cannot be swapped by position.
#[derive(Default)]
pub(crate) struct UdpTransportConfig {
    /// Per-connection bandwidth limit in bytes/sec.
    pub(crate) bandwidth_limit: Option<usize>,
    /// Shared bandwidth manager.
    pub(crate) global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
    /// Minimum slow-start threshold for LEDBAT.
    pub(crate) ledbat_min_ssthresh: Option<usize>,
    /// Congestion control config (defaults to FixedRate).
    pub(crate) congestion_config: Option<CongestionControlConfig>,
    /// The #5161 version-carrying-ack floor override; `None` in production.
    pub(crate) ack_version_floor_override: Option<(u8, u8, u16)>,
    /// QUIC endpoint settings when `transport-quic` is on, `None` (no
    /// endpoint, no QUIC ingress) when it is off. See `transport::quic`.
    pub(crate) quic: Option<super::quic::QuicSettings>,
}

pub(super) type UdpListenerParts<S> = (
    OutboundConnectionHandler<S>,
    InboundConnectionHandler<S>,
    tokio::task::JoinHandle<TransportError>,
);

/// Bind the UDP socket and start its listener.
///
/// This is the one place the transport binds its socket, for the real UDP
/// sockets and `SimulationSocket` alike, so it is where the socket is wrapped
/// in [`DemuxSocket`]: everything the listener receives has passed through the
/// first-byte split.
pub(super) async fn bind_udp_listener<S: Socket>(
    keypair: TransportKeypair,
    listen_host: IpAddr,
    listen_port: u16,
    is_gateway: bool,
    config: UdpTransportConfig,
) -> Result<UdpListenerParts<DemuxSocket<S>>, TransportError> {
    // Bind the UDP socket to the specified port with retry for transient failures
    let bind_addr: SocketAddr = (listen_host, listen_port).into();
    tracing::debug!(
        target: "freenet_core::transport::send_debug",
        bind_addr = %bind_addr,
        is_gateway,
        "Binding UDP socket"
    );

    // The node-wide egress cap exists only when `total-bandwidth-limit` is
    // set (which is when `global_bandwidth` is), so a node without it pays
    // nothing. The cap uses the same clock kind as the listener built below
    // (`RealTime`); waiters sleep on their own connection's time source.
    // That holds in simulation too: the listener and its connections run on
    // `RealTime` there, so a cap on the network's `VirtualTime` would compute
    // deficits on one clock for stream senders that sleep and time out on
    // another. A sender on `VirtualTime` (the QUIC endpoint, #5940) must use
    // the cap only once the cap and its other waiters share that clock.
    let egress_cap = config.global_bandwidth.as_ref().and_then(|manager| {
        super::egress_cap::EgressCap::from_total_limit(manager.total_limit(), RealTime::new())
    });
    let socket = Arc::new(
        bind_socket_with_retry::<DemuxSocket<S>>(bind_addr, SOCKET_BIND_MAX_RETRIES)
            .await?
            .with_egress_cap(egress_cap),
    );
    // With `transport-quic` off nothing is built and no QUIC ingress is
    // enabled, so QUIC-class datagrams stay `quic_ingress_no_ingress`. With
    // it on the endpoint runs on the egress cap's clock (`RealTime`, see
    // above), authenticates with this node's transport key, and has an
    // empty allow-set: nothing reaches it until an upgrade path allows a
    // peer.
    let quic_endpoint = config.quic.as_ref().and_then(|settings| {
        super::quic::QuicEndpoint::bind_or_warn(
            socket.clone(),
            bind_addr,
            RealTime::new(),
            settings,
            &keypair,
        )
    });

    tracing::info!(
        target: "freenet_core::transport::send_debug",
        bind_addr = %bind_addr,
        is_gateway,
        "UDP socket bound successfully"
    );
    let (och, new_connection_notifier, listen_handle) = OutboundConnectionHandler::config_listener(
        socket,
        keypair,
        is_gateway,
        (listen_host, listen_port).into(),
        config,
        quic_endpoint,
    )?;
    Ok((
        och,
        InboundConnectionHandler {
            new_connection_notifier,
        },
        listen_handle,
    ))
}

/// Open the UDP transport and return it as the node's [`TransportHandle`]
/// (`super::TransportHandle`). Binds the same way as
/// [`create_connection_handler`].
pub(crate) async fn open_udp_transport<S: Socket>(
    keypair: TransportKeypair,
    listen_host: IpAddr,
    listen_port: u16,
    is_gateway: bool,
    config: UdpTransportConfig,
) -> Result<super::TransportHandle, TransportError> {
    let (outbound, inbound, listen_task) =
        bind_udp_listener::<S>(keypair, listen_host, listen_port, is_gateway, config).await?;
    Ok(udp_transport_handle(outbound, inbound, listen_task))
}

/// Assemble the handle from the UDP transport's parts. Outbound connect and
/// expected-inbound registration share one expected-IP set: the tracker is a
/// view of the connector's set.
pub(crate) fn udp_transport_handle<S: Socket>(
    outbound: OutboundConnectionHandler<S>,
    inbound: InboundConnectionHandler<S>,
    listen_task: tokio::task::JoinHandle<TransportError>,
) -> super::TransportHandle {
    super::TransportHandle {
        expected_inbound: Arc::new(outbound.expected_inbound_tracker()),
        connector: Arc::new(outbound),
        acceptor: Box::new(inbound),
        listener: Box::pin(async move { super::ListenerExit::from_join(listen_task.await) }),
    }
}

/// Bind a socket with retry logic for transient "Address in use" errors.
///
/// In test environments with parallel tests, there can be a brief window between
/// releasing a reserved port and rebinding where another process grabs it.
/// This retry logic handles such transient conflicts gracefully.
async fn bind_socket_with_retry<S: Socket>(
    addr: SocketAddr,
    max_retries: usize,
) -> Result<S, TransportError> {
    let mut last_err = None;

    for attempt in 0..max_retries {
        match S::bind(addr).await {
            Ok(socket) => return Ok(socket),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && attempt + 1 < max_retries => {
                let delay = SOCKET_BIND_BACKOFF.delay(attempt as u32);
                tracing::debug!(
                    bind_addr = %addr,
                    attempt = attempt + 1,
                    max_retries,
                    delay_ms = delay.as_millis(),
                    "Socket bind failed with AddrInUse, retrying after delay"
                );
                last_err = Some(e);
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err(last_err.map(TransportError::from).unwrap_or_else(|| {
        TransportError::from(std::io::Error::from(std::io::ErrorKind::AddrInUse))
    }))
}

/// Receives  new inbound connections from the network.
pub struct InboundConnectionHandler<S = UdpSocket, TS: TimeSource = RealTime> {
    new_connection_notifier: mpsc::Receiver<PeerConnection<S, TS>>,
}

impl<S: Send + Sync, TS: TimeSource> InboundConnectionHandler<S, TS> {
    pub async fn next_connection(&mut self) -> Option<PeerConnection<S, TS>> {
        self.new_connection_notifier.recv().await
    }
}

impl<S: Socket> super::InboundAcceptor for InboundConnectionHandler<S> {
    fn next_connection(&mut self) -> super::NextConnection<'_> {
        // `mpsc::Receiver::recv` is cancel-safe, so the boxed future is too.
        Box::pin(async move {
            InboundConnectionHandler::<S>::next_connection(self)
                .await
                .map(|conn| Box::new(conn) as Box<dyn super::PeerConnectionApi>)
        })
    }
}

/// Requests a new outbound connection to a remote peer.
pub struct OutboundConnectionHandler<S = UdpSocket, TS: TimeSource = RealTime> {
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent<S, TS>)>,
    expected_non_gateway: Arc<DashSet<IpAddr>>,
    /// Whether this transport's QUIC endpoint is alive; `Disabled` when it
    /// has none (`transport-quic` off).
    quic_availability: super::quic::QuicAvailabilityWatch,
    /// The listener's QUIC endpoint, so tests can fail its driver.
    #[cfg(test)]
    quic_endpoint: Option<super::quic::QuicEndpoint>,
}

impl<S, TS: TimeSource> Clone for OutboundConnectionHandler<S, TS> {
    fn clone(&self) -> Self {
        Self {
            send_queue: self.send_queue.clone(),
            expected_non_gateway: self.expected_non_gateway.clone(),
            quic_availability: self.quic_availability.clone(),
            #[cfg(test)]
            quic_endpoint: self.quic_endpoint.clone(),
        }
    }
}

impl<S, TS: TimeSource> OutboundConnectionHandler<S, TS> {
    /// Whether this transport can use QUIC: `Disabled` with
    /// `transport-quic` off (no endpoint is built), else whether its
    /// endpoint is alive. e5 reads it before upgrading a connection.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn quic_availability(&self) -> super::quic::QuicAvailabilityWatch {
        self.quic_availability.clone()
    }

    /// The listener's QUIC endpoint, if it has one (tests).
    #[cfg(test)]
    pub(crate) fn quic_endpoint(&self) -> Option<&super::quic::QuicEndpoint> {
        self.quic_endpoint.as_ref()
    }
}

/// Return type for `config_listener`: the outbound handler, inbound connection receiver,
/// and the JoinHandle for the UDP listen task (must be monitored for unexpected exits).
type ListenerSetup<S> = (
    OutboundConnectionHandler<S>,
    mpsc::Receiver<PeerConnection<S>>,
    tokio::task::JoinHandle<TransportError>,
);

#[allow(private_bounds)]
impl<S: Socket> OutboundConnectionHandler<S> {
    fn config_listener(
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
        socket_addr: SocketAddr,
        config: UdpTransportConfig,
        quic_endpoint: Option<super::quic::QuicEndpoint>,
    ) -> Result<ListenerSetup<S>, TransportError> {
        let (transport, connection_handler, new_connection_notifier) = Self::new_listener(
            socket,
            keypair,
            is_gateway,
            socket_addr,
            config,
            quic_endpoint,
        );
        // Packets are now sent directly to socket from each connection,
        // bypassing the centralized rate limiter that was causing serialization bottlenecks.
        // Per-connection rate limiting is handled by TokenBucket and LEDBAT in RemoteConnection.
        let listen_handle = GlobalExecutor::spawn(RANDOM_U64.scope(
            {
                // Use GlobalRng for deterministic simulation
                let seed = GlobalRng::random_u64();
                let mut rng = StdRng::seed_from_u64(seed);
                rng.random()
            },
            transport.listen(),
        ));

        Ok((connection_handler, new_connection_notifier, listen_handle))
    }

    /// Build the listener (not yet running) and the handles onto it.
    #[allow(clippy::type_complexity)]
    fn new_listener(
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
        socket_addr: SocketAddr,
        config: UdpTransportConfig,
        quic_endpoint: Option<super::quic::QuicEndpoint>,
    ) -> (
        UdpPacketsListener<S>,
        OutboundConnectionHandler<S>,
        mpsc::Receiver<PeerConnection<S>>,
    ) {
        let UdpTransportConfig {
            bandwidth_limit,
            global_bandwidth,
            ledbat_min_ssthresh,
            congestion_config,
            ack_version_floor_override,
            // Used by `bind_udp_listener`, which built `quic_endpoint` from it.
            quic: _,
        } = config;
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(100);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(10);
        let expected_non_gateway = Arc::new(DashSet::new());
        let quic_availability = quic_endpoint.as_ref().map_or_else(
            super::quic::QuicAvailabilityWatch::disabled,
            super::quic::QuicEndpoint::availability,
        );
        #[cfg(test)]
        let test_quic_endpoint = quic_endpoint.clone();

        let time_source = RealTime::new();
        let transport = UdpPacketsListener {
            is_gateway,
            socket_listener: socket,
            this_peer_keypair: keypair,
            connections: ConnectionStateManager::new(time_source.clone()),
            connection_handler: conn_handler_receiver,
            new_connection_notifier: new_connection_sender,
            this_addr: socket_addr,
            dropped_packets: HashMap::new(),
            last_drop_warning_nanos: time_source.now_nanos(),
            bandwidth_limit,
            global_bandwidth,
            ledbat_min_ssthresh,
            expected_non_gateway: expected_non_gateway.clone(),
            recent_traversal_failures: RecentTraversalFailures::default(),
            // In simulation the admission ramp must advance with the network's
            // VirtualTime, not only the RealTime source this handler is built with
            // (see `GatewayConnectionRateLimiter::effective_elapsed_nanos`).
            // `get_socket_virtual_time` returns `Some` only when a SimNetwork has
            // registered a VirtualTime for this address, and `None` in production
            // (where the ramp stays on real wall-clock exactly as before).
            gw_rate_limiter: GatewayConnectionRateLimiter::new(
                time_source.clone(),
                get_socket_virtual_time(&socket_addr),
            ),
            time_source,
            congestion_config: Some(congestion_config.unwrap_or_default()),
            ack_version_floor_override,
            _quic_endpoint: quic_endpoint,
        };
        let connection_handler = OutboundConnectionHandler {
            send_queue: conn_handler_sender,
            expected_non_gateway,
            quic_availability,
            #[cfg(test)]
            quic_endpoint: test_quic_endpoint,
        };
        (transport, connection_handler, new_connection_notifier)
    }

    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn new_test(
        socket_addr: SocketAddr,
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
    ) -> Result<(Self, mpsc::Receiver<PeerConnection<S>>), TransportError> {
        Self::new_test_with_options(socket_addr, socket, keypair, is_gateway, None, None)
    }

    /// [`Self::new_test`] with every knob a mock peer can set, so no two of them
    /// are mutually exclusive.
    ///
    /// `ack_version_floor_override` is the #5161 gate: `Some(floor)` lets a test
    /// exercise `AckConnectionV2` before the crate version reaches
    /// `GATEWAY_ACK_VERSION_MIN_VERSION`, `None` is production behaviour.
    ///
    /// It takes `bandwidth_limit` too rather than hardcoding `None`. An earlier
    /// shape had the caller branch between this and a bandwidth-only
    /// constructor, which meant passing a floor override silently discarded any
    /// bandwidth limit — a trap that would have surfaced as a throughput test
    /// which mysteriously failed to throttle. That second constructor is gone;
    /// this is the one knob-bearing test constructor.
    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn new_test_with_options(
        socket_addr: SocketAddr,
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
        bandwidth_limit: Option<usize>,
        ack_version_floor_override: Option<(u8, u8, u16)>,
    ) -> Result<(Self, mpsc::Receiver<PeerConnection<S>>), TransportError> {
        let (handler, receiver, _listen_handle) = Self::config_listener(
            socket,
            keypair,
            is_gateway,
            socket_addr,
            UdpTransportConfig {
                bandwidth_limit,
                ack_version_floor_override,
                ..UdpTransportConfig::default()
            },
            None,
        )?;
        Ok((handler, receiver))
    }

    /// Connect with no node-level origin (tests, benchmarks); the attempt
    /// is tagged [`NatAttemptOrigin::RingPeerNoTransport`]. The node connects through
    /// [`super::OutboundConnector::connect`], which carries the real origin.
    pub async fn connect(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Result<PeerConnection<S>, TransportError>> + Send>> {
        self.connect_with_origin(
            remote_public_key,
            remote_addr,
            NatAttemptOrigin::RingPeerNoTransport,
        )
        .await
    }

    /// `OutboundConnector::connect` calls this on a clone, so it must only
    /// touch `Arc`-shared state.
    pub(crate) async fn connect_with_origin(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
        origin: NatAttemptOrigin,
    ) -> Pin<Box<dyn Future<Output = Result<PeerConnection<S>, TransportError>> + Send>> {
        if self.expected_non_gateway.insert(remote_addr.ip()) {
            tracing::debug!(
                peer_addr = %remote_addr,
                direction = "outbound",
                "Awaiting outbound handshake response from remote IP"
            );
        }
        let (open_connection, recv_connection) = oneshot::channel();
        if self
            .send_queue
            .send((
                remote_addr,
                ConnectionEvent::ConnectionStart {
                    remote_public_key,
                    open_connection,
                    origin,
                },
            ))
            .await
            .is_err()
        {
            return async { Err(TransportError::ChannelClosed) }.boxed();
        }
        recv_connection
            .map(|res| match res {
                Ok(Ok(remote_conn)) => Ok(PeerConnection::new(remote_conn)),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(TransportError::ConnectionEstablishmentFailure {
                    cause: "Failed to establish connection".into(),
                }),
            })
            .boxed()
    }

    pub fn expect_incoming(&self, remote_addr: SocketAddr) {
        if self.expected_non_gateway.insert(remote_addr.ip()) {
            tracing::debug!(
                peer_addr = %remote_addr,
                direction = "inbound",
                "Registered expected inbound handshake from remote IP"
            );
        }
    }

    /// Returns a clone of the expected inbound tracker.
    ///
    /// This is used by `EventListenerState` to register expected inbound connections
    /// without holding a reference to the entire `OutboundConnectionHandler`.
    pub fn expected_inbound_tracker(&self) -> ExpectedInboundTracker {
        ExpectedInboundTracker(self.expected_non_gateway.clone())
    }
}

impl<S: Socket> super::OutboundConnector for OutboundConnectionHandler<S> {
    // Yielding a future from an async block is the point: the first stage
    // queues the attempt and the second awaits the handshake (see the trait).
    #[allow(clippy::async_yields_async)]
    fn connect(
        &self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
        origin: NatAttemptOrigin,
    ) -> Pin<Box<dyn Future<Output = super::PendingConnection> + Send + '_>> {
        // The inherent `connect` takes `&mut self` but mutates nothing; a
        // clone shares the queue and the expected-IP set.
        let mut handler = self.clone();
        Box::pin(async move {
            let pending = OutboundConnectionHandler::<S>::connect_with_origin(
                &mut handler,
                remote_public_key,
                remote_addr,
                origin,
            )
            .await;
            let pending: super::PendingConnection = Box::pin(pending.map(|result| {
                result.map(|conn| Box::new(conn) as Box<dyn super::PeerConnectionApi>)
            }));
            pending
        })
    }
}

/// The UDP transport's expected-inbound IP set, shared with the outbound
/// handler that registers dialled IPs in the same set. The node reaches it
/// as `Arc<dyn InboundExpectations>`.
#[derive(Clone)]
pub struct ExpectedInboundTracker(Arc<DashSet<IpAddr>>);

impl ExpectedInboundTracker {
    /// Construct an empty tracker. Test-only; production code obtains one via
    /// `OutboundConnectionHandler::expected_inbound_tracker`.
    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Self {
        ExpectedInboundTracker(Arc::new(DashSet::new()))
    }

    /// Register an expected inbound connection from the given address.
    pub fn expect_incoming(&self, remote_addr: SocketAddr) {
        if self.0.insert(remote_addr.ip()) {
            tracing::debug!(
                peer_addr = %remote_addr,
                direction = "inbound",
                "Registered expected inbound handshake from remote IP"
            );
        }
    }
}

impl super::InboundExpectations for ExpectedInboundTracker {
    fn expect_incoming(&self, remote_addr: SocketAddr) {
        ExpectedInboundTracker::expect_incoming(self, remote_addr);
    }
}

// VirtualTime-specific impl block for instant benchmark simulation (test/bench only)
//
// This impl block provides methods for creating and using OutboundConnectionHandler with VirtualTime.
// By being specific to VirtualTime (not generic over TimeSource), it avoids conflicting with
// the OutboundConnectionHandler<S> impl which uses RealTime by default.
#[cfg(any(test, feature = "bench"))]
#[allow(private_bounds)]
impl<S: Socket> OutboundConnectionHandler<S, crate::simulation::VirtualTime> {
    /// Configure listener with VirtualTime for instant benchmark execution.
    ///
    /// The time_source is passed through to UdpPacketsListener, RemoteConnection,
    /// and PeerConnection so all timing operations use the same source.
    #[allow(clippy::too_many_arguments)]
    fn config_listener_with_virtual_time(
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
        socket_addr: SocketAddr,
        bandwidth_limit: Option<usize>,
        global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
        ledbat_min_ssthresh: Option<usize>,
        time_source: crate::simulation::VirtualTime,
        congestion_config: Option<CongestionControlConfig>,
    ) -> Result<
        (
            Self,
            mpsc::Receiver<PeerConnection<S, crate::simulation::VirtualTime>>,
        ),
        TransportError,
    > {
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(100);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(10);
        let expected_non_gateway = Arc::new(DashSet::new());

        let transport = UdpPacketsListener {
            is_gateway,
            socket_listener: socket,
            this_peer_keypair: keypair,
            connections: ConnectionStateManager::new(time_source.clone()),
            connection_handler: conn_handler_receiver,
            new_connection_notifier: new_connection_sender,
            this_addr: socket_addr,
            dropped_packets: HashMap::new(),
            last_drop_warning_nanos: time_source.now_nanos(),
            bandwidth_limit,
            global_bandwidth,
            ledbat_min_ssthresh,
            expected_non_gateway: expected_non_gateway.clone(),
            recent_traversal_failures: RecentTraversalFailures::default(),
            // This listener's `time_source` is already VirtualTime, so the admission
            // clock needs no override.
            gw_rate_limiter: GatewayConnectionRateLimiter::new(time_source.clone(), None),
            _quic_endpoint: None,
            time_source,
            congestion_config,
            // Benchmarks measure throughput, not handshake encoding; the real
            // floor applies here exactly as in production.
            ack_version_floor_override: None,
        };
        let connection_handler = OutboundConnectionHandler {
            send_queue: conn_handler_sender,
            expected_non_gateway,
            quic_availability: super::quic::QuicAvailabilityWatch::disabled(),
            #[cfg(test)]
            quic_endpoint: None,
        };

        // Packets are now sent directly to socket from each connection,
        // bypassing the centralized rate limiter that was causing serialization bottlenecks.
        // Per-connection rate limiting is handled by TokenBucket and LEDBAT in RemoteConnection.
        // Note: In test/bench code, the listen handle is intentionally dropped by callers
        // since mock sockets manage their own lifetimes.
        let _listen_handle = GlobalExecutor::spawn(RANDOM_U64.scope(
            {
                let seed = GlobalRng::random_u64();
                let mut rng = StdRng::seed_from_u64(seed);
                rng.random()
            },
            transport.listen(),
        ));

        Ok((connection_handler, new_connection_notifier))
    }

    /// Create a test connection handler with VirtualTime and custom congestion control.
    ///
    /// This allows benchmarks to test with different congestion control algorithms.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_test_with_congestion_config(
        socket_addr: SocketAddr,
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
        bandwidth_limit: Option<usize>,
        time_source: crate::simulation::VirtualTime,
        congestion_config: Option<CongestionControlConfig>,
    ) -> Result<
        (
            Self,
            mpsc::Receiver<PeerConnection<S, crate::simulation::VirtualTime>>,
        ),
        TransportError,
    > {
        Self::config_listener_with_virtual_time(
            socket,
            keypair,
            is_gateway,
            socket_addr,
            bandwidth_limit,
            None,
            None,
            time_source,
            congestion_config,
        )
    }

    /// Connect to a remote peer using VirtualTime. Tagged
    /// [`NatAttemptOrigin::RingPeerNoTransport`], like the real-time inherent `connect`.
    pub async fn connect(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        PeerConnection<S, crate::simulation::VirtualTime>,
                        TransportError,
                    >,
                > + Send,
        >,
    > {
        if self.expected_non_gateway.insert(remote_addr.ip()) {
            tracing::debug!(
                peer_addr = %remote_addr,
                direction = "outbound",
                "Awaiting outbound handshake response from remote IP"
            );
        }
        let (open_connection, recv_connection) = oneshot::channel();
        if self
            .send_queue
            .send((
                remote_addr,
                ConnectionEvent::ConnectionStart {
                    remote_public_key,
                    open_connection,
                    origin: NatAttemptOrigin::RingPeerNoTransport,
                },
            ))
            .await
            .is_err()
        {
            return async { Err(TransportError::ChannelClosed) }.boxed();
        }
        recv_connection
            .map(|res| match res {
                Ok(Ok(remote_conn)) => Ok(PeerConnection::new(remote_conn)),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(TransportError::ConnectionEstablishmentFailure {
                    cause: "Failed to establish connection".into(),
                }),
            })
            .boxed()
    }

    pub fn expect_incoming(&self, remote_addr: SocketAddr) {
        if self.expected_non_gateway.insert(remote_addr.ip()) {
            tracing::debug!(
                peer_addr = %remote_addr,
                direction = "inbound",
                "Registered expected inbound handshake from remote IP"
            );
        }
    }

    /// Returns a clone of the expected inbound tracker.
    pub fn expected_inbound_tracker(&self) -> ExpectedInboundTracker {
        ExpectedInboundTracker(self.expected_non_gateway.clone())
    }
}

/// Handles UDP transport internally.
struct UdpPacketsListener<S = UdpSocket, T: TimeSource = RealTime> {
    socket_listener: Arc<S>,
    /// Unified state manager for all remote connections.
    /// Ensures atomic state transitions during connection lifecycle
    /// (handshake → established → closed) to prevent race conditions.
    connections: ConnectionStateManager<S, T>,
    connection_handler: mpsc::Receiver<(SocketAddr, ConnectionEvent<S, T>)>,
    this_peer_keypair: TransportKeypair,
    is_gateway: bool,
    new_connection_notifier: mpsc::Sender<PeerConnection<S, T>>,
    this_addr: SocketAddr,
    /// Tracks dropped packet counts per remote address for congestion diagnostics.
    /// Used with `last_drop_warning_nanos` to rate-limit warning logs.
    dropped_packets: HashMap<SocketAddr, u64>,
    /// Timestamp (nanos) of last drop warning to prevent log spam.
    last_drop_warning_nanos: u64,
    /// Per-connection bandwidth limit in bytes/sec.
    /// For global fair-share limiting, use `global_bandwidth` instead.
    bandwidth_limit: Option<usize>,
    /// Global bandwidth manager for fair sharing across all connections.
    /// When set, per-connection rates are derived from total_limit / active_connections.
    global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
    /// Minimum ssthresh floor for LEDBAT timeout recovery.
    /// Prevents ssthresh death spiral on high-latency paths.
    ledbat_min_ssthresh: Option<usize>,
    /// IPs expected to connect as regular peers (not gateways).
    /// Used to validate connection attempts and prevent gateway impersonation.
    expected_non_gateway: Arc<DashSet<IpAddr>>,
    /// Outbound traversals that timed out (failure kind `NoResponse`,
    /// `Undecryptable` or `Incomplete`) in the last 10 s, to classify stray
    /// intros (telemetry only). Bounded by size and TTL.
    recent_traversal_failures: RecentTraversalFailures,
    time_source: T,
    /// Congestion control configuration for new connections.
    /// When None, uses the default configuration (BBR).
    congestion_config: Option<CongestionControlConfig>,
    /// Rate limiter for gateway connection acceptance after restart.
    /// Ramps up connection acceptance rate to prevent thundering herd.
    /// Only active when `is_gateway` is true.
    gw_rate_limiter: GatewayConnectionRateLimiter<T>,
    /// Test-only override for [`version_cmp::GATEWAY_ACK_VERSION_MIN_VERSION`]
    /// (#5161). `None` in production, where the real floor applies.
    ///
    /// Load-bearing rather than decorative, for the same reason
    /// `NodeConfig::hash_first_summaries_floor_override` is: simulations run
    /// the real transport handshake with the real crate version, which is
    /// BELOW the floor until the release that lifts it. Without an override
    /// the first integration-level run of this path would be the release PR
    /// itself, where a red sim could not be attributed between the feature and
    /// the version bump.
    ack_version_floor_override: Option<(u8, u8, u16)>,
    /// The QUIC endpoint on this listener's socket, when `transport-quic`
    /// is on. Held here so it lives as long as the listener reading the
    /// socket that feeds it. Nothing uses it yet (#5940): it accepts no
    /// connections and its allow-set is empty.
    _quic_endpoint: Option<super::quic::QuicEndpoint>,
}

type OngoingConnectionResult<S, T> = Option<
    Result<
        Result<
            (
                RemoteConnection<S, T>,
                InboundRemoteConnection,
                NatAttemptOrigin,
            ),
            (
                TransportError,
                SocketAddr,
                Arc<NatAttemptObservation>,
                NatAttemptOrigin,
            ),
        >,
        tokio::task::JoinError,
    >,
>;

type GwOngoingConnectionResult<S, T> = Option<
    Result<
        Result<
            (
                RemoteConnection<S, T>,
                InboundRemoteConnection,
                PacketData<SymmetricAES>,
            ),
            (TransportError, SocketAddr),
        >,
        tokio::task::JoinError,
    >,
>;

/// Unified connection state for a remote peer.
enum ConnectionState<S, T: TimeSource> {
    /// Gateway is performing asymmetric handshake with a connecting peer.
    GatewayHandshake {
        packet_sender: mpsc::Sender<PacketData<UnknownEncryption>>,
    },
    /// Outbound NAT traversal handshake in progress.
    NatTraversal {
        packet_sender: mpsc::Sender<PacketData<UnknownEncryption>>,
        result_sender: oneshot::Sender<Result<RemoteConnection<S, T>, TransportError>>,
    },
    /// Connection fully established with symmetric encryption.
    Established {
        inbound_packet_sender: mpsc::Sender<InboundPacket>,
    },
    /// Connection recently closed - drains in-flight packets.
    /// Allows new intro packets for reconnection.
    RecentlyClosed { closed_at_nanos: u64 },
}

/// Result of `ConnectionStateManager::try_send_nat_traversal` —
/// distinguishes a slow consumer from a vanished one so the listener
/// can log them differently.
enum TrySendNatTraversalOutcome {
    Sent,
    Full,
    Closed,
}

/// Manages connection state with atomic transitions.
/// Single source of truth for all connection states per address.
struct ConnectionStateManager<S, T: TimeSource> {
    states: BTreeMap<SocketAddr, ConnectionState<S, T>>,
    last_asym_attempt: HashMap<SocketAddr, u64>,
    time_source: T,
}

impl<S, T: TimeSource> ConnectionStateManager<S, T> {
    fn new(time_source: T) -> Self {
        Self {
            states: BTreeMap::new(),
            last_asym_attempt: HashMap::new(),
            time_source,
        }
    }

    /// Get immutable reference to current state for routing decisions.
    fn get_state(&self, addr: &SocketAddr) -> Option<&ConnectionState<S, T>> {
        self.states.get(addr)
    }

    /// Check if connection is established for the given address.
    fn is_established(&self, addr: &SocketAddr) -> bool {
        matches!(
            self.get_state(addr),
            Some(ConnectionState::Established { .. })
        )
    }

    /// Check if a gateway handshake is in progress for the given address.
    fn has_gateway_handshake(&self, addr: &SocketAddr) -> bool {
        matches!(
            self.get_state(addr),
            Some(ConnectionState::GatewayHandshake { .. })
        )
    }

    /// Check if a NAT traversal handshake is in progress for the given address.
    fn has_nat_traversal(&self, addr: &SocketAddr) -> bool {
        matches!(
            self.get_state(addr),
            Some(ConnectionState::NatTraversal { .. })
        )
    }

    /// Whether an outbound NAT traversal to any port of `ip` is in progress.
    /// A range scan over `ip`'s entries only (`SocketAddr` orders by IP
    /// first, then port), so cheap enough for the listener's drop path.
    fn has_nat_traversal_to_ip(&self, ip: IpAddr) -> bool {
        let (low, high) = match ip {
            IpAddr::V4(v4) => (SocketAddr::from((v4, 0)), SocketAddr::from((v4, u16::MAX))),
            // Bounds cover every flowinfo / scope id, whether or not the
            // ordering compares them.
            IpAddr::V6(v6) => (
                SocketAddr::V6(std::net::SocketAddrV6::new(v6, 0, 0, 0)),
                SocketAddr::V6(std::net::SocketAddrV6::new(
                    v6,
                    u16::MAX,
                    u32::MAX,
                    u32::MAX,
                )),
            ),
        };
        self.states
            .range(low..=high)
            .any(|(_, state)| matches!(state, ConnectionState::NatTraversal { .. }))
    }

    /// Try to send a packet to an established connection.
    /// Returns Ok(true) if sent, Ok(false) if channel full, Err if disconnected.
    fn try_send_established(
        &mut self,
        addr: &SocketAddr,
        packet: PacketData<UnknownEncryption>,
    ) -> Result<bool, ()> {
        if let Some(ConnectionState::Established {
            inbound_packet_sender,
        }) = self.states.get(addr)
        {
            // Stamped with the listener's clock, which its connections share,
            // so telemetry can tell when the packet arrived even if the
            // connection task takes it later (`InboundPacket`).
            let packet = InboundPacket {
                packet,
                queued_nanos: self.time_source.now_nanos(),
            };
            match inbound_packet_sender.try_send(packet) {
                Ok(()) => {
                    // Queue gauge (telemetry): two relaxed atomic reads on
                    // the channel's semaphore plus one fetch_max.
                    let depth = inbound_packet_sender
                        .max_capacity()
                        .saturating_sub(inbound_packet_sender.capacity());
                    super::TRANSPORT_METRICS
                        .record_inbound_queue_depth(depth.min(u32::MAX as usize) as u32);
                    Ok(true)
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    super::TRANSPORT_METRICS.record_inbound_queue_drop();
                    Ok(false)
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.mark_closed(addr);
                    Err(())
                }
            }
        } else {
            Err(())
        }
    }

    /// Try to send a packet to an ongoing gateway handshake.
    /// Returns Ok(true) if sent, Ok(false) if channel full/should drop.
    fn try_send_gateway_handshake(
        &self,
        addr: &SocketAddr,
        packet: PacketData<UnknownEncryption>,
    ) -> Result<bool, ()> {
        if let Some(ConnectionState::GatewayHandshake { packet_sender, .. }) = self.states.get(addr)
        {
            match packet_sender.try_send(packet) {
                Ok(()) => Ok(true),
                Err(mpsc::error::TrySendError::Full(_)) => Ok(false),
                Err(mpsc::error::TrySendError::Closed(_)) => Ok(false), // Handshake completing
            }
        } else {
            Err(())
        }
    }

    /// Try to send a packet to an ongoing NAT traversal, without blocking.
    ///
    /// Returns the outcome distinguishing `Sent` (delivered), `Full`
    /// (consumer not draining fast enough — usually transient), `Closed`
    /// (handshake task already exited — state about to be removed).
    /// Returns `Err(())` if no NAT traversal state exists for `addr`.
    ///
    /// Crucially this is non-blocking: the listener task calls this
    /// synchronously for every inbound packet, so a slow or wedged
    /// per-handshake consumer cannot stall the listener for other peers.
    /// Dropped intro packets are recoverable — `connect_outbound_inner`
    /// retries up to `NAT_TRAVERSAL_MAX_ATTEMPTS` (40 in release builds,
    /// 10 under `cfg(test)`) at a ~250 ms cadence (a 200 ms receive wait
    /// plus the 50 ms `INITIAL_INTERVAL` sleep), capped by the 3 s
    /// `overall_deadline`. So at production settings the remote peer
    /// fires roughly 12 intro packets during the deadline window — a
    /// transient channel-full event degrades latency but does not break
    /// the handshake.
    fn try_send_nat_traversal(
        &self,
        addr: &SocketAddr,
        packet: PacketData<UnknownEncryption>,
    ) -> Result<TrySendNatTraversalOutcome, ()> {
        if let Some(ConnectionState::NatTraversal { packet_sender, .. }) = self.states.get(addr) {
            match packet_sender.try_send(packet) {
                Ok(()) => Ok(TrySendNatTraversalOutcome::Sent),
                Err(mpsc::error::TrySendError::Full(_)) => Ok(TrySendNatTraversalOutcome::Full),
                Err(mpsc::error::TrySendError::Closed(_)) => Ok(TrySendNatTraversalOutcome::Closed),
            }
        } else {
            Err(())
        }
    }

    /// Start a gateway handshake. Only succeeds if no active state exists.
    fn start_gateway_handshake(
        &mut self,
        addr: SocketAddr,
        packet_sender: mpsc::Sender<PacketData<UnknownEncryption>>,
    ) -> bool {
        use std::collections::btree_map::Entry;
        let now = self.time_source.now_nanos();

        match self.states.entry(addr) {
            Entry::Vacant(entry) => {
                entry.insert(ConnectionState::GatewayHandshake { packet_sender });
                true
            }
            Entry::Occupied(mut entry) => {
                if let ConnectionState::RecentlyClosed { closed_at_nanos } = entry.get() {
                    if now.saturating_sub(*closed_at_nanos)
                        >= RECENTLY_CLOSED_DURATION.as_nanos() as u64
                    {
                        entry.insert(ConnectionState::GatewayHandshake { packet_sender });
                        return true;
                    } else {
                        tracing::debug!(
                            peer_addr = %addr,
                            remaining_ms = (RECENTLY_CLOSED_DURATION.as_nanos() as u64 - now.saturating_sub(*closed_at_nanos)) / 1_000_000,
                            "Rejecting gateway handshake - connection recently closed"
                        );
                    }
                }
                false
            }
        }
    }

    /// Complete gateway handshake atomically. Returns true if state was GatewayHandshake.
    fn complete_gateway_handshake(
        &mut self,
        addr: SocketAddr,
        inbound_packet_sender: mpsc::Sender<InboundPacket>,
    ) -> bool {
        use std::collections::btree_map::Entry;

        match self.states.entry(addr) {
            Entry::Occupied(mut entry) => {
                if matches!(entry.get(), ConnectionState::GatewayHandshake { .. }) {
                    entry.insert(ConnectionState::Established {
                        inbound_packet_sender,
                    });
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(_) => false,
        }
    }

    /// Fail a gateway handshake - remove state.
    fn fail_gateway_handshake(&mut self, addr: &SocketAddr) {
        if matches!(
            self.states.get(addr),
            Some(ConnectionState::GatewayHandshake { .. })
        ) {
            self.states.remove(addr);
        }
    }

    /// Start NAT traversal.
    fn start_nat_traversal(
        &mut self,
        addr: SocketAddr,
        packet_sender: mpsc::Sender<PacketData<UnknownEncryption>>,
        result_sender: oneshot::Sender<Result<RemoteConnection<S, T>, TransportError>>,
    ) -> bool {
        use std::collections::btree_map::Entry;
        let now = self.time_source.now_nanos();

        match self.states.entry(addr) {
            Entry::Vacant(entry) => {
                entry.insert(ConnectionState::NatTraversal {
                    packet_sender,
                    result_sender,
                });
                true
            }
            Entry::Occupied(mut entry) => {
                match entry.get() {
                    ConnectionState::Established { .. } => {
                        // Higher layer requested new connection, clear existing one
                        self.last_asym_attempt.remove(&addr);
                        tracing::info!(
                            peer_addr = %addr,
                            "Clearing existing connection to allow fresh NAT traversal"
                        );
                        entry.insert(ConnectionState::NatTraversal {
                            packet_sender,
                            result_sender,
                        });
                        true
                    }
                    ConnectionState::NatTraversal { .. } => {
                        // Already in progress, reject duplicate
                        false
                    }
                    ConnectionState::GatewayHandshake { .. } => {
                        // Don't overwrite ongoing gateway handshake to avoid orphaning its task
                        tracing::debug!(
                            peer_addr = %addr,
                            "Rejecting NAT traversal - gateway handshake already in progress"
                        );
                        false
                    }
                    ConnectionState::RecentlyClosed { closed_at_nanos } => {
                        // Allow if grace period expired
                        if now.saturating_sub(*closed_at_nanos)
                            >= RECENTLY_CLOSED_DURATION.as_nanos() as u64
                        {
                            entry.insert(ConnectionState::NatTraversal {
                                packet_sender,
                                result_sender,
                            });
                            true
                        } else {
                            tracing::debug!(
                                peer_addr = %addr,
                                remaining_ms = (RECENTLY_CLOSED_DURATION.as_nanos() as u64 - now.saturating_sub(*closed_at_nanos)) / 1_000_000,
                                "Rejecting NAT traversal - connection recently closed"
                            );
                            false
                        }
                    }
                }
            }
        }
    }

    /// Complete NAT traversal atomically. Returns result_sender if state was NatTraversal.
    fn complete_nat_traversal(
        &mut self,
        addr: SocketAddr,
        inbound_packet_sender: mpsc::Sender<InboundPacket>,
    ) -> Option<oneshot::Sender<Result<RemoteConnection<S, T>, TransportError>>> {
        use std::collections::btree_map::Entry;

        match self.states.entry(addr) {
            Entry::Occupied(mut entry) => {
                if matches!(entry.get(), ConnectionState::NatTraversal { .. }) {
                    let old = entry.insert(ConnectionState::Established {
                        inbound_packet_sender,
                    });
                    if let ConnectionState::NatTraversal { result_sender, .. } = old {
                        return Some(result_sender);
                    }
                }
                None
            }
            Entry::Vacant(_) => None,
        }
    }

    /// Fail NAT traversal - remove state and return result_sender for error reporting.
    fn fail_nat_traversal(
        &mut self,
        addr: &SocketAddr,
    ) -> Option<oneshot::Sender<Result<RemoteConnection<S, T>, TransportError>>> {
        if matches!(
            self.states.get(addr),
            Some(ConnectionState::NatTraversal { .. })
        ) {
            if let Some(ConnectionState::NatTraversal { result_sender, .. }) =
                self.states.remove(addr)
            {
                return Some(result_sender);
            }
        }
        None
    }

    /// Mark connection as closed. Only established connections enter the grace period.
    fn mark_closed(&mut self, addr: &SocketAddr) {
        match self.states.remove(addr) {
            Some(ConnectionState::Established { .. }) => {
                self.states.insert(
                    *addr,
                    ConnectionState::RecentlyClosed {
                        closed_at_nanos: self.time_source.now_nanos(),
                    },
                );
            }
            Some(ConnectionState::NatTraversal { result_sender, .. }) => {
                // Receiver may have timed out; log but don't fail
                if result_sender
                    .send(Err(TransportError::ConnectionClosed(*addr)))
                    .is_err()
                {
                    tracing::debug!(peer_addr = %addr, "NAT traversal result receiver already dropped");
                }
            }
            _ => {}
        }
    }

    /// Remove established connection and return the packet sender (for detection purposes).
    fn remove_established(&mut self, addr: &SocketAddr) -> Option<mpsc::Sender<InboundPacket>> {
        if matches!(
            self.states.get(addr),
            Some(ConnectionState::Established { .. })
        ) {
            if let Some(ConnectionState::Established {
                inbound_packet_sender,
            }) = self.states.remove(addr)
            {
                return Some(inbound_packet_sender);
            }
        }
        None
    }

    /// Check if address is rate limited for asymmetric decryption.
    fn is_rate_limited(&self, addr: &SocketAddr) -> bool {
        let now = self.time_source.now_nanos();
        self.last_asym_attempt.get(addr).is_some_and(|last_nanos| {
            now.saturating_sub(*last_nanos) < ASYM_DECRYPTION_RATE_LIMIT.as_nanos() as u64
        })
    }

    /// Record an asymmetric decryption attempt for rate limiting.
    fn record_asym_attempt(&mut self, addr: SocketAddr) {
        let now = self.time_source.now_nanos();
        self.last_asym_attempt.insert(addr, now);
    }

    /// Clear rate limit tracking for an address.
    fn clear_rate_limit(&mut self, addr: &SocketAddr) {
        self.last_asym_attempt.remove(addr);
    }

    /// Remove stale closed connections from the same IP.
    fn remove_stale_from_ip(&mut self, new_addr: SocketAddr) {
        let remote_ip = new_addr.ip();
        let stale_addrs: Vec<_> = self
            .states
            .iter()
            .filter_map(|(addr, state)| {
                if addr.ip() == remote_ip && *addr != new_addr {
                    if let ConnectionState::Established {
                        inbound_packet_sender,
                    } = state
                    {
                        if inbound_packet_sender.is_closed() {
                            return Some(*addr);
                        }
                    }
                }
                None
            })
            .collect();

        for stale_addr in stale_addrs {
            self.states.remove(&stale_addr);
            self.last_asym_attempt.remove(&stale_addr);
            tracing::debug!(
                stale_peer_addr = %stale_addr,
                new_peer_addr = %new_addr,
                "Removed stale closed connection from same IP"
            );
        }
    }

    /// Clean up expired RecentlyClosed states and rate limit entries.
    fn cleanup_expired(&mut self) {
        let now = self.time_source.now_nanos();

        self.states.retain(|_, state| {
            if let ConnectionState::RecentlyClosed { closed_at_nanos } = state {
                now.saturating_sub(*closed_at_nanos) < RECENTLY_CLOSED_DURATION.as_nanos() as u64
            } else {
                true
            }
        });

        self.last_asym_attempt.retain(|_, last_attempt_nanos| {
            now.saturating_sub(*last_attempt_nanos) < ASYM_DECRYPTION_RATE_LIMIT.as_nanos() as u64
        });
    }
}

/// Rate limiter for gateway connection acceptance after restart.
///
/// Prevents thundering herd when a gateway restarts and all peers reconnect
/// simultaneously. Uses a 1-second tumbling window to count accepted connections
/// and compares against a rate that ramps up over time since gateway start.
struct GatewayConnectionRateLimiter<T: TimeSource> {
    /// Real-clock timestamp (`time_source`) captured when the gateway started listening.
    real_start_nanos: u64,
    /// Simulation `VirtualTime` timestamp at gateway start, when a sim override clock is present.
    virtual_start_nanos: Option<u64>,
    /// Start of the current 1-second window, in effective-elapsed nanos
    /// (see [`Self::effective_elapsed_nanos`]).
    window_start_effective_nanos: u64,
    /// Number of connections accepted in the current window.
    window_count: u64,
    time_source: T,
    /// Simulation override clock. See [`Self::effective_elapsed_nanos`] for why the
    /// admission ramp uses `max(real_elapsed, virtual_elapsed)` rather than either clock alone.
    ///
    /// `Some` only in simulation (a `VirtualTime` is registered for this socket's
    /// network — note `VirtualTime is now always enabled` for SimNetworks). `None` in
    /// production, where the real `time_source` is the only and correct admission clock.
    virtual_clock: Option<VirtualTime>,
}

impl<T: TimeSource> GatewayConnectionRateLimiter<T> {
    fn new(time_source: T, virtual_clock: Option<VirtualTime>) -> Self {
        let real_start_nanos = time_source.now_nanos();
        let virtual_start_nanos = virtual_clock.as_ref().map(|v| v.now_nanos());
        Self {
            real_start_nanos,
            virtual_start_nanos,
            window_start_effective_nanos: 0,
            window_count: 0,
            time_source,
            virtual_clock,
        }
    }

    /// Nanoseconds of *simulation progress* since gateway start, defined as
    /// `max(real_elapsed, virtual_elapsed)`.
    ///
    /// The admission ramp (5/s for 30s, 20/s to 2min, then unlimited) must advance with
    /// whatever clock the simulation is actually progressing on, and the two SimNetwork
    /// execution modes use different clocks:
    /// - **Virtual-time-driven** (e.g. `test_thundering_herd_connect_storm` advances
    ///   `VirtualTime` explicitly while spending only ~10ms real per 100ms virtual): the
    ///   ramp must follow the virtual clock or it never progresses, leaving admission
    ///   pinned at the throttled initial rate and nondeterministic in real-scheduler order
    ///   (the root cause of that test's flakiness).
    /// - **Real-time-driven** (the virtual clock is not advanced): the ramp must follow the
    ///   real clock or the 1-second window never resets and the gateway permanently blocks
    ///   new peers after the first burst, so the network never forms.
    ///
    /// `max()` advances at least as fast as either clock alone, so it is correct for both
    /// modes and is **never more restrictive than the pre-existing real-clock behavior**
    /// (it can only let the ramp complete sooner, never later). Both elapsed terms are
    /// monotonic, so their max is monotonic. In production `virtual_clock` is `None`, so
    /// this is exactly real-clock elapsed and behavior is unchanged.
    fn effective_elapsed_nanos(&self) -> u64 {
        let real_elapsed = self
            .time_source
            .now_nanos()
            .saturating_sub(self.real_start_nanos);
        let virtual_elapsed = match (&self.virtual_clock, self.virtual_start_nanos) {
            (Some(virtual_time), Some(start)) => virtual_time.now_nanos().saturating_sub(start),
            _ => 0,
        };
        real_elapsed.max(virtual_elapsed)
    }

    /// Returns the maximum connections/second allowed based on time since gateway start.
    fn current_rate_limit(&self) -> Option<u64> {
        let elapsed = self.effective_elapsed_nanos();
        if elapsed < GW_RAMP_PHASE1_DURATION.as_nanos() as u64 {
            Some(GW_RAMP_PHASE1_RATE)
        } else if elapsed < GW_RAMP_PHASE2_DURATION.as_nanos() as u64 {
            Some(GW_RAMP_PHASE2_RATE)
        } else {
            None // Unlimited
        }
    }

    /// Check if a new connection should be accepted. Returns true if allowed.
    fn try_accept(&mut self) -> bool {
        let Some(max_rate) = self.current_rate_limit() else {
            return true; // Unlimited phase
        };

        let now = self.effective_elapsed_nanos();
        let window_elapsed = now.saturating_sub(self.window_start_effective_nanos);

        // Reset window every second
        if window_elapsed >= Duration::from_secs(1).as_nanos() as u64 {
            self.window_start_effective_nanos = now;
            self.window_count = 0;
        }

        if self.window_count < max_rate {
            self.window_count += 1;
            true
        } else {
            false
        }
    }
}

#[cfg(any(test, feature = "bench"))]
impl<S, T: TimeSource> Drop for UdpPacketsListener<S, T> {
    fn drop(&mut self) {
        tracing::info!(%self.this_addr, "Dropping UdpPacketsListener");
    }
}
task_local! {
    static RANDOM_U64: [u8; 8];
}

/// The amount of times to retry NAT traversal before giving up. It should be sufficient
/// so we give peers enough time for the other party to start the connection on its end.
#[cfg(not(any(test, feature = "bench")))]
pub(super) const NAT_TRAVERSAL_MAX_ATTEMPTS: usize = 40;
#[cfg(any(test, feature = "bench"))]
pub(super) const NAT_TRAVERSAL_MAX_ATTEMPTS: usize = 10;

/// How long an outbound hole punch keeps sending before it gives up, whatever
/// the attempt count. Both sides of a CONNECT punch for this long, so it is
/// also the window in which the joiner must start dialling an acceptor.
pub(crate) const NAT_TRAVERSAL_DEADLINE: Duration = Duration::from_secs(3);

impl<S: Socket, T: TimeSource> UdpPacketsListener<S, T> {
    #[tracing::instrument(level = "debug", name = "transport_listener", fields(peer = %self.this_peer_keypair.public), skip_all)]
    async fn listen(mut self) -> TransportError {
        tracing::debug!(
            bind_addr = %self.this_addr,
            "Listening for packets"
        );
        let mut buf = [0u8; RECV_BUF_SIZE];
        let mut connection_tasks = FuturesUnordered::new();
        let mut gw_connection_tasks = FuturesUnordered::new();
        let mut outdated_peer: HashMap<SocketAddr, u64> = HashMap::new();
        let mut last_cleanup_nanos = self.time_source.now_nanos();
        let mut recv_errors = RecvErrorTracker::default();

        'outer: loop {
            // DST: Use deterministic_select! for fair but deterministic branch ordering
            crate::deterministic_select! {
                recv_result = self.socket_listener.recv_classified(&mut buf) => {
                    match recv_result {
                        // The socket received a datagram that is not for this
                        // listener (`DemuxSocket` counted it and dropped it or
                        // queued it for the QUIC ingress). It is still a
                        // successful read, so it is evidence the socket works.
                        Ok(SocketRead::Diverted) => {
                            recv_errors.on_success();
                            continue;
                        }
                        Ok(SocketRead::Datagram(size, remote_addr)) => {
                            recv_errors.on_success();
                            if let Some(time_nanos) = outdated_peer.get(&remote_addr) {
                                let now_nanos = self.time_source.now_nanos();
                                if now_nanos.saturating_sub(*time_nanos) < OUTDATED_PEER_EXPIRY.as_nanos() as u64 {
                                    let packet_data = PacketData::<_, RECV_BUF_SIZE>::from_buf(&buf[..size]);
                                    if packet_data.is_intro_packet() {
                                        if self.connections.is_rate_limited(&remote_addr) {
                                            continue;
                                        }
                                        self.connections.record_asym_attempt(remote_addr);
                                        if let Ok(decrypted) = packet_data.try_decrypt_asym(&self.this_peer_keypair.secret) {
                                            let decrypted_data = decrypted.data();
                                            let proto_len = PROTOC_VERSION.len();
                                            if decrypted_data.len() >= proto_len + 16 {
                                                let protoc_bytes: Result<[u8; 8], _> =
                                                    decrypted_data[..proto_len].try_into();
                                                if let Ok(ref remote_ver) = protoc_bytes {
                                                    if version_cmp::is_compatible(&PROTOC_VERSION, remote_ver).is_ok() {
                                                        tracing::info!(
                                                            peer_addr = %remote_addr,
                                                            "Outdated peer upgraded. Allowing reconnection."
                                                        );
                                                        outdated_peer.remove(&remote_addr);
                                                        self.connections.clear_rate_limit(&remote_addr);
                                                    } else {
                                                        continue;
                                                    }
                                                } else {
                                                    continue;
                                                }
                                            } else {
                                                continue;
                                            }
                                        } else {
                                            continue;
                                        }
                                    } else {
                                        continue;
                                    }
                                } else {
                                    outdated_peer.remove(&remote_addr);
                                }
                            }
                            let packet_data = PacketData::from_buf(&buf[..size]);

                            tracing::trace!(
                                peer_addr = %remote_addr,
                                packet_size = size,
                                is_established = self.connections.is_established(&remote_addr),
                                has_gw_handshake = self.connections.has_gateway_handshake(&remote_addr),
                                has_nat_traversal = self.connections.has_nat_traversal(&remote_addr),
                                "Received packet from remote"
                            );

                            // Route packet based on connection state
                            if let Some(state) = self.connections.get_state(&remote_addr) {
                                match state {
                                    ConnectionState::Established { .. } => {
                                        // Check for new identity (peer restart).
                                        // Both gateways and regular peers detect restart: a valid
                                        // intro packet on an established connection means the remote
                                        // restarted and is attempting to reconnect (possibly reusing
                                        // an existing NAT hole). Tearing down the stale session
                                        // lets the new handshake proceed. Rate-limited to 1/sec per
                                        // IP to prevent asymmetric decryption DoS.
                                        let is_new_identity = if packet_data.is_intro_packet() {
                                            if self.connections.is_rate_limited(&remote_addr) {
                                                false
                                            } else {
                                                self.connections.record_asym_attempt(remote_addr);
                                                match packet_data.try_decrypt_asym(&self.this_peer_keypair.secret) {
                                                    Ok(decrypted) => {
                                                        let decrypted_data = decrypted.data();
                                                        let proto_len = PROTOC_VERSION.len();
                                                        if decrypted_data.len() >= proto_len + 16 {
                                                            decrypted_data[..proto_len].try_into().ok()
                                                                .map(|remote_ver: [u8; 8]| {
                                                                    version_cmp::is_compatible(&PROTOC_VERSION, &remote_ver).is_ok()
                                                                })
                                                                .unwrap_or(false)
                                                        } else {
                                                            false
                                                        }
                                                    }
                                                    Err(_) => false,
                                                }
                                            }
                                        } else {
                                            false
                                        };

                                        if is_new_identity {
                                            tracing::info!(
                                                peer_addr = %remote_addr,
                                                is_gateway = self.is_gateway,
                                                "Detected peer restart (new intro on established connection). Resetting session."
                                            );
                                            self.connections.clear_rate_limit(&remote_addr);
                                            self.connections.remove_established(&remote_addr);

                                            if !self.is_gateway {
                                                // Non-gateway: accept the restarted peer's intro
                                                // using a server-side handshake (same as gateways).
                                                // This lets the restarted peer reconnect directly
                                                // via an existing NAT hole without full gateway
                                                // re-bootstrap.
                                                let inbound_key_bytes = key_from_addr(&remote_addr);
                                                let (gw_ongoing_connection, packets_sender) =
                                                    self.gateway_connection(packet_data, remote_addr, inbound_key_bytes);
                                                if self.connections.start_gateway_handshake(remote_addr, packets_sender) {
                                                    let task = GlobalExecutor::spawn(gw_ongoing_connection
                                                        .instrument(tracing::span!(tracing::Level::DEBUG, "reconnect_accept"))
                                                        .map_err(move |error| {
                                                            tracing::debug!(peer_addr = %remote_addr, error = %error, "Reconnect accept error");
                                                            (error, remote_addr)
                                                        }));
                                                    gw_connection_tasks.push(task);
                                                }
                                                continue;
                                            }
                                            // Gateway: fall through to gateway handler for fresh session
                                        } else {
                                            match self.connections.try_send_established(&remote_addr, packet_data) {
                                                Ok(true) => continue,
                                                Ok(false) => {
                                                    // Channel full - track dropped packets
                                                    let dropped_count = self.dropped_packets.entry(remote_addr).or_insert(0);
                                                    *dropped_count += 1;
                                                    let now_nanos = self.time_source.now_nanos();
                                                    if now_nanos.saturating_sub(self.last_drop_warning_nanos) > Duration::from_secs(10).as_nanos() as u64 {
                                                        let total_dropped: u64 = self.dropped_packets.values().sum();
                                                        tracing::warn!(
                                                            total_dropped,
                                                            elapsed_secs = 10,
                                                            "Channel overflow: dropped packets (bandwidth limit may be too high or receiver too slow)"
                                                        );
                                                        for (addr, count) in &self.dropped_packets {
                                                            if *count > 100 {
                                                                tracing::warn!(
                                                                    peer_addr = %addr,
                                                                    dropped_count = count,
                                                                    "High packet drop rate for peer"
                                                                );
                                                            }
                                                        }
                                                        self.dropped_packets.clear();
                                                        self.last_drop_warning_nanos = now_nanos;
                                                    }
                                                    continue;
                                                }
                                                Err(()) => {
                                                    tracing::debug!(peer_addr = %remote_addr, "Connection closed");
                                                    continue;
                                                }
                                            }
                                        }
                                    }

                                    ConnectionState::GatewayHandshake { .. } => {
                                        if packet_data.is_intro_packet() {
                                            tracing::debug!(peer_addr = %remote_addr, "Ignoring duplicate intro during gateway handshake");
                                            continue;
                                        }
                                        match self.connections.try_send_gateway_handshake(&remote_addr, packet_data) {
                                            Ok(true) => continue,
                                            Ok(false) | Err(()) => {
                                                tracing::debug!(peer_addr = %remote_addr, "Gateway handshake channel busy/closed, dropping packet");
                                                continue;
                                            }
                                        }
                                    }

                                    ConnectionState::NatTraversal { .. } => {
                                        // Rate limit intro packets to prevent decryption storms
                                        // during NAT traversal handshake
                                        if packet_data.is_intro_packet() {
                                            if self.connections.is_rate_limited(&remote_addr) {
                                                tracing::trace!(peer_addr = %remote_addr, "Rate limiting NAT traversal intro packet");
                                                continue;
                                            }
                                            self.connections.record_asym_attempt(remote_addr);
                                        }
                                        match self.connections.try_send_nat_traversal(&remote_addr, packet_data) {
                                            Ok(TrySendNatTraversalOutcome::Sent) => continue,
                                            Ok(TrySendNatTraversalOutcome::Full) => {
                                                tracing::debug!(peer_addr = %remote_addr, "NAT traversal channel full, dropping packet (peer will retry)");
                                                continue;
                                            }
                                            Ok(TrySendNatTraversalOutcome::Closed) => {
                                                tracing::debug!(peer_addr = %remote_addr, "NAT traversal handshake task exited; dropping packet");
                                                continue;
                                            }
                                            Err(()) => {
                                                tracing::debug!(peer_addr = %remote_addr, "NAT traversal state missing");
                                                continue;
                                            }
                                        }
                                    }

                                    ConnectionState::RecentlyClosed { .. } => {
                                        if !packet_data.is_intro_packet() || !self.is_gateway {
                                            tracing::trace!(peer_addr = %remote_addr, "Dropping packet for recently closed connection");
                                            continue;
                                        }
                                        // Intro packet - fall through to gateway handler for reconnection
                                    }
                                }
                            }

                            // Gateway intro packet handling
                            if self.is_gateway {
                                if self.connections.has_gateway_handshake(&remote_addr) {
                                    tracing::debug!(peer_addr = %remote_addr, "Gateway connection already in progress");
                                    continue;
                                }

                                // Clean up stale closed connections from same IP
                                self.connections.remove_stale_from_ip(remote_addr);

                                // Only process intro packets — check before rate limiters
                                // to avoid wasting rate limit budget on non-intro packets.
                                if !packet_data.is_intro_packet() {
                                    tracing::trace!(peer_addr = %remote_addr, "Dropping non-intro packet from unknown address");
                                    continue;
                                }

                                // Rate limit intro attempts (per-IP).
                                // NOTE: this per-IP throttle is still measured on the real
                                // wall-clock `time_source`, unlike the gateway admission ramp
                                // below which uses the simulation clock. It is deliberately left
                                // on RealTime here (out of scope for the #4383 fix); see #4384.
                                if self.connections.is_rate_limited(&remote_addr) {
                                    tracing::trace!(peer_addr = %remote_addr, "Rate limiting gateway intro attempt");
                                    continue;
                                }

                                // Global rate limit: prevent thundering herd after gateway restart.
                                // Gradually ramps up connection acceptance rate over 2 minutes.
                                // Safe to drop: peers send multiple intro packets per attempt
                                // and retry via initial_join_procedure's loop with jitter.
                                if !self.gw_rate_limiter.try_accept() {
                                    tracing::trace!(
                                        peer_addr = %remote_addr,
                                        max_rate = ?self.gw_rate_limiter.current_rate_limit(),
                                        "Deferring connection: gateway ramp-up rate limit"
                                    );
                                    continue;
                                }

                                self.connections.record_asym_attempt(remote_addr);

                                let inbound_key_bytes = key_from_addr(&remote_addr);
                                let (gw_ongoing_connection, packets_sender) = self.gateway_connection(packet_data, remote_addr, inbound_key_bytes);
                                if !self.connections.start_gateway_handshake(remote_addr, packets_sender) {
                                    continue; // State transition failed (e.g., RecentlyClosed still draining)
                                }
                                let task = GlobalExecutor::spawn(gw_ongoing_connection
                                    .instrument(tracing::span!(tracing::Level::DEBUG, "gateway_connection"))
                                    .map_err(move |error| {
                                        tracing::debug!(peer_addr = %remote_addr, error = %error, "Gateway connection error");
                                        (error, remote_addr)
                                    }));
                                gw_connection_tasks.push(task);
                                continue;
                            } else {
                                if packet_data.is_intro_packet() {
                                    self.count_stray_intro(remote_addr);
                                }
                                // Non-gateway peers: mark as expected and wait for the normal peer handshake flow.
                                self.expected_non_gateway.insert(remote_addr.ip());
                                tracing::trace!(
                                    peer_addr = %remote_addr,
                                    "Ignoring unknown packet (not an intro, no active connection)"
                                );
                                continue;
                            }
                        }
                        Err(e) => {
                            // Ending this task force-exits the node, so only an
                            // error that means the socket itself is unusable may
                            // do it. Any other error drops this read and the loop
                            // carries on. See `recv_errors` for the classification.
                            let verdict = recv_errors.on_error(&e, self.time_source.now_nanos());
                            let suppressed = match verdict.log {
                                RecvErrorLog::Emit { suppressed } => Some(suppressed),
                                RecvErrorLog::Suppress => None,
                            };
                            match verdict.action {
                                RecvErrorAction::Exit => {
                                    tracing::error!(
                                        error = ?e,
                                        error_kind = ?e.kind(),
                                        os_error = ?e.raw_os_error(),
                                        class = ?verdict.class,
                                        suppressed_since_last_log = suppressed.unwrap_or(0),
                                        bind_addr = %self.this_addr,
                                        "Fatal UDP recv_from error, listen task exiting"
                                    );
                                    return e.into();
                                }
                                RecvErrorAction::Continue { pause } => {
                                    if let Some(suppressed) = suppressed {
                                        tracing::warn!(
                                            error = ?e,
                                            error_kind = ?e.kind(),
                                            os_error = ?e.raw_os_error(),
                                            class = ?verdict.class,
                                            suppressed_since_last_log = suppressed,
                                            "UDP recv_from error, dropping the read and continuing"
                                        );
                                    }
                                    match pause {
                                        RecvErrorPause::None => {}
                                        RecvErrorPause::Yield => tokio::task::yield_now().await,
                                        RecvErrorPause::Sleep(d) => self.time_source.sleep(d).await,
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                },
                // Handle completed gateway connection handshakes
                gw_connection_handshake = gw_connection_tasks.next(), if !gw_connection_tasks.is_empty() => {
                    let Some(res): GwOngoingConnectionResult<S, T> = gw_connection_handshake else {
                        unreachable!("gw_connection_tasks.next() should only return None if empty, which is guarded");
                    };
                    // Issue #2725: Handle JoinError properly - tasks can be cancelled during
                    // shutdown or cleanup, not just panic. Cancellation is expected behavior
                    // and should not crash the handler.
                    let task_result = match res {
                        Ok(inner) => inner,
                        Err(join_error) => {
                            if join_error.is_cancelled() {
                                tracing::debug!(
                                    direction = "inbound",
                                    "Gateway connection task was cancelled"
                                );
                            } else {
                                // Task panicked - log but don't propagate to avoid crashing handler
                                tracing::error!(
                                    direction = "inbound",
                                    "Gateway connection task panicked: {join_error}"
                                );
                            }
                            continue;
                        }
                    };
                    match task_result {
                        // Successful gateway connection
                        Ok((outbound_remote_conn, inbound_remote_connection, outbound_ack_packet)) => {
                            let remote_addr = outbound_remote_conn.remote_addr;
                            let sent_tracker = outbound_remote_conn.sent_tracker.clone();

                            if self.connections.complete_gateway_handshake(remote_addr, inbound_remote_connection.inbound_packet_sender) {
                                if self.new_connection_notifier
                                    .send(PeerConnection::new(outbound_remote_conn))
                                    .await
                                    .is_err() {
                                    tracing::error!(peer_addr = %remote_addr, "Failed to notify new gateway connection");
                                    break 'outer TransportError::ConnectionClosed(self.this_addr);
                                }
                                sent_tracker.lock().report_sent_packet(
                                    SymmetricMessage::FIRST_PACKET_ID,
                                    outbound_ack_packet.prepared_send(),
                                );
                            } else {
                                tracing::debug!(peer_addr = %remote_addr, "Connection state changed during handshake");
                            }
                        }
                        // Failed gateway connection
                        Err((error, remote_addr)) => {
                            tracing::info!(error = %error, peer_addr = %remote_addr, "Failed to establish gateway connection");
                            if matches!(error, TransportError::ProtocolVersionMismatch { .. }) {
                                outdated_peer.insert(remote_addr, self.time_source.now_nanos());
                            }
                            self.connections.fail_gateway_handshake(&remote_addr);
                        }
                    }
                },
                // Handle completed NAT traversal handshakes
                connection_handshake = connection_tasks.next(), if !connection_tasks.is_empty() => {
                    let Some(res): OngoingConnectionResult<S, T> = connection_handshake else {
                        unreachable!("connection_tasks.next() should only return None if empty, which is guarded");
                    };
                    // Issue #2725: Handle JoinError properly - tasks can be cancelled during
                    // shutdown or cleanup, not just panic. Cancellation is expected behavior
                    // and should not crash the handler.
                    let task_result = match res {
                        Ok(inner) => inner,
                        Err(join_error) => {
                            if join_error.is_cancelled() {
                                tracing::debug!(
                                    direction = "outbound",
                                    "Connection task was cancelled"
                                );
                            } else {
                                // Task panicked - log but don't propagate to avoid crashing handler
                                tracing::error!(
                                    direction = "outbound",
                                    "Connection task panicked: {join_error}"
                                );
                            }
                            continue;
                        }
                    };
                    match task_result {
                        // Successful NAT traversal
                        Ok((outbound_remote_conn, inbound_remote_connection, origin)) => {
                            let remote_addr = outbound_remote_conn.remote_addr;
                            self.expected_non_gateway.remove(&remote_addr.ip());

                            // Atomic transition: NatTraversal -> Established
                            if let Some(result_sender) = self.connections.complete_nat_traversal(
                                remote_addr,
                                inbound_remote_connection.inbound_packet_sender,
                            ) {
                                tracing::info!(peer_addr = %remote_addr, "NAT traversal connection established");
                                crate::transport::TRANSPORT_METRICS
                                    .record_nat_traversal_established(remote_addr, origin);
                                if result_sender.send(Ok(outbound_remote_conn)).is_err() {
                                    tracing::debug!(peer_addr = %remote_addr, "NAT traversal result receiver already dropped");
                                }
                            } else {
                                tracing::debug!(peer_addr = %remote_addr, "Connection state changed during NAT traversal");
                            }
                        }
                        // Failed NAT traversal
                        Err((error, remote_addr, observed, origin)) => {
                            let flags = observed.flags();
                            let kind = classify_nat_failure(&error, flags);
                            type O = NatAttemptObservation;
                            let seen = crate::transport::metrics::NatAttemptSeen {
                                received_any: flags & O::RECEIVED != 0,
                                decrypted_any: flags & O::DECRYPTED != 0,
                                deadline_reached: flags & O::DEADLINE != 0,
                            };
                            crate::transport::TRANSPORT_METRICS.record_nat_failure(
                                remote_addr,
                                kind,
                                seen,
                                origin,
                            );
                            self.recent_traversal_failures.record_failure(
                                remote_addr,
                                kind,
                                self.time_source.now_nanos(),
                                &crate::transport::TRANSPORT_METRICS,
                            );
                            match &error {
                                TransportError::ProtocolVersionMismatch { .. } => {
                                    tracing::warn!(%error);
                                    crate::transport::TRANSPORT_METRICS
                                        .record_nat_traversal_failed_version();
                                }
                                TransportError::ChannelClosed | TransportError::ConnectionClosed(_) | TransportError::ConnectionEstablishmentFailure { .. } | TransportError::Udp(UdpError(UdpErrorKind::SendFailed(..))) | TransportError::OutboundStreamFailed(_) | TransportError::OutboundStreamInvalidLength(..) | TransportError::Udp(UdpError(UdpErrorKind::IO(_))) | TransportError::Other(_) | TransportError::Udp(UdpError(UdpErrorKind::PubKeyDecryptionError(_))) | TransportError::Udp(UdpError(UdpErrorKind::Serialization(_))) | TransportError::Quic(_) => {
                                    tracing::error!(error = %error, peer_addr = %remote_addr, "Failed NAT traversal");
                                    crate::transport::TRANSPORT_METRICS
                                        .record_nat_traversal_failed_error();
                                }
                            }
                            self.expected_non_gateway.remove(&remote_addr.ip());
                            if let Some(result_sender) = self.connections.fail_nat_traversal(&remote_addr) {
                                if result_sender.send(Err(error)).is_err() {
                                    tracing::debug!(peer_addr = %remote_addr, "NAT traversal failure result receiver already dropped");
                                }
                            }
                        }
                    }
                },
                // Handle new connection requests
                connection_event = self.connection_handler.recv() => {
                    let Some((remote_addr, event)) = connection_event else {
                        // This fires both during graceful shutdown (event loop exits,
                        // drops senders) and unexpected sender drops. Use warn! since
                        // graceful shutdown is the common case.
                        tracing::warn!(
                            bind_addr = %self.this_addr,
                            "Connection handler channel closed — listen task exiting. \
                             This is expected during graceful shutdown."
                        );
                        return TransportError::ConnectionClosed(self.this_addr);
                    };
                    tracing::debug!(peer_addr = %remote_addr, "Received connection event");
                    let ConnectionEvent::ConnectionStart { remote_public_key, open_connection, origin } = event;

                    // Check for existing NAT traversal in progress
                    if self.connections.has_nat_traversal(&remote_addr) {
                        tracing::debug!(peer_addr = %remote_addr, "NAT traversal already in progress");
                        if open_connection.send(Err(TransportError::ConnectionEstablishmentFailure {
                            cause: "connection attempt already in progress".into(),
                        })).is_err() {
                            tracing::debug!(peer_addr = %remote_addr, "duplicate NAT traversal notification receiver already dropped");
                        }
                        continue;
                    }

                    tracing::info!(peer_addr = %remote_addr, "Starting NAT traversal");
                    let (ongoing_connection, packets_sender, nat_observation) = self.traverse_nat(remote_addr, remote_public_key.clone());

                    if !self.connections.start_nat_traversal(remote_addr, packets_sender, open_connection) {
                        // Rejected AFTER the `has_nat_traversal` guard above: a
                        // gateway handshake is in progress, or the peer is in its
                        // recently-closed grace period. No task is spawned and no
                        // established/failed outcome will fire, so counting this
                        // as an attempt would inflate the `nat_traversal_attempts`
                        // denominator and skew the hole-punch success rate.
                        continue;
                    }

                    // Admitted: a traversal task is now spawned and will emit
                    // exactly one established/failed outcome. Count the attempt
                    // here so the denominator matches the outcomes.
                    crate::transport::TRANSPORT_METRICS.record_nat_traversal_attempt(remote_addr, origin);

                    self.expected_non_gateway.insert(remote_addr.ip());
                    let task = GlobalExecutor::spawn(ongoing_connection
                        .map_ok(move |(conn, inbound)| (conn, inbound, origin))
                        .map_err(move |err| (err, remote_addr, nat_observation, origin))
                        .instrument(span!(tracing::Level::DEBUG, "traverse_nat"))
                    );
                    connection_tasks.push(task);
                },
                _ = async {}, if {
                    let now_nanos = self.time_source.now_nanos();
                    now_nanos.saturating_sub(last_cleanup_nanos) >= RATE_LIMIT_CLEANUP_INTERVAL.as_nanos() as u64
                } => {
                    self.connections.cleanup_expired();
                    let now_nanos = self.time_source.now_nanos();
                    self.recent_traversal_failures.prune(now_nanos);
                    last_cleanup_nanos = now_nanos;

                    let outdated_cutoff = now_nanos.saturating_sub(OUTDATED_PEER_EXPIRY.as_nanos() as u64);
                    outdated_peer.retain(|_, timestamp| *timestamp > outdated_cutoff);
                }
            }
        }
    }

    /// Count an intro packet dropped because nothing exists for its exact
    /// address (non-gateways only; telemetry). Bounded cost: a hash lookup
    /// and a range scan over this IP's entries, no allocation.
    fn count_stray_intro(&mut self, remote_addr: SocketAddr) {
        let now_nanos = self.time_source.now_nanos();
        let kind = stray_intro::classify(
            self.recent_traversal_failures
                .failed_recently(remote_addr, now_nanos),
            self.connections.has_nat_traversal_to_ip(remote_addr.ip()),
        );
        crate::transport::TRANSPORT_METRICS.record_stray_intro(remote_addr, kind);
    }

    /// Effective floor for emitting the version-carrying ack (#5161): the
    /// test-only override when set, otherwise the production constant.
    fn ack_version_floor(&self) -> (u8, u8, u16) {
        self.ack_version_floor_override
            .unwrap_or(version_cmp::GATEWAY_ACK_VERSION_MIN_VERSION)
    }

    #[allow(clippy::type_complexity)]
    fn gateway_connection(
        &mut self,
        remote_intro_packet: PacketData<UnknownEncryption>,
        remote_addr: SocketAddr,
        inbound_key_bytes: [u8; 16],
    ) -> (
        GatewayConnectionFuture<S, T>,
        mpsc::Sender<PacketData<UnknownEncryption>>,
    ) {
        let secret = self.this_peer_keypair.secret.clone();
        let bandwidth_limit = self.bandwidth_limit;
        let global_bandwidth = self.global_bandwidth.clone();
        let _ledbat_min_ssthresh = self.ledbat_min_ssthresh;
        let socket = self.socket_listener.clone();
        let time_source = self.time_source.clone();
        let congestion_config = self.congestion_config.clone();
        let ack_version_floor = self.ack_version_floor();

        let (inbound_from_remote, mut next_inbound) =
            mpsc::channel::<PacketData<UnknownEncryption>>(100);
        let f = async move {
            let decrypted_intro_packet =
                secret
                    .decrypt(remote_intro_packet.data())
                    .inspect_err(|err| {
                        tracing::debug!(
                            peer_addr = %remote_addr,
                            error = %err,
                            direction = "inbound",
                            "Failed to decrypt intro packet"
                        );
                    })?;

            let protoc = decrypted_intro_packet
                .get(..PROTOC_VERSION.len())
                .ok_or_else(|| TransportError::ConnectionEstablishmentFailure {
                    cause: "Packet too small to contain protocol version".into(),
                })?;

            let outbound_key_bytes = decrypted_intro_packet
                .get(PROTOC_VERSION.len()..PROTOC_VERSION.len() + 16)
                .ok_or_else(|| TransportError::ConnectionEstablishmentFailure {
                    cause: "Packet too small to contain outbound key bytes".into(),
                })?;

            let outbound_key = Aes128Gcm::new_from_slice(outbound_key_bytes).map_err(|_| {
                TransportError::ConnectionEstablishmentFailure {
                    cause: "invalid symmetric key".into(),
                }
            })?;
            let protoc_bytes: [u8; 8] =
                protoc
                    .try_into()
                    .map_err(|_| TransportError::ConnectionEstablishmentFailure {
                        cause: "Protocol version field wrong size".into(),
                    })?;
            // Capture the negotiated version so it can be stored on the
            // RemoteConnection below (only set on the compatible-peer path; the
            // Err arm returns before we ever build a connection).
            let remote_protoc_version =
                match version_cmp::is_compatible(&PROTOC_VERSION, &protoc_bytes) {
                    Ok(remote_info) => {
                        crate::transport::report_peer_version(remote_info.version);
                        Some(remote_info.version)
                    }
                    Err(reason) => {
                        // Track version even on incompatible peers — aids version discovery.
                        let remote_info = version_cmp::parse_version_bytes(&protoc_bytes);
                        crate::transport::report_peer_version(remote_info.version);
                        if version_cmp::remote_requires_newer_than_us(
                            &PROTOC_VERSION,
                            &protoc_bytes,
                        ) {
                            crate::transport::signal_urgent_update();
                        }
                        let packet = SymmetricMessage::ack_error(&outbound_key)?;
                        // Best effort: the peer is refused either way, and
                        // returning the mismatch rather than the send error
                        // is what makes the listener remember it as outdated.
                        // Logged at info!: a spoofed-source intro can trigger
                        // it (see `handshake_send_failed`).
                        if let Err(err) = socket.send_to(&packet.prepared_send(), remote_addr).await
                        {
                            tracing::info!(
                                peer_addr = %remote_addr,
                                direction = "inbound",
                                error = %err,
                                "Failed to send version-mismatch ack"
                            );
                        }
                        return Err(TransportError::ProtocolVersionMismatch {
                            expected: PCK_VERSION.to_string(),
                            actual: reason.to_string(),
                        });
                    }
                };

            let inbound_key = Aes128Gcm::new(&inbound_key_bytes.into());
            let outbound_ack_packet = build_success_ack(
                &outbound_key,
                inbound_key_bytes,
                remote_addr,
                remote_protoc_version,
                ack_version_floor,
            )?;

            tracing::trace!(
                peer_addr = %remote_addr,
                direction = "inbound",
                packet_len = outbound_ack_packet.data().len(),
                "Sending outbound ack packet"
            );

            socket
                .send_to(&outbound_ack_packet.clone().prepared_send(), remote_addr)
                .await
                .map_err(|err| handshake_send_failed(remote_addr, "inbound", err))?;

            // wait until the remote sends the ack packet
            let timeout_result = crate::deterministic_select! {
                result = next_inbound.recv() => Some(result),
                _ = time_source.sleep(Duration::from_secs(5)) => None,
            };
            match timeout_result {
                Some(Some(packet)) => {
                    let _ = packet.try_decrypt_sym(&inbound_key).map_err(|_| {
                        tracing::debug!(
                            peer_addr = %remote_addr,
                            direction = "inbound",
                            packet_len = packet.data().len(),
                            "Failed to decrypt packet with inbound key"
                        );
                        TransportError::ConnectionEstablishmentFailure {
                            cause: "invalid symmetric key".into(),
                        }
                    })?;
                    // Authenticated handshake-completion ACK; meter it just
                    // like any other authenticated inbound packet (#3999).
                    crate::transport::TRANSPORT_METRICS
                        .record_packet_received(remote_addr, packet.data().len() as u64);
                }
                Some(None) => {
                    return Err(TransportError::ConnectionEstablishmentFailure {
                        cause: "connection closed".into(),
                    });
                }
                None => {
                    return Err(TransportError::ConnectionEstablishmentFailure {
                        cause: "connection timed out waiting for response".into(),
                    });
                }
            }

            let sent_tracker = Arc::new(parking_lot::Mutex::new(
                SentPacketTracker::new_with_time_source(time_source.clone()),
            ));

            // Initialize congestion controller (FixedRate by default, configurable for benchmarks)
            let congestion_controller = congestion_config
                .clone()
                .unwrap_or_default()
                .build_arc_with_time_source(time_source.clone());

            // Initialize token bucket for smooth packet pacing.
            // Use global bandwidth manager if configured, otherwise use the
            // congestion controller's configured rate. Previously defaulted to
            // 10 MB/s which is 8x higher than FixedRate's 1.25 MB/s — the
            // initial burst triggered packet loss and immediate loss_pause,
            // collapsing throughput. See #3702.
            let initial_rate = if let Some(ref global) = global_bandwidth {
                global.register_connection()
            } else if let Some(limit) = bandwidth_limit {
                limit
            } else {
                congestion_controller.current_rate(TOKEN_BUCKET_WINDOW)
            };
            // Capacity = one TOKEN_BUCKET_WINDOW worth of tokens at the configured rate.
            // Previously 1MB, which allowed an immediate burst 800x larger
            // than FixedRate's per-100ms budget, triggering loss. See #3702.
            let bucket_capacity = (initial_rate / 10).max(MIN_BUCKET_CAPACITY_BYTES);
            let token_bucket = Arc::new(TokenBucket::new_with_time_source(
                bucket_capacity,
                initial_rate,
                time_source.clone(),
            ));

            let (inbound_packet_tx, inbound_packet_rx) = mpsc::channel(INBOUND_CHANNEL_CAPACITY);

            // Issue #2395: Drain any packets that arrived during the handshake.
            // The peer may send application-level messages (like ConnectRequest) immediately
            // after its side of the handshake completes, but before our handshake finishes.
            // These packets are buffered in `next_inbound` but only the ACK packet is read above.
            // Forward any remaining packets to the new connection channel so they're not lost.
            let forwarded_count = forward_handshake_buffered(
                &mut next_inbound,
                &inbound_packet_tx,
                &time_source,
                remote_addr,
            );
            if forwarded_count > 0 {
                tracing::debug!(
                    peer_addr = %remote_addr,
                    forwarded_count,
                    "Forwarded packets buffered during handshake to connection channel"
                );
            }

            let rolling_rtt_stats = super::rolling_rtt_stats::RollingRttStatsHandle::new(
                remote_addr,
                time_source.clone(),
            );
            let remote_conn = RemoteConnection {
                outbound_symmetric_key: outbound_key,
                remote_addr,
                sent_tracker,
                last_packet_id: Arc::new(AtomicU32::new(0)),
                inbound_packet_recv: inbound_packet_rx,
                inbound_symmetric_key: inbound_key,
                inbound_symmetric_key_bytes: inbound_key_bytes,
                global_bandwidth: global_bandwidth.clone(),
                my_address: None,
                remote_protoc_version,
                transport_secret_key: secret,
                congestion_controller,
                token_bucket,
                socket,
                rolling_rtt_stats,
                time_source,
            };

            let inbound_conn = InboundRemoteConnection {
                inbound_packet_sender: inbound_packet_tx,
            };

            tracing::debug!(
                peer_addr = %remote_addr,
                direction = "inbound",
                "Returning connection at gateway"
            );
            Ok((remote_conn, inbound_conn, outbound_ack_packet))
        };
        (f.boxed(), inbound_from_remote)
    }

    #[allow(clippy::type_complexity)]
    fn traverse_nat(
        &mut self,
        remote_addr: SocketAddr,
        remote_public_key: TransportPublicKey,
    ) -> (
        TraverseNatFuture<S, T>,
        mpsc::Sender<PacketData<UnknownEncryption>>,
        Arc<NatAttemptObservation>,
    ) {
        tracing::debug!(
            peer_addr = %remote_addr,
            direction = "outbound",
            "Starting NAT traversal"
        );
        let observation = Arc::new(NatAttemptObservation::default());
        let obs = observation.clone();

        #[allow(clippy::large_enum_variant)]
        enum HandshakePhase {
            /// Initiated the outbound handshake by sending our intro packet
            /// Waiting for remote's intro packet
            StartOutbound,
            /// Received remote's intro, now sending symmetric ACK with our key
            RemoteInbound,
        }

        fn decrypt_asym(
            remote_addr: SocketAddr,
            packet: &PacketData<UnknownEncryption>,
            transport_secret_key: &TransportSecretKey,
            outbound_sym_key: &mut Option<Aes128Gcm>,
            state: &mut HandshakePhase,
            remote_protoc_version: &mut Option<(u8, u8, u16)>,
            observation: &NatAttemptObservation,
        ) -> Result<(), ()> {
            if let Ok(decrypted_intro_packet) = packet.try_decrypt_asym(transport_secret_key) {
                observation.set(NatAttemptObservation::DECRYPTED);
                tracing::debug!(
                    peer_addr = %remote_addr,
                    direction = "outbound",
                    "Received intro packet"
                );
                // Bounds-check before slicing: asym encryption to our public
                // key does not authenticate the sender, so a too-short payload
                // is attacker-reachable and must not panic (#4406).
                let Some((protoc_bytes, outbound_key_bytes)) =
                    split_asym_intro_payload(decrypted_intro_packet.data())
                else {
                    tracing::debug!(
                        peer_addr = %remote_addr,
                        direction = "outbound",
                        "Intro packet too short to parse"
                    );
                    return Err(());
                };
                match version_cmp::is_compatible(&PROTOC_VERSION, &protoc_bytes) {
                    Ok(remote_info) => {
                        crate::transport::report_peer_version(remote_info.version);
                        *remote_protoc_version = Some(remote_info.version);
                    }
                    Err(reason) => {
                        observation.set(NatAttemptObservation::VERSION_INCOMPATIBLE);
                        // Track version even on incompatible peers — aids version discovery.
                        let remote_info = version_cmp::parse_version_bytes(&protoc_bytes);
                        crate::transport::report_peer_version(remote_info.version);
                        if version_cmp::remote_requires_newer_than_us(
                            &PROTOC_VERSION,
                            &protoc_bytes,
                        ) {
                            crate::transport::signal_urgent_update();
                        }
                        tracing::debug!(
                            peer_addr = %remote_addr,
                            %reason,
                            direction = "outbound",
                            "Remote is using an incompatible protocol version"
                        );
                        return Err(());
                    }
                }
                let outbound_key =
                    Aes128Gcm::new_from_slice(&outbound_key_bytes).expect("correct length");
                *outbound_sym_key = Some(outbound_key);
                // Got remote's key, now we can send ACK with our key
                *state = HandshakePhase::RemoteInbound;
                return Ok(());
            }
            tracing::trace!(
                peer_addr = %remote_addr,
                direction = "outbound",
                "Failed to decrypt packet"
            );
            Err(())
        }

        let transport_secret_key = self.this_peer_keypair.secret.clone();
        let bandwidth_limit = self.bandwidth_limit;
        let global_bandwidth = self.global_bandwidth.clone();
        let _ledbat_min_ssthresh = self.ledbat_min_ssthresh;
        let socket = self.socket_listener.clone();
        let time_source = self.time_source.clone();
        let congestion_config = self.congestion_config.clone();
        let (inbound_from_remote, mut next_inbound) =
            mpsc::channel::<PacketData<UnknownEncryption>>(100);
        let this_addr = self.this_addr;
        let ack_version_floor = self.ack_version_floor();
        let f = async move {
            tracing::info!(
                peer_addr = %remote_addr,
                direction = "outbound",
                "Starting outbound handshake (NAT traversal)"
            );
            let mut state = HandshakePhase::StartOutbound;
            let mut attempts = 0usize;
            let start_time_nanos = time_source.now_nanos();
            let overall_deadline = NAT_TRAVERSAL_DEADLINE;

            // Generate symmetric key using GlobalRng for deterministic simulation
            let mut inbound_sym_key_bytes = [0u8; 16];
            GlobalRng::fill_bytes(&mut inbound_sym_key_bytes);
            let inbound_sym_key = Aes128Gcm::new(&inbound_sym_key_bytes.into());

            let mut outbound_sym_key: Option<Aes128Gcm> = None;
            // Remote peer's negotiated protocol version, captured by decrypt_asym
            // on the hole-punch (RemoteInbound) path. Stays None on the
            // AckConnection (joiner->gateway) path, whose payload has no version.
            let mut remote_protoc_version: Option<(u8, u8, u16)> = None;
            let outbound_intro_packet = {
                let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
                data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
                data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
                PacketData::<_, MAX_PACKET_SIZE>::encrypt_with_pubkey(&data, &remote_public_key)
            };

            let mut sent_tracker = SentPacketTracker::new_with_time_source(time_source.clone());

            // A failed send is handled like a datagram lost on the wire: the
            // loop sends again on its next pass, so an error that clears (a
            // network coming back) does not end the attempt. The error itself
            // cannot say whether it will clear: a socket bound to [::1] gets
            // ENETUNREACH for every off-host destination, the same error a
            // returning network gives (#5871). The deadline bounds both, and
            // if the last send failed the attempt reports that error.
            let mut send_failures = HandshakeSendFailures::default();

            // NAT traversal: keep sending packets throughout the deadline to maintain NAT bindings.
            // The acceptor may start hole-punching well before the joiner, so we must keep
            // sending until the deadline expires, not just until we've sent MAX_ATTEMPTS packets.
            // End of the current receive window. A pass that may send opens
            // a new 200 ms window. A pass after a dropped packet
            // (`resume_receive`) skips the send and waits out what is left
            // of the window, so dropped packets neither cause extra sends
            // nor postpone the next paced one.
            let mut recv_window_end_nanos = 0u64;
            let mut resume_receive = false;
            while time_source.now_nanos().saturating_sub(start_time_nanos)
                < overall_deadline.as_nanos() as u64
            {
                let skip_send = std::mem::take(&mut resume_receive);
                if !skip_send {
                    recv_window_end_nanos = time_source
                        .now_nanos()
                        .saturating_add(Duration::from_millis(200).as_nanos() as u64);
                }
                // Send a packet if we haven't exceeded the max attempts
                if !skip_send && attempts < NAT_TRAVERSAL_MAX_ATTEMPTS {
                    match state {
                        HandshakePhase::StartOutbound => {
                            tracing::trace!(
                                peer_addr = %remote_addr,
                                direction = "outbound",
                                "Sending protocol version and inbound key"
                            );
                            send_failures.record(
                                socket
                                    .send_to(outbound_intro_packet.data(), remote_addr)
                                    .await,
                                remote_addr,
                                "intro",
                            );
                            attempts += 1;
                        }
                        HandshakePhase::RemoteInbound => {
                            tracing::trace!(
                                peer_addr = %remote_addr,
                                direction = "outbound",
                                "Sending back protocol version and inbound key to remote"
                            );
                            // Load-bearing for peer-to-peer, not just the
                            // gateway path: if the remote races ahead to its own
                            // `AckConnection` arm before parsing OUR intro, this
                            // ack is the only place it can learn our version.
                            // We reached `RemoteInbound` by parsing the remote's
                            // intro, so its version is known here (#5161).
                            let our_inbound = build_success_ack(
                                outbound_sym_key.as_ref().expect("should be set"),
                                inbound_sym_key_bytes,
                                remote_addr,
                                remote_protoc_version,
                                ack_version_floor,
                            )?;
                            // Bookkeeping hygiene only: this tracker is not
                            // consumed after RemoteInbound (a successful
                            // completion builds a fresh one), so skipping an
                            // unsent packet changes no accounting.
                            if send_failures.record(
                                socket.send_to(our_inbound.data(), remote_addr).await,
                                remote_addr,
                                "ack",
                            ) {
                                sent_tracker.report_sent_packet(
                                    SymmetricMessage::FIRST_PACKET_ID,
                                    our_inbound.data().into(),
                                );
                            }
                            attempts += 1;
                        }
                    }
                }
                let next_inbound_result = crate::deterministic_select! {
                    result = next_inbound.recv() => Some(result),
                    _ = time_source.sleep(Duration::from_nanos(
                        recv_window_end_nanos.saturating_sub(time_source.now_nanos()),
                    )) => None,
                };
                match next_inbound_result {
                    Some(Some(packet)) => {
                        obs.set(NatAttemptObservation::RECEIVED);
                        tracing::trace!(
                            peer_addr = %remote_addr,
                            direction = "outbound",
                            "Received packet after sending it"
                        );
                        match state {
                            HandshakePhase::StartOutbound => {
                                tracing::trace!(
                                    peer_addr = %remote_addr,
                                    direction = "outbound",
                                    packet_len = packet.data().len(),
                                    "Received packet from remote"
                                );
                                if let Ok(decrypted_packet) =
                                    packet.try_decrypt_sym(&inbound_sym_key)
                                {
                                    obs.set(NatAttemptObservation::DECRYPTED);
                                    // Authenticated handshake-completion ACK; meter
                                    // it just like any other authenticated inbound
                                    // packet (#3999).
                                    crate::transport::TRANSPORT_METRICS.record_packet_received(
                                        remote_addr,
                                        packet.data().len() as u64,
                                    );
                                    // Remote decrypted our intro and is sending ACK with their key
                                    let symmetric_message =
                                        SymmetricMessage::deser(decrypted_packet.data())?;

                                    #[cfg(test)]
                                    {
                                        tracing::trace!(
                                            peer_addr = %remote_addr,
                                            direction = "outbound",
                                            payload = ?symmetric_message.payload,
                                            "Received symmetric packet"
                                        );
                                    }
                                    #[cfg(not(test))]
                                    {
                                        tracing::trace!(
                                            peer_addr = %remote_addr,
                                            direction = "outbound",
                                            "Received symmetric packet"
                                        );
                                    }

                                    // #5161: the acceptor's ack comes in two
                                    // encodings — the legacy `AckConnection`
                                    // and the version-carrying
                                    // `AckConnectionV2` — that differ only in
                                    // whether the acceptor told us its
                                    // version. Normalize first so the
                                    // connection-construction body below stays
                                    // single-copy; duplicating it per arm is
                                    // how the two encodings would drift.
                                    if let Some((key, my_address, acceptor_version_bytes)) =
                                        ack_ok_parts(&symmetric_message.payload)
                                    {
                                        {
                                            let outbound_sym_key = Aes128Gcm::new_from_slice(&key)
                                                .map_err(|_| {
                                                    TransportError::ConnectionEstablishmentFailure {
                                                        cause: "invalid symmetric key".into(),
                                                    }
                                                })?;
                                            // Only V2 carries a version. When
                                            // it does, this is the ONLY point
                                            // on this path where the acceptor's
                                            // version can be learned: we never
                                            // parse an intro packet from it.
                                            // Report it so version discovery
                                            // (`gateway_version_probe`, the
                                            // auto-update signal) sees gateways
                                            // at all, not only peers that
                                            // hole-punched back to us (#5166).
                                            //
                                            // Deliberately NOT run through
                                            // `is_compatible` /
                                            // `remote_requires_newer_than_us`
                                            // the way the four intro-parsing
                                            // report sites are, and the
                                            // asymmetry is not an oversight:
                                            // the acceptor already ran the
                                            // BIDIRECTIONAL compatibility check
                                            // against our intro before choosing
                                            // to send this, and would have
                                            // replied `ack_error` instead had
                                            // it required a newer version than
                                            // ours. So the "we must update to
                                            // talk to this peer" branch is
                                            // unreachable here by construction.
                                            // The acceptor's view of our
                                            // external address (telemetry:
                                            // NAT-mapping signal; the
                                            // address itself is not exported).
                                            crate::transport::TRANSPORT_METRICS
                                                .record_observed_external_addr(
                                                    remote_addr,
                                                    my_address,
                                                );
                                            if let Some(bytes) = acceptor_version_bytes {
                                                let info = version_cmp::parse_version_bytes(&bytes);
                                                crate::transport::report_peer_version(info.version);
                                                remote_protoc_version = Some(info.version);
                                            }
                                            tracing::trace!(
                                                peer_addr = %remote_addr,
                                                direction = "outbound",
                                                "Sending back ack connection"
                                            );
                                            let ack_packet = SymmetricMessage::ack_ok(
                                                &outbound_sym_key,
                                                inbound_sym_key_bytes,
                                                remote_addr,
                                            )?;
                                            // One-shot: nothing later in this
                                            // attempt re-sends it, so a failure
                                            // ends the attempt.
                                            socket
                                                .send_to(ack_packet.data(), remote_addr)
                                                .await
                                                .map_err(|err| {
                                                    handshake_send_failed(
                                                        remote_addr,
                                                        "outbound",
                                                        err,
                                                    )
                                                })?;
                                            let (inbound_sender, inbound_recv) =
                                                mpsc::channel(INBOUND_CHANNEL_CAPACITY);

                                            // Initialize congestion controller (FixedRate by default, configurable for benchmarks)
                                            let congestion_controller = congestion_config
                                                .clone()
                                                .unwrap_or_default()
                                                .build_arc_with_time_source(time_source.clone());

                                            // Initialize token bucket at congestion controller's
                                            // rate to avoid initial burst. See #3702.
                                            let initial_rate =
                                                if let Some(ref global) = global_bandwidth {
                                                    global.register_connection()
                                                } else if let Some(limit) = bandwidth_limit {
                                                    limit
                                                } else {
                                                    congestion_controller
                                                        .current_rate(TOKEN_BUCKET_WINDOW)
                                                };
                                            let bucket_capacity =
                                                (initial_rate / 10).max(MIN_BUCKET_CAPACITY_BYTES);
                                            let token_bucket =
                                                Arc::new(TokenBucket::new_with_time_source(
                                                    bucket_capacity,
                                                    initial_rate,
                                                    time_source.clone(),
                                                ));

                                            tracing::info!(
                                                peer_addr = %remote_addr,
                                                attempts,
                                                direction = "outbound",
                                                "Outbound handshake completed (ack path)"
                                            );
                                            let rolling_rtt_stats =
                                                super::rolling_rtt_stats::RollingRttStatsHandle::new(
                                                    remote_addr,
                                                    time_source.clone(),
                                                );
                                            return Ok((
                                                RemoteConnection {
                                                    outbound_symmetric_key: outbound_sym_key,
                                                    remote_addr,
                                                    sent_tracker: Arc::new(
                                                        parking_lot::Mutex::new(sent_tracker),
                                                    ),
                                                    last_packet_id: Arc::new(AtomicU32::new(0)),
                                                    inbound_packet_recv: inbound_recv,
                                                    inbound_symmetric_key: inbound_sym_key,
                                                    inbound_symmetric_key_bytes:
                                                        inbound_sym_key_bytes,
                                                    my_address: Some(my_address),
                                                    // Set above iff the ack was
                                                    // `AckConnectionV2`; `None`
                                                    // means the acceptor is
                                                    // pre-`GATEWAY_ACK_VERSION_MIN_VERSION`
                                                    // and genuinely told us
                                                    // nothing (#5161).
                                                    remote_protoc_version,
                                                    transport_secret_key: transport_secret_key
                                                        .clone(),
                                                    congestion_controller,
                                                    token_bucket,
                                                    socket: socket.clone(),
                                                    global_bandwidth: global_bandwidth.clone(),
                                                    rolling_rtt_stats,
                                                    time_source: time_source.clone(),
                                                },
                                                InboundRemoteConnection {
                                                    inbound_packet_sender: inbound_sender,
                                                },
                                            ));
                                        }
                                    }
                                    match symmetric_message.payload {
                                        SymmetricMessagePayload::AckConnection {
                                            result: Err(err),
                                        } => {
                                            return Err(handle_ack_connection_error(err));
                                        }
                                        // Unreachable: `ack_ok_parts` consumed
                                        // both success encodings above and its
                                        // body always returns. Kept as its OWN
                                        // arm rather than folded into the
                                        // "unexpected packet" list below, and
                                        // logged at ERROR, because reaching it
                                        // means a success encoding exists that
                                        // `ack_ok_parts` does not destructure —
                                        // e.g. a future `AckConnectionV3` added
                                        // to the enum and to the list below but
                                        // forgotten there. That compiles, keeps
                                        // the match exhaustive, and would
                                        // otherwise present as a handshake that
                                        // silently never completes.
                                        SymmetricMessagePayload::AckConnection {
                                            result: Ok(_),
                                        }
                                        | SymmetricMessagePayload::AckConnectionV2 { .. } => {
                                            tracing::error!(
                                                peer_addr = %remote_addr,
                                                direction = "outbound",
                                                "Success ack not destructured by ack_ok_parts — a \
                                                 success encoding was added to the payload enum \
                                                 without teaching ack_ok_parts about it"
                                            );
                                            continue;
                                        }
                                        SymmetricMessagePayload::ShortMessage { .. }
                                        | SymmetricMessagePayload::StreamFragment { .. }
                                        | SymmetricMessagePayload::NoOp
                                        | SymmetricMessagePayload::Ping { .. }
                                        | SymmetricMessagePayload::Pong { .. }
                                        | SymmetricMessagePayload::Upgrade(_) => {
                                            tracing::trace!(
                                                peer_addr = %remote_addr,
                                                direction = "outbound",
                                                "Unexpected packet from remote"
                                            );
                                            continue;
                                        }
                                    }
                                }

                                // probably the first packet to punch through the NAT
                                if decrypt_asym(
                                    remote_addr,
                                    &packet,
                                    &transport_secret_key,
                                    &mut outbound_sym_key,
                                    &mut state,
                                    &mut remote_protoc_version,
                                    &obs,
                                )
                                .is_ok()
                                {
                                    continue;
                                }

                                tracing::trace!(
                                    peer_addr = %remote_addr,
                                    direction = "outbound",
                                    "Failed to decrypt packet"
                                );
                                continue;
                            }
                            HandshakePhase::RemoteInbound => {
                                // Handle repeated intro packet during NAT traversal
                                // or proceed if symmetric ack received
                                if packet.is_intro_packet() {
                                    tracing::trace!(
                                        peer_addr = %remote_addr,
                                        direction = "outbound",
                                        "Received repeated intro packet"
                                    );
                                    continue;
                                }
                                // Complete only on a packet encrypted under
                                // this attempt's inbound session key. The
                                // success ack sent above is unchanged here; a
                                // follow-up changes it. Every release encrypts
                                // the packets that complete this arm under this
                                // key, so the check needs no version gate.
                                // Any other packet is dropped and counted (once
                                // per packet), and the attempt goes back to
                                // receiving without sending, so the next ack
                                // keeps its normal pacing.
                                if !completes_on_any_packet(this_addr)
                                    && packet.try_decrypt_sym(&inbound_sym_key).is_err()
                                {
                                    crate::transport::TRANSPORT_METRICS
                                        .record_handshake_unauthenticated_dropped(remote_addr);
                                    // `direction` is "outbound" for every log in
                                    // traverse_nat, including this arm, where we
                                    // are answering the remote's intro.
                                    tracing::debug!(
                                        peer_addr = %remote_addr,
                                        direction = "outbound",
                                        arm = "remote_inbound",
                                        "Dropped a handshake packet not encrypted under \
                                         this attempt's inbound key"
                                    );
                                    resume_receive = true;
                                    continue;
                                }
                                #[cfg(test)]
                                record_remote_inbound_completion(this_addr, remote_addr);
                                let (inbound_sender, inbound_recv) =
                                    mpsc::channel(INBOUND_CHANNEL_CAPACITY);

                                // Initialize congestion controller (FixedRate by default, configurable for benchmarks)
                                let congestion_controller = congestion_config
                                    .clone()
                                    .unwrap_or_default()
                                    .build_arc_with_time_source(time_source.clone());

                                // Initialize token bucket at congestion controller's
                                // rate to avoid initial burst. See #3702.
                                let initial_rate = if let Some(ref global) = global_bandwidth {
                                    global.register_connection()
                                } else if let Some(limit) = bandwidth_limit {
                                    limit
                                } else {
                                    congestion_controller.current_rate(TOKEN_BUCKET_WINDOW)
                                };
                                let bucket_capacity =
                                    (initial_rate / 10).max(MIN_BUCKET_CAPACITY_BYTES);
                                let token_bucket = Arc::new(TokenBucket::new_with_time_source(
                                    bucket_capacity,
                                    initial_rate,
                                    time_source.clone(),
                                ));

                                tracing::info!(
                                    peer_addr = %remote_addr,
                                    attempts,
                                    direction = "outbound",
                                    "Outbound handshake completed (inbound ack path)"
                                );
                                let rolling_rtt_stats =
                                    super::rolling_rtt_stats::RollingRttStatsHandle::new(
                                        remote_addr,
                                        time_source.clone(),
                                    );
                                return Ok((
                                    RemoteConnection {
                                        outbound_symmetric_key: outbound_sym_key
                                            .expect("should be set at this stage"),
                                        remote_addr,
                                        sent_tracker: Arc::new(parking_lot::Mutex::new(
                                            SentPacketTracker::new_with_time_source(
                                                time_source.clone(),
                                            ),
                                        )),
                                        last_packet_id: Arc::new(AtomicU32::new(0)),
                                        inbound_packet_recv: inbound_recv,
                                        inbound_symmetric_key: inbound_sym_key,
                                        inbound_symmetric_key_bytes: inbound_sym_key_bytes,
                                        my_address: None,
                                        remote_protoc_version,
                                        transport_secret_key: transport_secret_key.clone(),
                                        congestion_controller,
                                        token_bucket,
                                        socket: socket.clone(),
                                        global_bandwidth: global_bandwidth.clone(),
                                        rolling_rtt_stats,
                                        time_source: time_source.clone(),
                                    },
                                    InboundRemoteConnection {
                                        inbound_packet_sender: inbound_sender,
                                    },
                                ));
                            }
                        }
                    }
                    Some(None) => {
                        tracing::debug!(
                            bind_addr = %this_addr,
                            peer_addr = %remote_addr,
                            direction = "outbound",
                            "Connection closed"
                        );
                        return Err(TransportError::ConnectionClosed(remote_addr));
                    }
                    None => {
                        tracing::trace!(
                            bind_addr = %this_addr,
                            peer_addr = %remote_addr,
                            direction = "outbound",
                            "Failed to receive UDP response in time, retrying"
                        );
                    }
                }

                time_source.sleep(INITIAL_INTERVAL).await;
            }

            obs.set(NatAttemptObservation::DEADLINE);
            let elapsed_nanos = time_source.now_nanos().saturating_sub(start_time_nanos);
            let elapsed_ms = elapsed_nanos / 1_000_000;
            // The returned error follows the LAST send: if it failed, the
            // send error is the most specific explanation available. If it
            // succeeded, the remote was reachable from our side at the end and
            // simply did not answer, so the attempt ends as before. Either way
            // the warn line carries the failure count and the last OS error,
            // so sends that failed mid-attempt are visible in release builds.
            if let (true, Some(err)) = (
                send_failures.last_send_failed,
                send_failures.last_error.as_ref(),
            ) {
                tracing::warn!(
                    peer_addr = %remote_addr,
                    attempts,
                    failed_sends = send_failures.count,
                    elapsed_ms,
                    direction = "outbound",
                    error = %err,
                    "Outbound handshake failed: sends to the remote are failing"
                );
                return Err(TransportError::send_failed(remote_addr, err.kind()));
            }
            tracing::warn!(
                peer_addr = %remote_addr,
                attempts,
                failed_sends = send_failures.count,
                last_send_error = %send_failures.last_error_text(),
                elapsed_ms,
                direction = "outbound",
                "Outbound handshake failed: max connection attempts reached"
            );
            Err(TransportError::ConnectionEstablishmentFailure {
                cause: "max connection attempts reached".into(),
            })
        };
        (f.boxed(), inbound_from_remote, observation)
    }
}

/// Whether the listener bound at `this_addr` completes a hole-punch
/// handshake on ANY non-intro packet, as releases before this check did.
/// Always `false` outside tests; tests set it per address to run a peer with
/// the earlier behaviour against one with the check (interop in both
/// directions).
#[cfg(not(test))]
fn completes_on_any_packet(_this_addr: SocketAddr) -> bool {
    false
}

#[cfg(test)]
fn completes_on_any_packet(this_addr: SocketAddr) -> bool {
    LEGACY_HANDSHAKE_COMPLETION.contains(&this_addr)
}

/// Test-only: `(this_addr, remote_addr)` of every handshake completed in
/// `traverse_nat`'s `RemoteInbound` arm, so tests can check which arm ran.
#[cfg(test)]
static REMOTE_INBOUND_COMPLETIONS: std::sync::LazyLock<
    parking_lot::Mutex<Vec<(SocketAddr, SocketAddr)>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
fn record_remote_inbound_completion(this_addr: SocketAddr, remote_addr: SocketAddr) {
    REMOTE_INBOUND_COMPLETIONS
        .lock()
        .push((this_addr, remote_addr));
}

/// Test-only: whether `this_addr` completed a handshake with `remote_addr`
/// in the `RemoteInbound` arm.
#[cfg(test)]
fn completed_in_remote_inbound(this_addr: SocketAddr, remote_addr: SocketAddr) -> bool {
    REMOTE_INBOUND_COMPLETIONS
        .lock()
        .contains(&(this_addr, remote_addr))
}

/// Test-only: listener addresses that use the earlier completion rule. Keyed
/// by address so concurrent tests (which use unique addresses) do not
/// affect each other.
#[cfg(test)]
static LEGACY_HANDSHAKE_COMPLETION: std::sync::LazyLock<DashSet<SocketAddr>> =
    std::sync::LazyLock::new(DashSet::new);

/// What one outbound NAT traversal attempt saw, for classifying its failure
/// in telemetry. Set from the traversal task, read once the task has ended.
#[derive(Default)]
pub(super) struct NatAttemptObservation(std::sync::atomic::AtomicU8);

impl NatAttemptObservation {
    /// Any packet arrived from the remote.
    const RECEIVED: u8 = 1;
    /// A packet from the remote was decrypted (intro or symmetric).
    const DECRYPTED: u8 = 1 << 1;
    /// The remote's intro carried an incompatible protocol version.
    const VERSION_INCOMPATIBLE: u8 = 1 << 2;
    /// The attempt ran to its deadline.
    const DEADLINE: u8 = 1 << 3;

    fn set(&self, flag: u8) {
        self.0.fetch_or(flag, std::sync::atomic::Ordering::Relaxed);
    }

    fn flags(&self) -> u8 {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Map a failed traversal to exactly one [`NatFailureKind`].
fn classify_nat_failure(
    error: &TransportError,
    flags: u8,
) -> crate::transport::metrics::NatFailureKind {
    use crate::transport::metrics::NatFailureKind;
    type O = NatAttemptObservation;
    // A known version mismatch is the actionable cause however the attempt
    // ended (e.g. a later send failure), so it is checked first.
    if flags & O::VERSION_INCOMPATIBLE != 0 {
        return NatFailureKind::VersionIncompatible;
    }
    match error {
        TransportError::ProtocolVersionMismatch { .. } => NatFailureKind::VersionIncompatible,
        TransportError::Udp(UdpError(UdpErrorKind::SendFailed(..))) => NatFailureKind::SendError,
        TransportError::ChannelClosed
        | TransportError::ConnectionClosed(_)
        | TransportError::ConnectionEstablishmentFailure { .. }
        | TransportError::OutboundStreamFailed(_)
        | TransportError::OutboundStreamInvalidLength(..)
        | TransportError::Udp(UdpError(UdpErrorKind::IO(_)))
        | TransportError::Other(_)
        | TransportError::Udp(UdpError(UdpErrorKind::PubKeyDecryptionError(_)))
        | TransportError::Udp(UdpError(UdpErrorKind::Serialization(_)))
        // The UDP handshake never produces a QUIC error.
        | TransportError::Quic(_) => {
            if flags & O::DEADLINE == 0 {
                NatFailureKind::Other
            } else if flags & O::RECEIVED == 0 {
                NatFailureKind::NoResponse
            } else if flags & O::DECRYPTED == 0 {
                NatFailureKind::Undecryptable
            } else {
                NatFailureKind::Incomplete
            }
        }
    }
}

#[cfg(test)]
mod nat_failure_classification_tests {
    use super::*;
    use crate::transport::metrics::NatFailureKind;

    type O = NatAttemptObservation;

    fn deadline() -> TransportError {
        TransportError::ConnectionEstablishmentFailure {
            cause: "max connection attempts reached".into(),
        }
    }

    #[test]
    fn classification_table() {
        let addr: SocketAddr = "192.0.2.1:1".parse().unwrap();
        let cases = [
            (deadline(), O::DEADLINE, NatFailureKind::NoResponse),
            (
                deadline(),
                O::DEADLINE | O::RECEIVED,
                NatFailureKind::Undecryptable,
            ),
            (
                deadline(),
                O::DEADLINE | O::RECEIVED | O::DECRYPTED,
                NatFailureKind::Incomplete,
            ),
            (
                deadline(),
                O::DEADLINE | O::RECEIVED | O::DECRYPTED | O::VERSION_INCOMPATIBLE,
                NatFailureKind::VersionIncompatible,
            ),
            (
                TransportError::ProtocolVersionMismatch {
                    expected: "a".into(),
                    actual: "b".into(),
                },
                0,
                NatFailureKind::VersionIncompatible,
            ),
            (
                TransportError::send_failed(addr, std::io::ErrorKind::NetworkUnreachable),
                O::DEADLINE,
                NatFailureKind::SendError,
            ),
            // An error before the deadline (channel closed, bad ack) is not a
            // reachability verdict, whatever was received.
            (
                TransportError::ConnectionClosed(addr),
                O::RECEIVED,
                NatFailureKind::Other,
            ),
            (deadline(), O::RECEIVED, NatFailureKind::Other),
            // A version-incompatible remote is classified as such even when
            // the attempt ended before its deadline.
            (
                TransportError::ConnectionClosed(addr),
                O::RECEIVED | O::DECRYPTED | O::VERSION_INCOMPATIBLE,
                NatFailureKind::VersionIncompatible,
            ),
            // ...and even when the attempt then ended on a failed send.
            (
                TransportError::send_failed(addr, std::io::ErrorKind::NetworkUnreachable),
                O::DEADLINE | O::RECEIVED | O::DECRYPTED | O::VERSION_INCOMPATIBLE,
                NatFailureKind::VersionIncompatible,
            ),
        ];
        for (error, flags, expected) in cases {
            assert_eq!(
                classify_nat_failure(&error, flags),
                expected,
                "{error:?} / {flags:#b}"
            );
        }
    }

    #[test]
    fn observation_flags_accumulate() {
        let o = NatAttemptObservation::default();
        assert_eq!(o.flags(), 0);
        o.set(O::RECEIVED);
        o.set(O::DEADLINE);
        o.set(O::RECEIVED);
        assert_eq!(o.flags(), O::RECEIVED | O::DEADLINE);
    }
}

/// Send failures seen during one `traverse_nat` attempt (#5871).
#[derive(Default)]
struct HandshakeSendFailures {
    /// Failed sends so far in this attempt.
    count: usize,
    /// The most recent OS error, kept after a later send succeeds so the
    /// attempt's final log line can still name it.
    last_error: Option<std::io::Error>,
    /// Whether the most recent send failed. Decides the returned error.
    last_send_failed: bool,
}

impl HandshakeSendFailures {
    /// Record one send's outcome. Returns true if the send succeeded.
    fn record(
        &mut self,
        result: std::io::Result<usize>,
        remote_addr: SocketAddr,
        packet: &'static str,
    ) -> bool {
        match result {
            Ok(_) => {
                self.last_send_failed = false;
                true
            }
            Err(err) => {
                tracing::debug!(
                    peer_addr = %remote_addr,
                    direction = "outbound",
                    packet,
                    error = %err,
                    "Handshake send failed, re-sending on the next pass"
                );
                self.count += 1;
                self.last_send_failed = true;
                self.last_error = Some(err);
                false
            }
        }
    }

    fn last_error_text(&self) -> String {
        self.last_error
            .as_ref()
            .map_or_else(|| "none".to_string(), ToString::to_string)
    }
}

/// Turn a failed handshake `send_to` into the error the handshake returns.
///
/// Logs the full OS error first, with the destination, because `SendFailed`
/// keeps only its `ErrorKind`, and some kinds (`Uncategorized`) name nothing.
/// Before #5871 these sends mapped every error to `ChannelClosed` and the OS
/// error was lost. No consumer of a handshake result classifies it with
/// `is_transient`; that predicate is for established connections.
///
/// Inbound (gateway) failures log at `info!`: an unauthenticated intro with a
/// spoofed source address can trigger them, limited only by the per-IP intro
/// rate limit, so they must not flood the warn level.
fn handshake_send_failed(
    remote_addr: SocketAddr,
    direction: &'static str,
    err: std::io::Error,
) -> TransportError {
    if direction == "inbound" {
        tracing::info!(
            peer_addr = %remote_addr,
            direction,
            error = %err,
            "Handshake send failed"
        );
    } else {
        tracing::warn!(
            peer_addr = %remote_addr,
            direction,
            error = %err,
            "Handshake send failed"
        );
    }
    TransportError::send_failed(remote_addr, err.kind())
}

/// Build the success ack for a peer whose protocol version we have just parsed
/// from its intro packet, choosing the encoding by version (#5161).
///
/// Both acceptors — the gateway path and the peer-to-peer `RemoteInbound` path —
/// go through here so the gate cannot drift between them, and so the reasoning
/// below lives in one place.
///
/// The decision reads ONLY the version parsed during THIS handshake, never a
/// cached one, which is what makes it impossible for the gate to go stale
/// against a peer that restarted on an older build at a reused address.
///
/// `None` (or below the floor) yields the unmodified `ack_ok`, so a pre-floor
/// peer receives byte-identical traffic to today. That matters more here than on
/// the other version-gated surfaces: a peer that cannot decode this particular
/// message does not merely drop a connection, it fails to complete the handshake
/// at all, so it could not connect to an upgraded gateway.
fn build_success_ack(
    outbound_sym_key: &Aes128Gcm,
    our_inbound_key: [u8; 16],
    remote_addr: SocketAddr,
    remote_protoc_version: Option<(u8, u8, u16)>,
    ack_version_floor: (u8, u8, u16),
) -> Result<PacketData<SymmetricAES>, bincode::Error> {
    if version_cmp::version_supports_ack_version(remote_protoc_version, ack_version_floor) {
        SymmetricMessage::ack_ok_with_version(
            outbound_sym_key,
            our_inbound_key,
            remote_addr,
            PROTOC_VERSION,
        )
    } else {
        SymmetricMessage::ack_ok(outbound_sym_key, our_inbound_key, remote_addr)
    }
}

/// Destructure a successful connection ack, whichever encoding it arrived in
/// (#5161), into `(symmetric key, our observed address, acceptor's version
/// bytes)`.
///
/// `None` for anything that is not a success ack — including the failure ack,
/// which the caller must still handle separately.
///
/// The version is `None` for the legacy [`SymmetricMessagePayload::AckConnection`]
/// and `Some` for [`SymmetricMessagePayload::AckConnectionV2`]. That difference
/// is the entire point of the variant, and it is deliberately reported as an
/// `Option` rather than defaulted: "the acceptor is below the floor" and "the
/// acceptor is version X" are different facts, and every `version_supports_*`
/// gate fails closed on the former.
#[allow(clippy::type_complexity)]
fn ack_ok_parts(
    payload: &SymmetricMessagePayload,
) -> Option<([u8; 16], SocketAddr, Option<[u8; 8]>)> {
    match payload {
        SymmetricMessagePayload::AckConnection {
            result:
                Ok(OutboundConnection {
                    key,
                    remote_addr: my_address,
                }),
        } => Some((*key, *my_address, None)),
        SymmetricMessagePayload::AckConnectionV2 { connection } => Some((
            connection.key,
            connection.remote_addr,
            Some(connection.protoc_version),
        )),
        SymmetricMessagePayload::AckConnection { result: Err(_) }
        | SymmetricMessagePayload::ShortMessage { .. }
        | SymmetricMessagePayload::StreamFragment { .. }
        | SymmetricMessagePayload::NoOp
        | SymmetricMessagePayload::Ping { .. }
        | SymmetricMessagePayload::Pong { .. }
        | SymmetricMessagePayload::Upgrade(_) => None,
    }
}

fn handle_ack_connection_error(err: Cow<'static, str>) -> TransportError {
    if let Some(expected) = err.split("expected version").nth(1) {
        let remote_version_str = expected.trim();
        // Check if this old peer's rejection actually means we need to update.
        // Old peers do strict exact-match. If they're compatible with our range
        // but just don't support range-checking, we should NOT self-update.
        if !version_cmp::old_peer_rejection_means_we_must_update(
            remote_version_str,
            &PROTOC_VERSION,
        ) {
            tracing::info!(
                remote_version = remote_version_str,
                "Old peer rejected us (strict match) but is within our compatible range — not triggering self-update"
            );
            return TransportError::ProtocolVersionMismatch {
                expected: remote_version_str.to_string(),
                actual: PCK_VERSION.to_string(),
            };
        }
        // Remote is genuinely newer — signal version mismatch
        crate::transport::signal_version_mismatch();
        TransportError::ProtocolVersionMismatch {
            expected: remote_version_str.to_string(),
            actual: PCK_VERSION.to_string(),
        }
    } else {
        TransportError::ConnectionEstablishmentFailure { cause: err }
    }
}

/// Number of symmetric-key bytes carried by an asymmetric intro packet
/// (AES-128 key for the reverse direction of the handshake).
const ASYM_INTRO_SYM_KEY_LEN: usize = 16;

/// Minimum decrypted length of a well-formed asymmetric intro packet:
/// `PROTOC_VERSION` (8 bytes) followed by the 16-byte symmetric key.
const ASYM_INTRO_MIN_LEN: usize = PROTOC_VERSION.len() + ASYM_INTRO_SYM_KEY_LEN;

/// Split a decrypted asymmetric intro payload into the protocol-version
/// prefix and the symmetric-key bytes, bounds-checking the length first.
///
/// Asymmetric encryption to our public key does NOT authenticate the
/// sender — the key is public, so any peer can encrypt an arbitrarily
/// short payload to us. Slicing such a payload directly (`data[..8]` /
/// `data[8..24]`) would panic with an index-out-of-bounds, crashing the
/// node: a remote, unauthenticated denial of service (#4406). Returning
/// `None` here lets the caller drop the packet silently per the transport
/// decryption-failure rule.
fn split_asym_intro_payload(data: &[u8]) -> Option<([u8; 8], [u8; ASYM_INTRO_SYM_KEY_LEN])> {
    if data.len() < ASYM_INTRO_MIN_LEN {
        return None;
    }
    let protoc_bytes: [u8; 8] = data[..PROTOC_VERSION.len()]
        .try_into()
        .expect("slice is PROTOC_VERSION.len() bytes after length check");
    let key_bytes: [u8; ASYM_INTRO_SYM_KEY_LEN] = data[PROTOC_VERSION.len()..ASYM_INTRO_MIN_LEN]
        .try_into()
        .expect("slice is ASYM_INTRO_SYM_KEY_LEN bytes after length check");
    Some((protoc_bytes, key_bytes))
}

fn key_from_addr(addr: &SocketAddr) -> [u8; 16] {
    let current_time = chrono::Utc::now();
    let mut hasher = blake3::Hasher::new();
    match addr {
        SocketAddr::V4(v4) => {
            hasher.update(&v4.ip().octets());
            hasher.update(&v4.port().to_le_bytes());
        }
        SocketAddr::V6(v6) => {
            hasher.update(&v6.ip().octets());
            hasher.update(&v6.port().to_le_bytes());
        }
    }
    hasher.update(
        current_time
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp()
            .to_le_bytes()
            .as_ref(),
    );
    RANDOM_U64.with(|&random_value| {
        hasher.update(random_value.as_ref());
    });
    hasher.finalize().as_bytes()[..16].try_into().unwrap()
}

pub(crate) enum ConnectionEvent<S = UdpSocket, TS: TimeSource = RealTime> {
    ConnectionStart {
        remote_public_key: TransportPublicKey,
        open_connection: oneshot::Sender<Result<RemoteConnection<S, TS>, TransportError>>,
        /// Why the node started this attempt (telemetry only).
        origin: NatAttemptOrigin,
    },
}

/// Move the packets a gateway handshake buffered after its ACK into the new
/// connection's channel, stamped as the listener stamps packets
/// (`InboundPacket`). Returns how many were forwarded; stops at the first
/// that does not fit.
///
/// They all get the one stamp of the moment they are forwarded: they arrived
/// during the handshake, before anything else on the new channel, so the
/// stamps on that channel still never decrease. The shared stamp counts
/// them as arriving together, which can raise the inbound concurrency
/// estimate of a new gateway connection.
fn forward_handshake_buffered<T: TimeSource>(
    next_inbound: &mut mpsc::Receiver<PacketData<UnknownEncryption>>,
    inbound_packet_tx: &mpsc::Sender<InboundPacket>,
    time_source: &T,
    remote_addr: SocketAddr,
) -> usize {
    let mut forwarded_count = 0;
    let queued_nanos = time_source.now_nanos();
    while let Ok(packet) = next_inbound.try_recv() {
        let packet = InboundPacket {
            packet,
            queued_nanos,
        };
        if inbound_packet_tx.try_send(packet).is_err() {
            tracing::warn!(
                peer_addr = %remote_addr,
                "Failed to forward handshake-buffered packet to connection channel (channel full)"
            );
            break;
        }
        forwarded_count += 1;
    }
    forwarded_count
}

struct InboundRemoteConnection {
    inbound_packet_sender: mpsc::Sender<InboundPacket>,
}

mod recv_errors;
mod stray_intro;
mod version_cmp;

/// Smoke tests pinning the contract the transport layer relies on from
/// `tokio::sync::mpsc`. These tests replace the wrapper-level tests that
/// were deleted along with `fast_channel.rs` in #3960 — they exercise the
/// same scenarios (basic send/recv, backpressure, sender clone, disconnect
/// detection, no-busy-loop wakeup, burst drain, multi-producer concurrency,
/// stress under contention, sender drop while receiver parked) directly
/// against the primitive the transport now uses, so any future regression
/// (e.g. swapping back to a wrapper that wedges) trips a clear unit test.
#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "bench"))]
pub mod mock_transport {
    #![allow(clippy::single_range_in_vec_init)]

    use std::{
        fmt::Debug,
        net::Ipv4Addr,
        ops::Range,
        sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering},
    };

    use dashmap::DashMap;
    use futures::{TryStreamExt, stream::FuturesOrdered};
    use rand::{Rng, SeedableRng};
    use serde::{Serialize, de::DeserializeOwned};
    use tokio::sync::Mutex;
    use tracing::info;

    use super::*;
    #[allow(unused_imports)] // Used in some test configurations
    use crate::transport::packet_data::MAX_DATA_SIZE;

    #[test]
    fn test_handle_ack_connection_error() {
        let err_msg =
            "remote is using a different protocol version, expected version 1.2.3".to_string();
        match handle_ack_connection_error(err_msg.into()) {
            TransportError::ProtocolVersionMismatch { expected, actual } => {
                assert_eq!(expected, "1.2.3");
                assert_eq!(actual, PCK_VERSION);
            }
            TransportError::ChannelClosed
            | TransportError::ConnectionClosed(_)
            | TransportError::ConnectionEstablishmentFailure { .. }
            | TransportError::Udp(UdpError(UdpErrorKind::SendFailed(..)))
            | TransportError::OutboundStreamFailed(_)
            | TransportError::OutboundStreamInvalidLength(..)
            | TransportError::Udp(UdpError(UdpErrorKind::IO(_)))
            | TransportError::Other(_)
            | TransportError::Udp(UdpError(UdpErrorKind::PubKeyDecryptionError(_)))
            | TransportError::Udp(UdpError(UdpErrorKind::Serialization(_)))
            | TransportError::Quic(_) => {
                panic!("Expected ProtocolVersionMismatch error")
            }
        }

        let err_msg_no_version = "remote is using a different protocol version".to_string();
        match handle_ack_connection_error(err_msg_no_version.clone().into()) {
            TransportError::ConnectionEstablishmentFailure { cause } => {
                assert_eq!(cause, err_msg_no_version);
            }
            TransportError::ChannelClosed
            | TransportError::ConnectionClosed(_)
            | TransportError::ProtocolVersionMismatch { .. }
            | TransportError::Udp(UdpError(UdpErrorKind::SendFailed(..)))
            | TransportError::OutboundStreamFailed(_)
            | TransportError::OutboundStreamInvalidLength(..)
            | TransportError::Udp(UdpError(UdpErrorKind::IO(_)))
            | TransportError::Other(_)
            | TransportError::Udp(UdpError(UdpErrorKind::PubKeyDecryptionError(_)))
            | TransportError::Udp(UdpError(UdpErrorKind::Serialization(_)))
            | TransportError::Quic(_) => {
                panic!("Expected ConnectionEstablishmentFailure error")
            }
        }
    }

    /// Channel mapping for mock socket communication between peers.
    pub type Channels = Arc<DashMap<SocketAddr, mpsc::UnboundedSender<MockDatagram>>>;

    /// A datagram on its way to a `MockSocket`.
    pub struct MockDatagram {
        from: SocketAddr,
        data: Vec<u8>,
        /// VirtualTime at which the packet becomes available (0 = immediate).
        available_at_nanos: u64,
        /// Tie-break between equal arrival times: the link model's own
        /// scheduling order, or the global send order without a link model.
        order: u64,
    }

    /// Send order for sockets without a link model.
    static SEND_ORDER: AtomicU64 = AtomicU64::new(0);

    /// How many queued packets `recv_from` moves off its channel per pass
    /// (code-style.md, per-iteration caps). It bounds the work of one pass, so
    /// a sender on another runtime thread cannot keep the loop draining
    /// indefinitely. The cost: delivery is in arrival order among parked
    /// packets only, so a due packet may be delivered ahead of one still in
    /// the channel that was due earlier. On the current-thread runtime the
    /// WAN bench uses, nothing is sent while the loop runs, so this cannot
    /// happen there.
    const MAX_INBOUND_DRAIN: usize = 64;

    /// Policy for simulating packet loss in mock transport.
    #[derive(Default, Clone)]
    pub enum PacketDropPolicy {
        /// Receive all packets without dropping
        #[default]
        ReceiveAll,
        /// Drop the packets randomly based on the factor (0.0 to 1.0)
        Factor(f64),
        /// Drop packets with the given packet index ranges
        Ranges(Vec<Range<usize>>),
    }

    /// Policy for simulating network delay in mock transport.
    #[derive(Clone, Default)]
    pub enum PacketDelayPolicy {
        /// No artificial delay (instant delivery)
        #[default]
        NoDelay,
        /// Fixed delay for all packets
        Fixed(Duration),
        /// Uniform random delay between min and max
        Uniform { min: Duration, max: Duration },
    }

    /// Mock socket implementation for testing transport without real network I/O.
    pub struct MockSocket {
        /// Inbound packets: (source_addr, data, available_at_nanos)
        inbound: Mutex<mpsc::UnboundedReceiver<MockDatagram>>,
        this: SocketAddr,
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        num_packets_sent: AtomicUsize,
        rng: std::sync::Mutex<rand::rngs::SmallRng>,
        channels: Channels,
        /// Optional VirtualTime for instant delay simulation in benchmarks.
        /// When set, packet delivery waits until VirtualTime reaches available_at_nanos.
        virtual_time: Option<crate::simulation::VirtualTime>,
        /// When set, the link model decides every packet's fate and replaces
        /// the drop and delay policies. Requires `virtual_time`.
        link: Option<Arc<crate::transport::link_model::LinkModel>>,
        /// Packets taken off `inbound` whose arrival time has not come yet,
        /// ordered by (arrival, order). Held on the socket so a `recv_from`
        /// cancelled while waiting (the listener polls it inside a
        /// `select!`) loses nothing, and ordered by arrival so a packet from
        /// one peer never waits behind a later-arriving one from another.
        /// Packets from one source never reorder: each is held to no earlier
        /// than the previous one from that source, which matters for
        /// `PacketDelayPolicy::Uniform` (the link model is FIFO per path
        /// already).
        pending_inbound: std::sync::Mutex<PendingInbound>,
    }

    /// A VirtualTime sleep kept across `recv_from` calls.
    type PendingSleep = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

    /// A parked inbound packet: (arrival nanos, order, source, bytes).
    type ParkedPacket = std::cmp::Reverse<(u64, u64, SocketAddr, Vec<u8>)>;

    /// See `MockSocket::pending_inbound`.
    #[derive(Default)]
    struct PendingInbound {
        heap: std::collections::BinaryHeap<ParkedPacket>,
        last_arrival_from: std::collections::HashMap<SocketAddr, u64>,
        /// The sleep for the earliest parked packet, kept across calls so a
        /// re-entered or cancelled `recv_from` reuses it instead of
        /// registering another VirtualTime wakeup for the same deadline.
        /// When an earlier packet arrives and the deadline changes, the old
        /// sleep is dropped but its wakeup stays registered (VirtualTime has
        /// no deregistration): it costs one extra driver step and a spurious
        /// wake when its deadline passes, nothing else.
        wait: Option<(u64, PendingSleep)>,
    }

    impl MockSocket {
        /// Create a new MockSocket with the given configuration.
        pub async fn new(
            packet_drop_policy: PacketDropPolicy,
            packet_delay_policy: PacketDelayPolicy,
            addr: SocketAddr,
            channels: Channels,
        ) -> Self {
            let (outbound, inbound) = mpsc::unbounded_channel();
            channels.insert(addr, outbound);
            static SEED: AtomicU64 = AtomicU64::new(0xfeedbeef);
            MockSocket {
                inbound: Mutex::new(inbound),
                this: addr,
                packet_drop_policy,
                packet_delay_policy,
                num_packets_sent: AtomicUsize::new(0),
                rng: std::sync::Mutex::new(rand::rngs::SmallRng::seed_from_u64(
                    SEED.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                )),
                channels,
                virtual_time: None,
                link: None,
                pending_inbound: std::sync::Mutex::new(PendingInbound::default()),
            }
        }

        /// Create a new MockSocket with VirtualTime support for instant delay simulation.
        ///
        /// When `time_source` is provided, packet delays advance virtual time instead of
        /// waiting wall-clock time. This enables instant simulation of high-RTT scenarios.
        pub async fn with_time_source(
            packet_drop_policy: PacketDropPolicy,
            packet_delay_policy: PacketDelayPolicy,
            addr: SocketAddr,
            channels: Channels,
            time_source: Option<crate::simulation::VirtualTime>,
        ) -> Self {
            let (outbound, inbound) = mpsc::unbounded_channel();
            channels.insert(addr, outbound);
            static SEED: AtomicU64 = AtomicU64::new(0xfeedbeef);
            MockSocket {
                inbound: Mutex::new(inbound),
                this: addr,
                packet_drop_policy,
                packet_delay_policy,
                num_packets_sent: AtomicUsize::new(0),
                rng: std::sync::Mutex::new(rand::rngs::SmallRng::seed_from_u64(
                    SEED.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                )),
                channels,
                virtual_time: time_source,
                link: None,
                pending_inbound: std::sync::Mutex::new(PendingInbound::default()),
            }
        }
    }

    impl Socket for MockSocket {
        async fn bind(_addr: SocketAddr) -> Result<Self, std::io::Error> {
            unimplemented!()
        }

        async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            // Cancel-safe: every packet taken off the channel is parked in
            // `pending_inbound` before any await, and leaves it only when it
            // is delivered. Only this socket's listener calls recv_from.
            let mut inbound = self.inbound.try_lock().unwrap();
            loop {
                for _ in 0..MAX_INBOUND_DRAIN {
                    let Ok(packet) = inbound.try_recv() else {
                        break;
                    };
                    self.park_inbound(packet);
                }
                let next_at = self
                    .pending()
                    .heap
                    .peek()
                    .map(|std::cmp::Reverse((at, ..))| *at);
                match (next_at, &self.virtual_time) {
                    (Some(at), Some(vt)) if at > vt.now_nanos() => {
                        // Wait for its arrival time, unless something that
                        // may arrive earlier comes in first. `biased` keeps
                        // the polling order fixed for reproducibility.
                        let mut closed = false;
                        tokio::select! {
                            biased;
                            packet = inbound.recv() => match packet {
                                Some(packet) => self.park_inbound(packet),
                                None => closed = true,
                            },
                            _ = self.wait_until(vt, at) => {}
                        }
                        if closed {
                            self.wait_until(vt, at).await;
                        }
                    }
                    (Some(_), _) => {
                        let Some(std::cmp::Reverse((_, _, remote, packet))) =
                            self.pending().heap.pop()
                        else {
                            continue;
                        };
                        buf[..packet.len()].copy_from_slice(&packet[..]);
                        return Ok((packet.len(), remote));
                    }
                    (None, _) => match inbound.recv().await {
                        Some(packet) => self.park_inbound(packet),
                        None => {
                            tracing::error!(this = %self.this, "connection closed");
                            return Err(std::io::ErrorKind::ConnectionAborted.into());
                        }
                    },
                }
            }
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            if self.link.is_some() {
                let result = self.send_via_link(buf, target);
                tokio::task::yield_now().await;
                return result;
            }
            let (delay, available_at_nanos) = self.compute_delay_and_timestamp();

            // Send packet with delivery timestamp (don't advance VirtualTime here)
            let result = self.send_packet_internal(buf, target, available_at_nanos);

            // For non-VirtualTime with delay, use wall-clock sleep
            if self.virtual_time.is_none() && !delay.is_zero() {
                tokio::time::sleep(delay).await;
            } else {
                // Just yield to let receiver process
                tokio::task::yield_now().await;
            }

            result
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            if self.link.is_some() {
                return self.send_via_link(buf, target);
            }
            let (_delay, available_at_nanos) = self.compute_delay_and_timestamp();
            self.send_packet_internal(buf, target, available_at_nanos)
        }
    }

    impl MockSocket {
        fn pending(&self) -> std::sync::MutexGuard<'_, PendingInbound> {
            self.pending_inbound
                .lock()
                .unwrap_or_else(|e| e.into_inner())
        }

        fn park_inbound(&self, packet: MockDatagram) {
            let mut pending = self.pending();
            let last = pending.last_arrival_from.entry(packet.from).or_insert(0);
            let at = packet.available_at_nanos.max(*last);
            *last = at;
            pending.heap.push(std::cmp::Reverse((
                at,
                packet.order,
                packet.from,
                packet.data,
            )));
        }

        /// Resolves once VirtualTime reaches `at`, reusing the sleep left by
        /// an earlier call for the same deadline.
        fn wait_until<'a>(
            &'a self,
            vt: &'a crate::simulation::VirtualTime,
            at: u64,
        ) -> impl std::future::Future<Output = ()> + 'a {
            std::future::poll_fn(move |cx| {
                let mut pending = self.pending();
                if pending.wait.as_ref().map(|(deadline, _)| *deadline) != Some(at) {
                    pending.wait = Some((at, vt.sleep_until(at)));
                }
                let Some((_, sleep)) = pending.wait.as_mut() else {
                    return std::task::Poll::Pending;
                };
                let ready = sleep.as_mut().poll(cx);
                if ready.is_ready() {
                    pending.wait = None;
                }
                ready
            })
        }

        /// Route a packet through the link model, which decides whether and
        /// when it arrives. A dropped packet still reports success, like UDP.
        fn send_via_link(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            let (Some(link), Some(vt)) = (&self.link, &self.virtual_time) else {
                return Err(std::io::ErrorKind::Unsupported.into());
            };
            assert!(self.this != target, "cannot send to self");
            // No socket at the target: nothing to deliver to, so the link
            // does not record a delivery that never happens.
            let Some(sender) = self.channels.get(&target).map(|v| v.value().clone()) else {
                return Ok(0);
            };
            for arrival in link.on_send(vt.now_nanos(), self.this, target, buf) {
                sender
                    .send(MockDatagram {
                        from: self.this,
                        data: buf.to_vec(),
                        available_at_nanos: arrival.at_nanos,
                        order: arrival.seq,
                    })
                    .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            }
            Ok(buf.len())
        }

        /// Compute delay and delivery timestamp based on packet delay policy.
        fn compute_delay_and_timestamp(&self) -> (Duration, u64) {
            let delay = match &self.packet_delay_policy {
                PacketDelayPolicy::NoDelay => Duration::ZERO,
                PacketDelayPolicy::Fixed(delay) => *delay,
                PacketDelayPolicy::Uniform { min, max } => {
                    let range = max.as_nanos().saturating_sub(min.as_nanos());
                    if range == 0 {
                        *min
                    } else {
                        let random_nanos = {
                            let mut rng = self.rng.try_lock().unwrap();
                            rng.random::<u128>() % range
                        };
                        *min + Duration::from_nanos(random_nanos as u64)
                    }
                }
            };

            // Calculate delivery timestamp based on current VirtualTime + delay
            let available_at_nanos = if let Some(ref vt) = self.virtual_time {
                vt.now_nanos() + delay.as_nanos() as u64
            } else {
                0 // No VirtualTime, deliver immediately
            };

            (delay, available_at_nanos)
        }

        /// Internal packet send with explicit delivery timestamp.
        fn send_packet_internal(
            &self,
            buf: &[u8],
            target: SocketAddr,
            available_at_nanos: u64,
        ) -> std::io::Result<usize> {
            let packet_idx = self.num_packets_sent.fetch_add(1, Ordering::Release);
            match &self.packet_drop_policy {
                PacketDropPolicy::ReceiveAll => {}
                PacketDropPolicy::Factor(factor) => {
                    if *factor > self.rng.try_lock().unwrap().random::<f64>() {
                        tracing::trace!(id=%packet_idx, %self.this, "drop packet");
                        return Ok(buf.len());
                    }
                }
                PacketDropPolicy::Ranges(ranges) => {
                    if ranges.iter().any(|r| r.contains(&packet_idx)) {
                        tracing::trace!(id=%packet_idx, %self.this, "drop packet");
                        return Ok(buf.len());
                    }
                }
            }

            assert!(self.this != target, "cannot send to self");
            let Some(sender) = self.channels.get(&target).map(|v| v.value().clone()) else {
                return Ok(0);
            };
            sender
                .send(MockDatagram {
                    from: self.this,
                    data: buf.to_vec(),
                    available_at_nanos,
                    order: SEND_ORDER.fetch_add(1, Ordering::Relaxed),
                })
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }
    }

    impl Drop for MockSocket {
        fn drop(&mut self) {
            self.channels.remove(&self.this);
        }
    }

    /// Create a mock peer connection for testing/benchmarking.
    ///
    /// Returns the peer's public key, outbound connection handler, and socket address.
    pub async fn create_mock_peer(
        packet_drop_policy: PacketDropPolicy,
        channels: Channels,
    ) -> anyhow::Result<(
        TransportPublicKey,
        OutboundConnectionHandler<MockSocket>,
        SocketAddr,
    )> {
        create_mock_peer_internal(
            packet_drop_policy,
            PacketDelayPolicy::NoDelay,
            false,
            channels,
            None,
            None,
        )
        .await
        .map(|(pk, (o, _), s)| (pk, o, s))
    }

    /// Create a mock peer connection with custom delay policy for testing/benchmarking.
    ///
    /// Returns the peer's public key, outbound connection handler, and socket address.
    pub async fn create_mock_peer_with_delay(
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        channels: Channels,
    ) -> anyhow::Result<(
        TransportPublicKey,
        OutboundConnectionHandler<MockSocket>,
        SocketAddr,
    )> {
        create_mock_peer_internal(
            packet_drop_policy,
            packet_delay_policy,
            false,
            channels,
            None,
            None,
        )
        .await
        .map(|(pk, (o, _), s)| (pk, o, s))
    }

    /// Create a mock peer connection with custom delay and bandwidth limit for testing/benchmarking.
    ///
    /// Returns the peer's public key, outbound connection handler, and socket address.
    pub async fn create_mock_peer_with_bandwidth(
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        channels: Channels,
        bandwidth_limit: Option<usize>,
    ) -> anyhow::Result<(
        TransportPublicKey,
        OutboundConnectionHandler<MockSocket>,
        SocketAddr,
    )> {
        create_mock_peer_internal(
            packet_drop_policy,
            packet_delay_policy,
            false,
            channels,
            bandwidth_limit,
            None,
        )
        .await
        .map(|(pk, (o, _), s)| (pk, o, s))
    }

    /// Create a mock peer connection with virtual time support for benchmarking.
    ///
    /// When `time_source` is provided, the entire transport stack uses VirtualTime
    /// for all timing operations. Delays advance virtual time instead of waiting
    /// real time. This enables instant simulation of high-RTT scenarios
    /// (e.g., 300ms RTT completes in ~1ms wall time).
    ///
    /// Returns the peer's public key, outbound connection handler, and socket address.
    pub async fn create_mock_peer_with_virtual_time(
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        channels: Channels,
        time_source: crate::simulation::VirtualTime,
    ) -> anyhow::Result<(
        TransportPublicKey,
        OutboundConnectionHandler<MockSocket, crate::simulation::VirtualTime>,
        SocketAddr,
    )> {
        create_mock_peer_internal_vt(
            packet_drop_policy,
            packet_delay_policy,
            false,
            channels,
            None,
            Some(time_source),
        )
        .await
        .map(|(pk, (o, _), s)| (pk, o, s))
    }

    /// Create a mock gateway connection for testing/benchmarking.
    ///
    /// Returns the gateway's public key, outbound handler, inbound connection receiver, and socket address.
    pub async fn create_mock_gateway(
        packet_drop_policy: PacketDropPolicy,
        channels: Channels,
    ) -> Result<
        (
            TransportPublicKey,
            (
                OutboundConnectionHandler<MockSocket>,
                mpsc::Receiver<PeerConnection<MockSocket>>,
            ),
            SocketAddr,
        ),
        anyhow::Error,
    > {
        create_mock_peer_internal(
            packet_drop_policy,
            PacketDelayPolicy::NoDelay,
            true,
            channels,
            None,
            None,
        )
        .await
    }

    /// Create a mock (non-gateway) peer whose version-carrying-ack floor (#5161)
    /// is overridden, with a caller-chosen packet-drop policy.
    ///
    /// Both parameters are needed together for the peer-to-peer arm:
    /// `create_mock_gateway_with_ack_floor` hardcodes `gateway = true`, so
    /// without this no NAT-traversal pair could carry a floor override — and the
    /// drop policy is what forces the intro/ack RACE that arm exists to handle.
    pub async fn create_mock_peer_with_ack_floor(
        packet_drop_policy: PacketDropPolicy,
        channels: Channels,
        ack_version_floor_override: Option<(u8, u8, u16)>,
    ) -> anyhow::Result<(
        TransportPublicKey,
        OutboundConnectionHandler<MockSocket>,
        SocketAddr,
    )> {
        create_mock_peer_internal(
            packet_drop_policy,
            PacketDelayPolicy::NoDelay,
            false,
            channels,
            None,
            ack_version_floor_override,
        )
        .await
        .map(|(pk, (o, _), s)| (pk, o, s))
    }

    /// Create a mock gateway whose version-carrying-ack floor (#5161) is
    /// overridden, so a test can exercise BOTH sides of the gate without
    /// depending on the crate's current version.
    pub async fn create_mock_gateway_with_ack_floor(
        channels: Channels,
        ack_version_floor_override: Option<(u8, u8, u16)>,
    ) -> Result<
        (
            TransportPublicKey,
            (
                OutboundConnectionHandler<MockSocket>,
                mpsc::Receiver<PeerConnection<MockSocket>>,
            ),
            SocketAddr,
        ),
        anyhow::Error,
    > {
        create_mock_peer_internal(
            PacketDropPolicy::ReceiveAll,
            PacketDelayPolicy::NoDelay,
            true,
            channels,
            None,
            ack_version_floor_override,
        )
        .await
    }

    /// One port allocator for every mock-peer constructor, so peers built by
    /// different constructors never share an address (a collision could let
    /// an address-keyed assertion pass on another test's traffic).
    static MOCK_PEER_PORT: AtomicU16 = AtomicU16::new(25000);

    #[allow(clippy::too_many_arguments)]
    async fn create_mock_peer_internal(
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        gateway: bool,
        channels: Channels,
        bandwidth_limit: Option<usize>,
        ack_version_floor_override: Option<(u8, u8, u16)>,
    ) -> Result<
        (
            TransportPublicKey,
            (
                OutboundConnectionHandler<MockSocket>,
                mpsc::Receiver<PeerConnection<MockSocket>>,
            ),
            SocketAddr,
        ),
        anyhow::Error,
    > {
        let peer_keypair = TransportKeypair::new();
        let peer_pub = peer_keypair.public.clone();
        let port = MOCK_PEER_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let socket = Arc::new(
            MockSocket::new(
                packet_drop_policy,
                packet_delay_policy,
                (Ipv4Addr::LOCALHOST, port).into(),
                channels,
            )
            .await,
        );
        let (peer_conn, inbound_conn) = OutboundConnectionHandler::new_test_with_options(
            (Ipv4Addr::LOCALHOST, port).into(),
            socket,
            peer_keypair,
            gateway,
            bandwidth_limit,
            ack_version_floor_override,
        )
        .expect("failed to create peer");
        Ok((
            peer_pub,
            (peer_conn, inbound_conn),
            (Ipv4Addr::LOCALHOST, port).into(),
        ))
    }

    /// Internal function with VirtualTime support for instant delay simulation.
    ///
    /// When time_source is provided, the entire transport stack uses VirtualTime
    /// for all timing operations (MockSocket delays, protocol timers, retransmit checks).
    async fn create_mock_peer_internal_vt(
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        gateway: bool,
        channels: Channels,
        bandwidth_limit: Option<usize>,
        time_source: Option<crate::simulation::VirtualTime>,
    ) -> Result<
        (
            TransportPublicKey,
            (
                OutboundConnectionHandler<MockSocket, crate::simulation::VirtualTime>,
                mpsc::Receiver<PeerConnection<MockSocket, crate::simulation::VirtualTime>>,
            ),
            SocketAddr,
        ),
        anyhow::Error,
    > {
        create_mock_peer_internal_vt_with_congestion(
            packet_drop_policy,
            packet_delay_policy,
            gateway,
            channels,
            bandwidth_limit,
            time_source,
            None,
            None,
        )
        .await
    }

    /// Internal function with VirtualTime and congestion control support.
    ///
    /// This is the most flexible internal peer creation function, supporting:
    /// - VirtualTime for instant delay simulation
    /// - Custom congestion control algorithm selection (BBR or LEDBAT)
    #[allow(clippy::too_many_arguments)]
    async fn create_mock_peer_internal_vt_with_congestion(
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        gateway: bool,
        channels: Channels,
        bandwidth_limit: Option<usize>,
        time_source: Option<crate::simulation::VirtualTime>,
        congestion_config: Option<CongestionControlConfig>,
        link: Option<Arc<crate::transport::link_model::LinkModel>>,
    ) -> Result<
        (
            TransportPublicKey,
            (
                OutboundConnectionHandler<MockSocket, crate::simulation::VirtualTime>,
                mpsc::Receiver<PeerConnection<MockSocket, crate::simulation::VirtualTime>>,
            ),
            SocketAddr,
        ),
        anyhow::Error,
    > {
        let peer_keypair = TransportKeypair::new();
        let peer_pub = peer_keypair.public.clone();
        let port = MOCK_PEER_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Use provided time source or create a new one
        let vt = time_source.unwrap_or_default();

        let mut socket = MockSocket::with_time_source(
            packet_drop_policy,
            packet_delay_policy,
            (Ipv4Addr::LOCALHOST, port).into(),
            channels,
            Some(vt.clone()),
        )
        .await;
        socket.link = link;
        let socket = Arc::new(socket);

        // Use the same VirtualTime for both MockSocket and protocol layer
        let (peer_conn, inbound_conn) = OutboundConnectionHandler::new_test_with_congestion_config(
            (Ipv4Addr::LOCALHOST, port).into(),
            socket,
            peer_keypair,
            gateway,
            bandwidth_limit,
            vt,
            congestion_config,
        )
        .expect("failed to create peer");
        Ok((
            peer_pub,
            (peer_conn, inbound_conn),
            (Ipv4Addr::LOCALHOST, port).into(),
        ))
    }

    /// Create a mock peer connection with VirtualTime and custom congestion control.
    ///
    /// This function supports selecting the congestion control algorithm
    /// for benchmarking purposes. Pass `None` for `congestion_config` to use the
    /// default (`CongestionControlAlgorithm::default()`, FixedRate).
    ///
    /// Returns the peer's public key, outbound connection handler, and socket address.
    pub async fn create_mock_peer_with_congestion_config(
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        channels: Channels,
        time_source: crate::simulation::VirtualTime,
        congestion_config: Option<CongestionControlConfig>,
    ) -> anyhow::Result<(
        TransportPublicKey,
        OutboundConnectionHandler<MockSocket, crate::simulation::VirtualTime>,
        SocketAddr,
    )> {
        create_mock_peer_internal_vt_with_congestion(
            packet_drop_policy,
            packet_delay_policy,
            false,
            channels,
            None,
            Some(time_source),
            congestion_config,
            None,
        )
        .await
        .map(|(pk, (o, _), s)| (pk, o, s))
    }

    /// Create a mock peer whose every packet goes through `link`, a shared
    /// [`LinkModel`](crate::transport::link_model::LinkModel) that applies a
    /// bottleneck queue, propagation delay, loss and scheduled spikes on
    /// virtual time. The socket's own drop and delay policies are unused.
    pub async fn create_mock_peer_with_link_model(
        link: Arc<crate::transport::link_model::LinkModel>,
        channels: Channels,
        time_source: crate::simulation::VirtualTime,
        congestion_config: Option<CongestionControlConfig>,
    ) -> anyhow::Result<(
        TransportPublicKey,
        OutboundConnectionHandler<MockSocket, crate::simulation::VirtualTime>,
        SocketAddr,
    )> {
        create_mock_peer_internal_vt_with_congestion(
            PacketDropPolicy::ReceiveAll,
            PacketDelayPolicy::NoDelay,
            false,
            channels,
            None,
            Some(time_source),
            congestion_config,
            Some(link),
        )
        .await
        .map(|(pk, (o, _), s)| (pk, o, s))
    }

    #[cfg(test)]
    mod link_socket_tests {
        use futures::FutureExt;

        use super::*;
        use crate::simulation::VirtualTime;
        use crate::transport::link_model::{HostLink, LinkEvent, LinkModel, Loss};

        async fn linked_socket(
            channels: &Channels,
            vt: &VirtualTime,
            link: &Arc<LinkModel>,
        ) -> (MockSocket, SocketAddr) {
            let port = MOCK_PEER_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
            let mut socket = MockSocket::with_time_source(
                PacketDropPolicy::ReceiveAll,
                PacketDelayPolicy::NoDelay,
                addr,
                channels.clone(),
                Some(vt.clone()),
            )
            .await;
            socket.link = Some(link.clone());
            (socket, addr)
        }

        fn fixture() -> (VirtualTime, Arc<LinkModel>, Channels) {
            let link = LinkModel::new(
                HostLink {
                    uplink_bps: 0,
                    queue_bytes: 0,
                    one_way_delay: Duration::from_millis(100),
                    loss: Loss::None,
                },
                1,
            );
            (VirtualTime::new(), Arc::new(link), Arc::new(DashMap::new()))
        }

        /// The listener polls `recv_from` inside a `select!`, so it is
        /// routinely dropped while waiting for a packet's arrival time.
        #[tokio::test]
        async fn recv_from_keeps_a_packet_when_cancelled_mid_wait() {
            let (vt, link, channels) = fixture();
            let (a, a_addr) = linked_socket(&channels, &vt, &link).await;
            let (b, b_addr) = linked_socket(&channels, &vt, &link).await;
            a.send_to(b"hello", b_addr).await.unwrap();
            let mut buf = [0u8; 64];
            assert!(
                b.recv_from(&mut buf).now_or_never().is_none(),
                "not due for 100 ms"
            );
            vt.advance(Duration::from_millis(100));
            let (n, from) = b
                .recv_from(&mut buf)
                .now_or_never()
                .expect("due now, and must not have been lost by the cancelled call")
                .unwrap();
            assert_eq!((&buf[..n], from), (&b"hello"[..], a_addr));
        }

        /// `PacketDelayPolicy::Uniform` draws an independent delay per packet,
        /// but packets from one source must still arrive in the order sent.
        #[tokio::test]
        async fn uniform_delay_keeps_each_sources_order() {
            let vt = VirtualTime::new();
            let channels: Channels = Arc::new(DashMap::new());
            let mut sockets = Vec::new();
            for _ in 0..2 {
                let port = MOCK_PEER_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
                let socket = MockSocket::with_time_source(
                    PacketDropPolicy::ReceiveAll,
                    PacketDelayPolicy::Uniform {
                        min: Duration::from_millis(1),
                        max: Duration::from_millis(100),
                    },
                    addr,
                    channels.clone(),
                    Some(vt.clone()),
                )
                .await;
                sockets.push((socket, addr));
            }
            let (a, _) = &sockets[0];
            let (b, b_addr) = &sockets[1];
            for i in 0..50u8 {
                a.send_to(&[i], *b_addr).await.unwrap();
            }
            vt.advance(Duration::from_millis(200));
            let mut buf = [0u8; 8];
            let got: Vec<u8> = (0..50)
                .map(|_| {
                    let (n, _) = b.recv_from(&mut buf).now_or_never().expect("due").unwrap();
                    assert_eq!(n, 1);
                    buf[0]
                })
                .collect();
            assert_eq!(got, (0..50).collect::<Vec<u8>>());
        }

        /// Re-entering `recv_from` for the same not-yet-due packet reuses one
        /// VirtualTime wakeup instead of registering a new one each time.
        #[tokio::test]
        async fn waiting_recv_from_registers_one_wakeup_per_deadline() {
            let (vt, link, channels) = fixture();
            let (a, _) = linked_socket(&channels, &vt, &link).await;
            let (b, b_addr) = linked_socket(&channels, &vt, &link).await;
            a.send_to(b"x", b_addr).await.unwrap();
            let mut buf = [0u8; 8];
            let before = vt.pending_wakeup_count();
            for _ in 0..10 {
                assert!(b.recv_from(&mut buf).now_or_never().is_none());
            }
            assert_eq!(vt.pending_wakeup_count(), before + 1);
        }

        /// Equal arrival times are ordered by the link model's sequence
        /// number, not by the order the packets reached the channel (two
        /// concurrent `send_to` calls can enqueue in the opposite order to
        /// the one the model decided).
        #[tokio::test]
        async fn equal_arrivals_follow_the_models_order_not_the_channels() {
            let (vt, link, channels) = fixture();
            let (_b, b_addr) = linked_socket(&channels, &vt, &link).await;
            let b = &_b;
            // A sorts after C by address, so neither address nor channel
            // order can produce the expected result; only `order` can.
            let a_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 2).into();
            let c_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 1).into();
            let tx = channels.get(&b_addr).unwrap().value().clone();
            for (from, data, order) in [(c_addr, b"second", 8), (a_addr, b"first!", 7)] {
                tx.send(MockDatagram {
                    from,
                    data: data.to_vec(),
                    available_at_nanos: 100,
                    order,
                })
                .unwrap();
            }
            vt.advance(Duration::from_nanos(100));
            let mut buf = [0u8; 8];
            for (want, from) in [(&b"first!"[..], a_addr), (&b"second"[..], c_addr)] {
                let (n, got_from) = b.recv_from(&mut buf).now_or_never().expect("due").unwrap();
                assert_eq!((&buf[..n], got_from), (want, from));
            }
        }

        /// FIFO is per path: a packet from one peer must not wait behind a
        /// later-arriving packet from another.
        #[tokio::test]
        async fn a_delayed_packet_does_not_block_another_peers_earlier_one() {
            let (vt, link, channels) = fixture();
            let (a, a_addr) = linked_socket(&channels, &vt, &link).await;
            let (b, b_addr) = linked_socket(&channels, &vt, &link).await;
            let (c, c_addr) = linked_socket(&channels, &vt, &link).await;
            link.schedule(LinkEvent::DelaySpike {
                start: Duration::ZERO,
                duration: Duration::from_secs(1),
                extra_delay: Duration::from_secs(1),
                from: Some(a_addr),
            });
            a.send_to(b"late", b_addr).await.unwrap();
            c.send_to(b"early", b_addr).await.unwrap();
            let mut buf = [0u8; 64];
            vt.advance(Duration::from_millis(100));
            let (n, from) = b.recv_from(&mut buf).now_or_never().expect("due").unwrap();
            assert_eq!((&buf[..n], from), (&b"early"[..], c_addr));
            assert!(b.recv_from(&mut buf).now_or_never().is_none());
            vt.advance(Duration::from_secs(1));
            let (n, from) = b.recv_from(&mut buf).now_or_never().expect("due").unwrap();
            assert_eq!((&buf[..n], from), (&b"late"[..], a_addr));
        }
    }

    // Test infrastructure for multi-peer communication tests (reserved for future use)
    #[allow(dead_code)]
    trait TestFixture: Clone + Send + Sync + 'static {
        type Message: DeserializeOwned + Serialize + Send + Debug + 'static;
        fn expected_iterations(&self) -> usize;
        fn gen_msg(&mut self) -> Self::Message;
        fn assert_message_ok(&self, peer_idx: usize, msg: Self::Message) -> bool;
    }

    #[allow(dead_code)]
    struct TestConfig {
        packet_drop_policy: PacketDropPolicy,
        peers: usize,
        wait_time: Duration,
    }

    impl Default for TestConfig {
        fn default() -> Self {
            Self {
                packet_drop_policy: PacketDropPolicy::ReceiveAll,
                peers: 2,
                wait_time: Duration::from_secs(10), // Increased for CI reliability
            }
        }
    }

    #[allow(dead_code)]
    async fn run_test<G: TestFixture>(
        config: TestConfig,
        generators: Vec<G>,
    ) -> anyhow::Result<()> {
        assert_eq!(generators.len(), config.peers);
        let mut peer_keys_and_addr = vec![];
        let mut peer_conns = vec![];
        let channels = Arc::new(DashMap::new());
        for _ in 0..config.peers {
            let (peer_pub, peer, peer_addr) =
                create_mock_peer(config.packet_drop_policy.clone(), channels.clone()).await?;
            peer_keys_and_addr.push((peer_pub, peer_addr));
            peer_conns.push(peer);
        }

        let mut tasks = vec![];
        let barrier = Arc::new(tokio::sync::Barrier::new(config.peers));
        for (i, (mut peer, test_generator)) in peer_conns.into_iter().zip(generators).enumerate() {
            let mut peer_keys_and_addr = peer_keys_and_addr.clone();
            peer_keys_and_addr.remove(i);
            let barrier_cp = barrier.clone();
            let peer = GlobalExecutor::spawn(async move {
                let mut conns = FuturesOrdered::new();
                let mut establish_conns = Vec::new();
                barrier_cp.wait().await;
                for (peer_pub, peer_addr) in &peer_keys_and_addr {
                    let peer_conn = tokio::time::timeout(
                        Duration::from_secs(30), // Increased connection timeout for CI reliability
                        peer.connect(peer_pub.clone(), *peer_addr).await,
                    );
                    establish_conns.push(peer_conn);
                }
                let connections = futures::future::try_join_all(establish_conns)
                    .await?
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?;
                // additional wait time so we can clear up any additional messages that may need to be sent
                let extra_wait = if config.wait_time.as_secs() > 10 {
                    Duration::from_secs(3)
                } else {
                    Duration::from_millis(200)
                };
                for ((_, peer_addr), mut peer_conn) in
                    peer_keys_and_addr.into_iter().zip(connections)
                {
                    let mut test_gen_cp = test_generator.clone();
                    conns.push_back(async move {
                        let mut messages = vec![];
                        let mut to = tokio::time::interval(config.wait_time);
                        to.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        to.tick().await;
                        let start = tokio::time::Instant::now();
                        let iters = test_gen_cp.expected_iterations();
                        while messages.len() < iters {
                            peer_conn.send(test_gen_cp.gen_msg()).await?;
                            let msg = tokio::select! {
                                _ = to.tick() => {
                                    return Err::<_, anyhow::Error>(
                                        anyhow::anyhow!(
                                            "timeout waiting for messages, total time: {time:.2}; done iters {iter}",
                                            time = start.elapsed().as_secs_f64(),
                                            iter = messages.len()
                                        )
                                    );
                                }
                                msg = peer_conn.recv() => {
                                    msg
                                }
                            };
                            match msg {
                                Ok(msg) => {
                                    let output_as_str: G::Message = bincode::deserialize(&msg)?;
                                    messages.push(output_as_str);
                                    info!("{peer_addr:?} received {} messages", messages.len());
                                }
                                Err(error) => {
                                    tracing::error!(%error, "error receiving message");
                                    return Err(error.into());
                                }
                            }
                        }
                        let _sent = peer_conn.send(test_gen_cp.gen_msg()).await;

                        tracing::info!(%peer_addr, "finished");
                        let _recv = tokio::time::timeout(extra_wait, peer_conn.recv()).await;
                        Ok(messages)
                    });
                }
                let results = conns.try_collect::<Vec<_>>().await?;
                Ok::<_, anyhow::Error>((results, test_generator))
            });
            tasks.push(peer);
        }

        let all_results = futures::future::try_join_all(tasks)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        for (peer_results, test_gen) in all_results {
            for (idx, result) in peer_results.into_iter().enumerate() {
                assert_eq!(result.len(), test_gen.expected_iterations());
                for msg in result {
                    assert!(test_gen.assert_message_ok(idx, msg));
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn simulate_nat_traversal() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(Default::default(), channels).await?;

        // Expected negotiated version is our own compiled PROTOC_VERSION, since
        // both mock peers run the same build.
        let expected_version = version_cmp::parse_version_bytes(&PROTOC_VERSION).version;

        let peer_b = GlobalExecutor::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let conn = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, anyhow::Error>(conn.remote_version())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let conn = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            Ok::<_, anyhow::Error>(conn.remote_version())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        let a_version = a?;
        let b_version = b?;

        // This is a symmetric peer-to-peer hole-punch (no gateway), so each side
        // captures the remote version while parsing the other's intro packet on
        // the RemoteInbound path. Whichever side races ahead to the AckConnection
        // path instead reports None here, because both mock peers run this build
        // and this build is below `GATEWAY_ACK_VERSION_MIN_VERSION` — so we
        // require any captured version to match and at least one side to capture
        // it. `nat_traversal_ack_racer_learns_version_from_the_ack` is the test
        // that forces that race deliberately, with the floor overridden, and
        // shows the ack now carries a version for the racing side (#5161).
        for v in [a_version, b_version].into_iter().flatten() {
            assert_eq!(v, expected_version);
        }
        assert!(
            a_version.is_some() || b_version.is_some(),
            "at least one side should capture the remote protocol version on the RemoteInbound path"
        );
        Ok(())
    }

    #[ignore = "should be fixed"]
    #[tokio::test]
    async fn simulate_nat_traversal_drop_first_packets_for_all() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(PacketDropPolicy::Ranges(vec![0..1]), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(PacketDropPolicy::Ranges(vec![0..1]), channels).await?;

        let peer_b = GlobalExecutor::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let _conn = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let _conn = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test]
    async fn simulate_nat_traversal_drop_first_packets_of_peerb() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(PacketDropPolicy::Ranges(vec![0..1]), channels).await?;

        let peer_b = GlobalExecutor::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            let _recv = tokio::time::timeout(Duration::from_secs(3), conn.recv()).await;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            let _recv = tokio::time::timeout(Duration::from_secs(3), conn.recv()).await;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test]
    async fn simulate_nat_traversal_drop_packet_ranges_of_peerb_killed() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer(
            PacketDropPolicy::Ranges(vec![0..1, 3..usize::MAX]),
            channels,
        )
        .await?;

        let peer_b = GlobalExecutor::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(2), peer_a_conn).await??;
            conn.send("some data").await.inspect_err(|error| {
                tracing::error!(%error, "error while sending message to peer a");
            })?;
            tracing::info!("Dropping peer b");
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(2), peer_b_conn).await??;
            let b = tokio::time::timeout(Duration::from_secs(2), conn.recv()).await??;
            // we should receive the message
            assert_eq!(&b[8..], b"some data");
            tracing::info!("Peer a received package from peer b");
            tokio::time::sleep(Duration::from_secs(3)).await;
            // conn should be broken as the remote peer cannot receive message and ping
            // Use timeout to avoid hanging if connection detection is slow
            let res = tokio::time::timeout(Duration::from_secs(10), conn.recv()).await;
            if res.is_err() {
                tracing::error!(
                    "Test timeout: connection detection took longer than 10s - investigate if this causes test failures"
                );
            }
            assert!(res.is_err() || res.unwrap().is_err());
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;

        Ok(())
    }

    #[tokio::test]
    async fn simulate_nat_traversal_drop_packet_ranges_of_peerb() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        // Drop pattern: packet 0 dropped (retry succeeds at count 1), packets 1-2 succeed
        // (backoff resets), packets 3-4 dropped (retry succeeds at count 5).
        // With RFC 6298 exponential backoff: 1s + 2s = 3s for the 3..5 range.
        // Original 3..9 (6 consecutive drops) would take 1+2+4+8+16+32=63s, exceeding
        // reasonable connection timeout - that scenario should fail, not recover.
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(PacketDropPolicy::Ranges(vec![0..1, 3..5]), channels).await?;

        let peer_b = GlobalExecutor::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(15), peer_a_conn)
                .await
                .inspect_err(|_| tracing::error!("peer a timed out"))?
                .inspect_err(|error| tracing::error!(%error, "error while connecting to peer a"))?;
            tracing::info!("Connected peer b to peer a");
            conn.send("some foo").await.inspect_err(|error| {
                tracing::error!(%error, "error while sending 1st message");
            })?;
            tokio::time::sleep(Duration::from_secs(2)).await;
            // although we drop some packets, we still alive
            conn.send("some data").await.inspect_err(|error| {
                tracing::error!(%error, "error while sending 2nd message");
            })?;
            let _recv = tokio::time::timeout(Duration::from_secs(10), conn.recv()).await;
            Ok::<_, anyhow::Error>(conn)
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(15), peer_b_conn)
                .await
                .inspect_err(|_| tracing::error!("peer b timed out"))?
                .inspect_err(|error| tracing::error!(%error, "error while connecting to peer b"))?;
            tracing::info!("Connected peer a to peer b");

            // we should receive the message
            let b = conn.recv().await.inspect_err(|error| {
                tracing::error!(%error, "error while receiving 1st message");
            })?;
            assert_eq!(&b[8..], b"some foo");
            // conn should not be broken
            let b = conn.recv().await.inspect_err(|error| {
                tracing::error!(%error, "error while receiving 2nd message");
            })?;
            assert_eq!(&b[8..], b"some data");
            let _sent = conn.send("complete").await.inspect_err(|error| {
                tracing::error!(%error, "error while sending 3rd message");
            });
            Ok::<_, anyhow::Error>(conn)
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        let _a = a?;
        let _b = b?;

        Ok(())
    }

    #[tokio::test]
    async fn simulate_gateway_connection() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            create_mock_gateway(Default::default(), channels).await?;

        let expected_version = version_cmp::parse_version_bytes(&PROTOC_VERSION).version;

        let gw = GlobalExecutor::spawn(async move {
            let gw_conn = gw_conn.recv();
            let gw = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            // Gateway/acceptor side captures the joiner's version from its intro packet.
            Ok::<_, anyhow::Error>(gw.remote_version())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(gw_pub, gw_addr).await;
            let conn = tokio::time::timeout(Duration::from_secs(60), peer_b_conn).await??;
            // Joiner->gateway side: the gateway now tells us its version in the
            // ack (#5161), gated on ours. Same build here, and the sim/test
            // default floor is unreachable-high only when explicitly set — this
            // constructor leaves the production floor in place.
            Ok::<_, anyhow::Error>(conn.remote_version())
        });

        let (joiner_view, gw_view) = tokio::try_join!(peer_a, gw)?;
        assert_eq!(
            gw_view?,
            Some(expected_version),
            "gateway should capture the joiner's protocol version"
        );
        // Both mock peers run this build, so whether the joiner learns the
        // gateway's version here depends purely on whether this build is at or
        // above the production floor. Asserting a bare `None` would make this
        // test flip at the 0.2.120 release.
        //
        // The expectation is derived by comparing the crate version against the
        // floor DIRECTLY, not via `version_supports_ack_version` — that is the
        // predicate under test, and computing the expectation with it makes the
        // assertion tautological. It was written that way first, and a
        // reviewer's mutation showed that the catastrophic regression (a gateway
        // emitting V2 to EVERY peer regardless of version, breaking every
        // pre-0.2.120 joiner's handshake) left the test green.
        let joiner_view = joiner_view?;
        let crate_at_or_above_floor =
            expected_version >= version_cmp::GATEWAY_ACK_VERSION_MIN_VERSION;
        assert_eq!(
            joiner_view.is_some(),
            crate_at_or_above_floor,
            "joiner's view of the gateway version must follow the production floor exactly"
        );
        Ok(())
    }

    /// The joiner learns its external address from the gateway's handshake
    /// ack, and that report reaches the NAT-mapping telemetry. Asserted on
    /// this test's gateway address, so concurrent tests cannot satisfy it.
    #[tokio::test]
    async fn gateway_ack_reports_observed_external_addr() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_pub, mut peer, _peer_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            create_mock_gateway(Default::default(), channels).await?;
        let gw = GlobalExecutor::spawn(async move {
            tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!("no connection"))
                .map(|_| ())
        });
        let joiner = GlobalExecutor::spawn(async move {
            let conn = peer.connect(gw_pub, gw_addr).await;
            let _connection = tokio::time::timeout(Duration::from_secs(60), conn).await??;
            Ok::<_, anyhow::Error>(())
        });
        let (j, g) = tokio::try_join!(joiner, gw)?;
        j?;
        g?;
        assert!(
            crate::transport::TRANSPORT_METRICS.observed_addr_reported_by(gw_addr),
            "the gateway ack's view of our address must be recorded, with the gateway as reporter"
        );
        Ok(())
    }

    /// Through the real handler path: a traversal to a peer that never
    /// answers fails at its deadline and is classified as no-response.
    /// Asserted on this test's (unique) remote address, so concurrent tests
    /// cannot satisfy it; the classification itself is pinned by
    /// `classification_table`.
    #[tokio::test]
    async fn unanswered_nat_traversal_is_classified_no_response() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_pub, mut peer, _peer_addr) =
            create_mock_peer(Default::default(), channels).await?;
        let silent_addr: SocketAddr = "10.255.0.9:9".parse().unwrap();
        let silent_pub = TransportKeypair::new().public().clone();
        let conn = peer.connect(silent_pub, silent_addr).await;
        let result = tokio::time::timeout(Duration::from_secs(30), conn).await?;
        assert!(
            result.is_err(),
            "nobody answers, so the traversal must fail"
        );
        assert_eq!(
            crate::transport::TRANSPORT_METRICS.nat_failures_for(silent_addr),
            vec![crate::transport::metrics::NatFailureKind::NoResponse],
            "exactly one failure, classified as no-response"
        );
        Ok(())
    }

    /// Polls `done` every 10 ms until it holds, failing after 10 s.
    #[cfg(test)]
    async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !done() {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Through the real handler path: the origin given to the connector is
    /// the one the admitted attempt and its failure are recorded under.
    #[tokio::test]
    async fn nat_attempt_is_recorded_under_its_origin() -> anyhow::Result<()> {
        use crate::transport::metrics::NatAttemptOrigin;
        let channels = Arc::new(DashMap::new());
        let (_peer_pub, mut peer, _peer_addr) =
            create_mock_peer(Default::default(), channels).await?;
        let silent_addr: SocketAddr = "10.255.0.10:9".parse().unwrap();
        let silent_pub = TransportKeypair::new().public().clone();
        let conn = peer
            .connect_with_origin(silent_pub, silent_addr, NatAttemptOrigin::ConnectAcceptor)
            .await;
        let result = tokio::time::timeout(Duration::from_secs(30), conn).await?;
        assert!(result.is_err(), "nobody answers");
        let metrics = &crate::transport::TRANSPORT_METRICS;
        assert_eq!(
            metrics.nat_attempt_origins_for(silent_addr),
            vec![NatAttemptOrigin::ConnectAcceptor]
        );
        assert_eq!(
            metrics.nat_failure_origins_for(silent_addr),
            vec![NatAttemptOrigin::ConnectAcceptor]
        );
        Ok(())
    }

    /// Through the real handler path: when both sides punch, each records
    /// the attempt and its establishment under the origin it dialled with.
    #[tokio::test]
    async fn established_traversal_is_recorded_under_its_origin() -> anyhow::Result<()> {
        use crate::transport::metrics::NatAttemptOrigin;
        let channels = Arc::new(DashMap::new());
        let (a_pub, mut a, a_addr) = create_mock_peer(Default::default(), channels.clone()).await?;
        let (b_pub, mut b, b_addr) = create_mock_peer(Default::default(), channels).await?;
        let to_a = GlobalExecutor::spawn(async move {
            let conn = b
                .connect_with_origin(a_pub, a_addr, NatAttemptOrigin::ConnectAcceptor)
                .await;
            let conn = tokio::time::timeout(Duration::from_secs(30), conn).await??;
            Ok::<_, anyhow::Error>(conn)
        });
        let to_b = GlobalExecutor::spawn(async move {
            let conn = a
                .connect_with_origin(b_pub, b_addr, NatAttemptOrigin::ConnectJoiner)
                .await;
            let conn = tokio::time::timeout(Duration::from_secs(30), conn).await??;
            Ok::<_, anyhow::Error>(conn)
        });
        let (x, y) = tokio::try_join!(to_a, to_b)?;
        // Held until both sides have established.
        let (_x, _y) = (x?, y?);
        let metrics = &crate::transport::TRANSPORT_METRICS;
        // Each attempt is logged under its remote: B dialled A, A dialled B.
        assert_eq!(
            metrics.nat_attempt_origins_for(a_addr),
            vec![NatAttemptOrigin::ConnectAcceptor]
        );
        assert_eq!(
            metrics.nat_established_origins_for(a_addr),
            vec![NatAttemptOrigin::ConnectAcceptor]
        );
        assert_eq!(
            metrics.nat_attempt_origins_for(b_addr),
            vec![NatAttemptOrigin::ConnectJoiner]
        );
        assert_eq!(
            metrics.nat_established_origins_for(b_addr),
            vec![NatAttemptOrigin::ConnectJoiner]
        );
        Ok(())
    }

    /// Through the real handler path at a non-gateway: an intro from an
    /// unknown address is `Other`; one from another port of an IP being
    /// punched is `SameIpOtherPort`; one from an address whose traversal
    /// just failed is `AfterDeadline`. A non-intro packet is not counted.
    /// Asserted per (unique) source address, so concurrent tests cannot
    /// satisfy it.
    #[tokio::test]
    async fn stray_intros_at_a_non_gateway_are_classified() -> anyhow::Result<()> {
        use crate::transport::crypto::PACKET_TYPE_INTRO;
        use crate::transport::metrics::{NatAttemptOrigin, StrayIntro};
        let channels: Channels = Arc::new(DashMap::new());
        let (_peer_pub, mut peer, peer_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let inject = |from: SocketAddr, first_byte: u8| {
            let mut data = vec![0u8; 64];
            data[0] = first_byte;
            channels
                .get(&peer_addr)
                .expect("the peer's inbound channel")
                .send(MockDatagram {
                    from,
                    data,
                    available_at_nanos: 0,
                    order: SEND_ORDER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                })
                .expect("the peer is listening");
        };
        let metrics = &crate::transport::TRANSPORT_METRICS;
        let unknown: SocketAddr = "10.255.1.1:7".parse().unwrap();
        let not_intro: SocketAddr = "10.255.1.3:7".parse().unwrap();
        let target: SocketAddr = "10.255.1.2:9".parse().unwrap();
        let sibling: SocketAddr = "10.255.1.2:10".parse().unwrap();

        inject(not_intro, 0x7f);
        inject(unknown, PACKET_TYPE_INTRO);
        wait_until("the unknown intro is counted", || {
            !metrics.stray_intros_from(unknown).is_empty()
        })
        .await;

        let conn = peer
            .connect_with_origin(
                TransportKeypair::new().public().clone(),
                target,
                NatAttemptOrigin::RingPeerNoTransport,
            )
            .await;
        wait_until("the traversal is admitted", || {
            !metrics.nat_attempt_origins_for(target).is_empty()
        })
        .await;
        inject(sibling, PACKET_TYPE_INTRO);
        wait_until("the sibling intro is counted", || {
            !metrics.stray_intros_from(sibling).is_empty()
        })
        .await;
        let result = tokio::time::timeout(Duration::from_secs(30), conn).await?;
        assert!(result.is_err(), "nobody answers, so the traversal fails");

        inject(target, PACKET_TYPE_INTRO);
        wait_until("the late intro is counted", || {
            !metrics.stray_intros_from(target).is_empty()
        })
        .await;

        assert_eq!(metrics.stray_intros_from(unknown), vec![StrayIntro::Other]);
        assert_eq!(
            metrics.stray_intros_from(sibling),
            vec![StrayIntro::SameIpOtherPort]
        );
        assert_eq!(
            metrics.stray_intros_from(target),
            vec![StrayIntro::AfterDeadline]
        );
        assert!(
            metrics.stray_intros_from(not_intro).is_empty(),
            "only intro packets are counted"
        );
        Ok(())
    }

    /// **The peer-to-peer half of #5161.** The version-carrying ack is not only
    /// a gateway mechanism: `traverse_nat`'s `RemoteInbound` arm sends an ack
    /// too, and the peer receiving it may have raced ahead to its own
    /// `AckConnection` arm without ever parsing our intro. On that link the ack
    /// is the ONLY channel through which a version can travel.
    ///
    /// Forcing the race is the whole difficulty, and is why this drops peer B's
    /// first packet. Without the drop both peers parse each other's intro on the
    /// `StartOutbound` path and learn versions the old way, so the assertion
    /// holds with or without the fix — the first version of this test did
    /// exactly that and a mutation confirmed it was vacuous. With B's intro
    /// dropped, A never parses one, stays in `StartOutbound`, and receives B's
    /// ack instead: precisely the arm that used to yield `None`.
    ///
    /// The pre-existing `simulate_nat_traversal` cannot cover this either — it
    /// requires only that AT LEAST ONE side captured a version, precisely
    /// because the loser of the race captured nothing.
    #[tokio::test]
    async fn nat_traversal_ack_racer_learns_version_from_the_ack() -> anyhow::Result<()> {
        // A floor every build clears, so this does not move with the crate
        // version.
        const REACHABLE_FLOOR: (u8, u8, u16) = (0, 0, 0);

        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) = create_mock_peer_with_ack_floor(
            PacketDropPolicy::ReceiveAll,
            channels.clone(),
            Some(REACHABLE_FLOOR),
        )
        .await?;
        // Drop B's first outbound packet — its intro — so A cannot learn B's
        // version by parsing one.
        let (peer_b_pub, mut peer_b, peer_b_addr) = create_mock_peer_with_ack_floor(
            PacketDropPolicy::Ranges(vec![0..1]),
            channels,
            Some(REACHABLE_FLOOR),
        )
        .await?;

        let expected_version = version_cmp::parse_version_bytes(&PROTOC_VERSION).version;

        let peer_b = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(
                Duration::from_secs(500),
                peer_b.connect(peer_a_pub, peer_a_addr).await,
            )
            .await??;
            Ok::<_, anyhow::Error>(conn.remote_version())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(
                Duration::from_secs(500),
                peer_a.connect(peer_b_pub, peer_b_addr).await,
            )
            .await??;
            Ok::<_, anyhow::Error>(conn.remote_version())
        });

        let (a_version, b_version) = tokio::try_join!(peer_a, peer_b)?;
        assert_eq!(
            a_version?,
            Some(expected_version),
            "peer A never saw peer B's intro, so B's ack is the only place A \
             could have learned B's version — before #5161 this was None"
        );
        assert_eq!(
            b_version?,
            Some(expected_version),
            "peer B parsed A's intro and should be unaffected"
        );
        Ok(())
    }

    /// **#5161, the fix.** A joiner at or above the floor learns the gateway's
    /// protocol version from the ack — the whole point of the issue, since the
    /// joiner never parses an intro packet from a gateway and previously had no
    /// other channel for it.
    #[tokio::test]
    async fn joiner_learns_gateway_version_when_at_or_above_the_floor() -> anyhow::Result<()> {
        // A floor every build clears, so the assertion does not move with the
        // crate version.
        const REACHABLE_FLOOR: (u8, u8, u16) = (0, 0, 0);

        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            create_mock_gateway_with_ack_floor(channels, Some(REACHABLE_FLOOR)).await?;

        let expected_version = version_cmp::parse_version_bytes(&PROTOC_VERSION).version;

        let gw = GlobalExecutor::spawn(async move {
            let gw = tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(gw.remote_version())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(
                Duration::from_secs(60),
                peer_a.connect(gw_pub, gw_addr).await,
            )
            .await??;
            Ok::<_, anyhow::Error>(conn.remote_version())
        });

        let (joiner_view, gw_view) = tokio::try_join!(peer_a, gw)?;
        assert_eq!(
            joiner_view?,
            Some(expected_version),
            "the joiner MUST record the gateway's version from AckConnectionV2 — \
             without it every version-gated wire feature fails closed on gateway links"
        );
        assert_eq!(
            gw_view?,
            Some(expected_version),
            "the gateway still learns the joiner's version from its intro packet"
        );
        Ok(())
    }

    /// **#5161, the compatibility guarantee.** A gateway must NOT emit
    /// `AckConnectionV2` to a joiner below the floor: such a peer does not carry
    /// the variant index, cannot deserialize the ack, and would fail to complete
    /// the handshake at all.
    ///
    /// Simulated by raising the gateway's floor out of reach, which puts the
    /// same-build joiner below it. The joiner still connects (proving the legacy
    /// path is intact end to end) and learns nothing — exactly today's
    /// behaviour. The byte-level half of this guarantee is
    /// `symmetric_message::test::ack_ok_produces_identical_bytes_to_pre_5161_encoding`.
    #[tokio::test]
    async fn pre_floor_joiner_gets_the_legacy_ack_and_still_connects() -> anyhow::Result<()> {
        // Unreachable by any real build, so the same-build joiner is "pre-floor".
        const UNREACHABLE_FLOOR: (u8, u8, u16) = (255, 255, 65535);

        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            create_mock_gateway_with_ack_floor(channels, Some(UNREACHABLE_FLOOR)).await?;

        let gw = GlobalExecutor::spawn(async move {
            let gw = tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(gw.remote_version())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(
                Duration::from_secs(60),
                peer_a.connect(gw_pub, gw_addr).await,
            )
            .await??;
            Ok::<_, anyhow::Error>((conn.remote_version(), conn.remote_addr()))
        });

        let (joiner_view, gw_view) = tokio::try_join!(peer_a, gw)?;
        let (joiner_remote_version, joiner_remote_addr) = joiner_view?;
        assert_eq!(
            joiner_remote_addr, gw_addr,
            "the pre-floor handshake must still COMPLETE — the legacy ack path is intact"
        );
        assert_eq!(
            joiner_remote_version, None,
            "a pre-floor joiner must be told nothing: it cannot decode AckConnectionV2"
        );
        assert!(
            gw_view?.is_some(),
            "the gateway's own view is unaffected by the gate"
        );
        Ok(())
    }

    #[ignore = "should be fixed"]
    #[tokio::test]
    async fn simulate_gateway_connection_drop_first_packets_of_gateway() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            create_mock_gateway(PacketDropPolicy::Ranges(vec![0..1]), channels.clone()).await?;

        let gw = GlobalExecutor::spawn(async move {
            let gw_conn = gw_conn.recv();
            let _gw = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(gw_pub, gw_addr).await;
            let _conn = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, gw)?;
        a?;
        b?;
        Ok(())
    }

    #[ignore = "should be fixed"]
    #[tokio::test]
    async fn simulate_gateway_connection_drop_first_packets_for_all() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            create_mock_peer(PacketDropPolicy::Ranges(vec![0..1]), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            create_mock_gateway(PacketDropPolicy::Ranges(vec![0..1]), channels).await?;

        let gw = GlobalExecutor::spawn(async move {
            let gw_conn = gw_conn.recv();
            let _gw = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(gw_pub, gw_addr).await;
            let _conn = tokio::time::timeout(Duration::from_secs(10), peer_b_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, gw)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test]
    async fn simulate_gateway_connection_drop_first_packets_of_peer() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            create_mock_peer(PacketDropPolicy::Ranges(vec![0..1]), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            create_mock_gateway(Default::default(), channels).await?;

        let gw = GlobalExecutor::spawn(async move {
            let gw_conn = gw_conn.recv();
            let _gw = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(gw_pub, gw_addr).await;
            let _conn = tokio::time::timeout(Duration::from_secs(10), peer_b_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, gw)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Flaky in CI - connection to remote closed errors"]
    async fn simulate_send_short_message() -> anyhow::Result<()> {
        #[derive(Clone, Copy)]
        struct TestData(&'static str);

        impl TestFixture for TestData {
            type Message = String;
            fn expected_iterations(&self) -> usize {
                10
            }

            fn gen_msg(&mut self) -> Self::Message {
                self.0.to_string()
            }

            fn assert_message_ok(&self, _peer_idx: usize, msg: Self::Message) -> bool {
                msg == "foo"
            }
        }

        run_test(
            TestConfig {
                peers: 10,
                wait_time: Duration::from_secs(60), // Increased timeout for CI reliability
                ..Default::default()
            },
            Vec::from_iter((0..10).map(|_| TestData("foo"))),
        )
        .await
    }

    /// Tests sending a large short message using the high-level send() API.
    /// Uses a simple connection pattern for reliability.
    ///
    /// The max short message size is MAX_DATA_SIZE - SymmetricMessage::short_message_overhead().
    /// We test with a payload just under MAX_DATA_SIZE to verify short message handling
    /// near the boundary. The payload must fit in MAX_DATA_SIZE minus serialization and
    /// SymmetricMessage overhead.
    #[tokio::test]
    async fn simulate_send_max_short_message() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(Default::default(), channels).await?;

        // Use a large payload that will be sent as a short message.
        // Must be small enough that bincode serialization + SymmetricMessage overhead
        // fits within MAX_DATA_SIZE (currently 1171 bytes).
        let test_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        let expected_len = test_data.len();

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(5), peer_b_conn).await??;
            let msg = tokio::time::timeout(Duration::from_secs(5), conn.recv()).await??;
            let deserialized: Vec<u8> = bincode::deserialize(&msg)?;
            assert_eq!(deserialized.len(), expected_len);
            Ok::<_, anyhow::Error>(())
        });

        let peer_b = GlobalExecutor::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(5), peer_a_conn).await??;
            // Small delay to ensure peer_a's recv() is ready before we send.
            // Without this, the send can complete before peer_a starts receiving,
            // causing the message to be lost in the mock transport.
            tokio::time::sleep(Duration::from_millis(100)).await;
            conn.send(test_data).await?;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::join!(peer_a, peer_b);
        a??;
        b??;
        Ok(())
    }

    /// Tests that sending a message larger than MAX_DATA_SIZE fails as expected.
    #[tokio::test]
    async fn simulate_send_max_short_message_plus_1() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(Default::default(), channels).await?;

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(1), peer_b_conn).await??;
            let _recv = tokio::time::timeout(Duration::from_secs(1), conn.recv()).await??;
            Ok::<_, anyhow::Error>(())
        });

        let peer_b = GlobalExecutor::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(1), peer_a_conn).await??;
            let data = vec![0u8; MAX_DATA_SIZE + 1];
            let data =
                tokio::task::spawn_blocking(move || bincode::serialize(&data).unwrap()).await?;
            // This should panic or return an error since data is too large
            conn.outbound_short_message(data).await?;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::join!(peer_a, peer_b);
        // peer_a should get an error (timeout waiting for message)
        assert!(a?.is_err());
        // peer_b task should error when trying to send oversized message
        let b_result = b?;
        assert!(b_result.is_err(), "Expected error from peer_b, got Ok");
        Ok(())
    }

    #[tokio::test]
    async fn simulate_send_streamed_message() -> anyhow::Result<()> {
        #[derive(Clone, Copy)]
        struct TestData(&'static str);

        impl TestFixture for TestData {
            type Message = String;
            fn expected_iterations(&self) -> usize {
                10
            }

            fn gen_msg(&mut self) -> Self::Message {
                self.0.repeat(3000)
            }

            fn assert_message_ok(&self, _: usize, msg: Self::Message) -> bool {
                if self.0 == "foo" {
                    msg.contains("bar") && msg.len() == "bar".len() * 3000
                } else {
                    msg.contains("foo") && msg.len() == "foo".len() * 3000
                }
            }
        }

        run_test(
            TestConfig::default(),
            vec![TestData("foo"), TestData("bar")],
        )
        .await
    }

    #[ignore = "should be fixed"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn simulate_packet_dropping() -> anyhow::Result<()> {
        #[derive(Clone, Copy)]
        struct TestData(&'static str);

        impl TestFixture for TestData {
            type Message = String;
            fn expected_iterations(&self) -> usize {
                10
            }

            fn gen_msg(&mut self) -> Self::Message {
                self.0.repeat(1000)
            }

            fn assert_message_ok(&self, _: usize, msg: Self::Message) -> bool {
                if self.0 == "foo" {
                    msg.contains("bar") && msg.len() == "bar".len() * 1000
                } else {
                    msg.contains("foo") && msg.len() == "foo".len() * 1000
                }
            }
        }

        let mut tests = FuturesOrdered::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        let mut test_no = 0;
        for _ in 0..2 {
            for factor in std::iter::repeat(())
                .map(|_| rng.random::<f64>())
                .filter(|x| *x > 0.05 && *x < 0.25)
                .take(3)
            {
                let wait_time = Duration::from_secs(((factor * 5.0 * 15.0) + 15.0) as u64);
                tracing::info!(
                    "test #{test_no}: packet loss factor: {factor} (wait time: {wait_time})",
                    wait_time = wait_time.as_secs()
                );

                let now = tokio::time::Instant::now();
                tests.push_back(GlobalExecutor::spawn(
                    run_test(
                        TestConfig {
                            packet_drop_policy: PacketDropPolicy::Factor(factor),
                            wait_time,
                            ..Default::default()
                        },
                        vec![TestData("foo"), TestData("bar")],
                    )
                    .inspect(move |r| {
                        let msg = if r.is_ok() {
                            format!(
                                "successfully, total time: {}s (t/o: {}s, factor: {factor:.3})",
                                now.elapsed().as_secs(),
                                wait_time.as_secs()
                            )
                        } else {
                            format!(
                                "with error, total time: {}s (t/o: {}s, factor: {factor:.3})",
                                now.elapsed().as_secs(),
                                wait_time.as_secs()
                            )
                        };
                        if r.is_err() {
                            tracing::error!("test #{test_no} finished {}", msg);
                        } else {
                            tracing::info!("test #{test_no} finished {}", msg);
                        }
                    }),
                ));
                test_no += 1;
            }

            while let Some(result) = tests.next().await {
                result??;
            }
        }

        Ok(())
    }

    /// Test that a peer reconnecting from a different port is handled correctly.
    ///
    /// This simulates the scenario from issue #2235 where:
    /// 1. Peer connects to gateway from port A - succeeds
    /// 2. Peer disconnects (connection dropped)
    /// 3. Peer reconnects from port B (new ephemeral port)
    /// 4. Gateway should recognize this as a new session and accept immediately
    ///
    /// The bug was that the gateway retained stale crypto state, causing AEAD
    /// decryption failures on the new connection.
    #[tokio::test]
    async fn gateway_handles_peer_reconnection_from_different_port() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());

        // Create the gateway
        let (gw_pub, (gw_outbound, mut gw_conn), gw_addr) =
            create_mock_gateway(Default::default(), channels.clone()).await?;

        // Create peer A at port X
        let (_peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;

        tracing::info!(
            "Step 1: Peer A at {} connecting to gateway at {}",
            peer_a_addr,
            gw_addr
        );

        // Peer A connects to gateway
        let gw_task = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!("gateway: no inbound connection"))?;
            tracing::info!("Gateway received connection from {}", conn.remote_addr());
            Ok::<_, anyhow::Error>((gw_conn, conn))
        });

        let peer_a_conn = tokio::time::timeout(
            Duration::from_secs(10),
            peer_a.connect(gw_pub.clone(), gw_addr).await,
        )
        .await??;
        tracing::info!("Peer A connected successfully");

        let (mut gw_conn, _gw_peer_a_conn) = gw_task.await??;

        // Step 2: Simulate peer disconnect by dropping the connection
        // The gateway still has state for the old connection in remote_connections
        tracing::info!("Step 2: Dropping peer A connection");
        drop(peer_a_conn);
        drop(peer_a);

        // Give a moment for cleanup (in reality this could be much longer)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Step 3: Create a new peer at a DIFFERENT port (simulating restart with new ephemeral port)
        // This peer uses the same shared channels so packets can still route
        tracing::info!("Step 3: Creating peer B at different port");
        let (_peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;

        // Verify this is testing the same-IP-different-port scenario
        assert_eq!(
            peer_a_addr.ip(),
            peer_b_addr.ip(),
            "Test requires both peers to have same IP (simulating reconnection)"
        );
        assert_ne!(
            peer_a_addr.port(),
            peer_b_addr.port(),
            "Peer B should have different port than peer A"
        );
        tracing::info!(
            "Peer B created at {} (different from peer A at {})",
            peer_b_addr,
            peer_a_addr
        );

        // Step 4: Peer B connects to gateway - this should work on first attempt
        tracing::info!("Step 4: Peer B connecting to gateway");

        let gw_task = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(5), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!("gateway: no inbound connection for peer B"))?;
            tracing::info!(
                "Gateway received connection from {} (peer B)",
                conn.remote_addr()
            );
            Ok::<_, anyhow::Error>(conn)
        });

        let start = tokio::time::Instant::now();
        let peer_b_conn = tokio::time::timeout(
            Duration::from_secs(5),
            peer_b.connect(gw_pub.clone(), gw_addr).await,
        )
        .await??;
        let elapsed = start.elapsed();

        tracing::info!("Peer B connected in {:?}", elapsed);

        let _gw_peer_b_conn = gw_task.await??;

        // Verify the connection works by checking addresses match
        assert_eq!(peer_b_conn.remote_addr(), gw_addr);

        // The connection should have completed quickly (within the first attempt window)
        // If the bug exists, this would take multiple retries (~12 attempts at ~250ms each = 3 seconds)
        assert!(
            elapsed < Duration::from_secs(2),
            "Connection took {:?}, which suggests multiple retry attempts were needed. \
             This indicates stale crypto state issue from #2235.",
            elapsed
        );

        // Cleanup
        drop(peer_b_conn);
        drop(peer_b);
        drop(gw_outbound);

        Ok(())
    }

    /// Test that multiple peers behind the same NAT (same IP, different ports) can
    /// connect simultaneously without interfering with each other.
    ///
    /// This verifies that the stale connection cleanup (issue #2235) doesn't break
    /// legitimate multi-peer scenarios where multiple devices share the same public IP.
    #[tokio::test]
    async fn multiple_peers_behind_same_nat() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());

        // Create the gateway
        let (gw_pub, (_gw_outbound, mut gw_conn), gw_addr) =
            create_mock_gateway(Default::default(), channels.clone()).await?;

        // Create peer A (first device behind NAT)
        let (_peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;

        // Create peer B (second device behind NAT, same IP different port)
        let (_peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;

        // Verify both peers have same IP but different ports (simulating NAT)
        assert_eq!(
            peer_a_addr.ip(),
            peer_b_addr.ip(),
            "Both peers should have same IP (behind same NAT)"
        );
        assert_ne!(
            peer_a_addr.port(),
            peer_b_addr.port(),
            "Peers should have different ports"
        );

        tracing::info!(
            "Testing multiple peers behind same NAT: Peer A at {}, Peer B at {}",
            peer_a_addr,
            peer_b_addr
        );

        // Step 1: Peer A connects to gateway
        let gw_task_a = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!("gateway: no connection from peer A"))?;
            tracing::info!(
                "Gateway received connection from peer A: {}",
                conn.remote_addr()
            );
            Ok::<_, anyhow::Error>((gw_conn, conn))
        });

        let peer_a_conn = tokio::time::timeout(
            Duration::from_secs(10),
            peer_a.connect(gw_pub.clone(), gw_addr).await,
        )
        .await??;
        tracing::info!("Peer A connected successfully");

        let (mut gw_conn, gw_peer_a_conn) = gw_task_a.await??;

        // Step 2: Peer B connects to gateway (while peer A is still connected)
        let gw_task_b = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!("gateway: no connection from peer B"))?;
            tracing::info!(
                "Gateway received connection from peer B: {}",
                conn.remote_addr()
            );
            Ok::<_, anyhow::Error>((gw_conn, conn))
        });

        let peer_b_conn = tokio::time::timeout(
            Duration::from_secs(10),
            peer_b.connect(gw_pub.clone(), gw_addr).await,
        )
        .await??;
        tracing::info!("Peer B connected successfully");

        let (_gw_conn, gw_peer_b_conn) = gw_task_b.await??;

        // Step 3: Verify BOTH connections are still valid
        // The critical check: peer A's connection should NOT have been disrupted
        // when peer B connected from the same IP
        assert_eq!(
            peer_a_conn.remote_addr(),
            gw_addr,
            "Peer A connection should still be valid"
        );
        assert_eq!(
            peer_b_conn.remote_addr(),
            gw_addr,
            "Peer B connection should be valid"
        );
        assert_eq!(
            gw_peer_a_conn.remote_addr(),
            peer_a_addr,
            "Gateway should still have peer A's connection"
        );
        assert_eq!(
            gw_peer_b_conn.remote_addr(),
            peer_b_addr,
            "Gateway should have peer B's connection"
        );

        tracing::info!("Both peers successfully connected and remain active");

        Ok(())
    }

    /// Create a mock peer at a specific socket address for testing.
    ///
    /// This allows testing scenarios where a peer reconnects from the same IP:port.
    #[allow(dead_code)]
    async fn create_mock_peer_at_addr(
        packet_drop_policy: PacketDropPolicy,
        addr: SocketAddr,
        channels: Channels,
    ) -> anyhow::Result<(
        TransportPublicKey,
        OutboundConnectionHandler<MockSocket>,
        SocketAddr,
    )> {
        let peer_keypair = TransportKeypair::new();
        let peer_pub = peer_keypair.public.clone();
        let socket = Arc::new(
            MockSocket::new(
                packet_drop_policy,
                PacketDelayPolicy::NoDelay,
                addr,
                channels,
            )
            .await,
        );
        let (peer_conn, _inbound_conn) =
            OutboundConnectionHandler::new_test(addr, socket, peer_keypair, false)
                .expect("failed to create peer");
        Ok((peer_pub, peer_conn, addr))
    }

    /// Create a mock gateway at a specific socket address for testing.
    ///
    /// This allows testing scenarios where a gateway restarts at the same IP:port with new keys.
    #[allow(dead_code)]
    async fn create_mock_gateway_at_addr(
        packet_drop_policy: PacketDropPolicy,
        addr: SocketAddr,
        channels: Channels,
    ) -> anyhow::Result<(
        TransportPublicKey,
        (
            OutboundConnectionHandler<MockSocket>,
            mpsc::Receiver<PeerConnection<MockSocket>>,
        ),
        SocketAddr,
    )> {
        let gw_keypair = TransportKeypair::new();
        let gw_pub = gw_keypair.public.clone();
        let socket = Arc::new(
            MockSocket::new(
                packet_drop_policy,
                PacketDelayPolicy::NoDelay,
                addr,
                channels,
            )
            .await,
        );
        let (gw_conn, inbound_conn) =
            OutboundConnectionHandler::new_test(addr, socket, gw_keypair, true)
                .expect("failed to create gateway");
        Ok((gw_pub, (gw_conn, inbound_conn), addr))
    }

    /// Test that a peer reconnecting to a gateway after gateway restart is handled correctly.
    ///
    /// This simulates the scenario from issue #2470 where:
    /// 1. Peer connects to Gateway A (identity X) at gw_addr
    /// 2. Gateway A restarts (simulated by dropping and recreating)
    /// 3. Gateway B (identity Y) starts at the SAME gw_addr
    /// 4. Peer tries to reconnect to gw_addr with Gateway B's public key
    ///
    /// The bug was that the peer retained the old session with Gateway A's encryption keys.
    /// When the peer called connect() again, the old code would return the stale connection
    /// instead of establishing a fresh handshake with Gateway B's new keys.
    ///
    /// The fix clears any existing remote_connections entry when connect() is called,
    /// ensuring a fresh handshake occurs.
    #[tokio::test]
    async fn peer_handles_gateway_restart_same_addr_new_identity() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());

        // Use a fixed port for the gateway to simulate restart at same address
        let gw_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 55555).into();

        // Step 1: Create Gateway A with identity X
        tracing::info!("Step 1: Creating Gateway A (identity X) at {}", gw_addr);
        let (gw_a_pub, (gw_a_outbound, mut gw_a_conn), _) =
            create_mock_gateway_at_addr(Default::default(), gw_addr, channels.clone()).await?;
        tracing::info!("Gateway A public key: {:?}", gw_a_pub);

        // Create the peer at a regular address
        let (_peer_pub, mut peer, _peer_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;

        // Peer connects to Gateway A
        let gw_task = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(10), gw_a_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!(
                    "Gateway A: no inbound connection from peer"
                ))?;
            tracing::info!(
                "Gateway A received connection from peer at {}",
                conn.remote_addr()
            );
            Ok::<_, anyhow::Error>(conn)
        });

        let peer_conn_a = tokio::time::timeout(
            Duration::from_secs(10),
            peer.connect(gw_a_pub.clone(), gw_addr).await,
        )
        .await??;
        tracing::info!("Peer connected to Gateway A successfully");

        let _gw_a_peer_conn = gw_task.await??;

        // Verify the connection works
        assert_eq!(peer_conn_a.remote_addr(), gw_addr);

        // Step 2: Simulate gateway restart by dropping Gateway A
        tracing::info!("Step 2: Simulating Gateway A restart (dropping connection and handler)");
        drop(peer_conn_a);
        drop(gw_a_outbound);

        // Give time for cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Step 3: Create Gateway B with NEW identity (Y) at the SAME address
        tracing::info!(
            "Step 3: Creating Gateway B (NEW identity Y) at SAME address {}",
            gw_addr
        );
        let (gw_b_pub, (_gw_b_outbound, mut gw_b_conn), _) =
            create_mock_gateway_at_addr(Default::default(), gw_addr, channels.clone()).await?;
        tracing::info!("Gateway B public key: {:?}", gw_b_pub);

        // Verify this test is actually testing new identity
        assert_ne!(
            gw_a_pub, gw_b_pub,
            "Gateway B must have different identity than Gateway A"
        );

        // Step 4: Peer reconnects to Gateway B (new identity) at same address
        // This is where the bug manifests:
        // - Peer still has remote_connections entry for gw_addr with Gateway A's session
        // - Without the fix: connect() returns stale connection with wrong keys
        // - With the fix: connect() clears stale entry and does fresh handshake
        tracing::info!("Step 4: Peer reconnecting to Gateway B (new identity) at same address");

        let gw_task = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(5), gw_b_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!(
                    "Gateway B: no inbound connection from peer (bug #2470 - peer didn't clear stale session)"
                ))?;
            tracing::info!(
                "Gateway B received connection from peer at {}",
                conn.remote_addr()
            );
            Ok::<_, anyhow::Error>(conn)
        });

        let start = tokio::time::Instant::now();
        let peer_conn_b = tokio::time::timeout(
            Duration::from_secs(5),
            peer.connect(gw_b_pub.clone(), gw_addr).await,
        )
        .await??;
        let elapsed = start.elapsed();

        tracing::info!("Peer reconnected to Gateway B in {:?}", elapsed);

        let _gw_b_peer_conn = gw_task.await??;

        // Verify the connection works
        assert_eq!(peer_conn_b.remote_addr(), gw_addr);

        // The connection should complete quickly
        assert!(
            elapsed < Duration::from_secs(2),
            "Connection took {:?}, which suggests the peer failed to clear stale session. \
             This is bug #2470: peer doesn't reset session when gateway restarts with new identity.",
            elapsed
        );

        tracing::info!("Test passed: Peer successfully reconnected to restarted gateway");

        Ok(())
    }

    /// Test that a peer reconnecting from the SAME IP:port with a NEW identity is handled correctly.
    ///
    /// This simulates the scenario from issue #2277 where:
    /// 1. Peer A (identity X) connects to gateway from IP:port
    /// 2. Peer A disconnects (peer restarts, clears state, gets new identity)
    /// 3. Peer B (identity Y) tries to connect from SAME IP:port (NAT assigns same mapping)
    /// 4. Gateway should recognize this as a new peer and establish a fresh session
    ///
    /// The bug was that the gateway retained the old session with peer A's encryption keys,
    /// causing peer B's handshake packets to fail decryption and be silently dropped.
    #[tokio::test]
    async fn gateway_handles_peer_reconnection_same_addr_new_identity() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());

        // Create the gateway
        let (gw_pub, (gw_outbound, mut gw_conn), gw_addr) =
            create_mock_gateway(Default::default(), channels.clone()).await?;

        // Use a fixed port for testing the same-address scenario
        let peer_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 44444).into();

        // Step 1: Create peer A with identity X at the fixed address
        tracing::info!(
            "Step 1: Creating peer A (identity X) at {} connecting to gateway at {}",
            peer_addr,
            gw_addr
        );

        let (peer_a_pub, mut peer_a, _) =
            create_mock_peer_at_addr(Default::default(), peer_addr, channels.clone()).await?;

        tracing::info!("Peer A public key: {:?}", peer_a_pub);

        // Peer A connects to gateway
        let gw_task = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(10), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!(
                    "gateway: no inbound connection from peer A"
                ))?;
            tracing::info!(
                "Gateway received connection from peer A at {}",
                conn.remote_addr()
            );
            Ok::<_, anyhow::Error>((gw_conn, conn))
        });

        let peer_a_conn = tokio::time::timeout(
            Duration::from_secs(10),
            peer_a.connect(gw_pub.clone(), gw_addr).await,
        )
        .await??;
        tracing::info!("Peer A connected successfully");

        let (mut gw_conn, _gw_peer_a_conn) = gw_task.await??;

        // Step 2: Simulate peer restart by dropping everything related to peer A
        // This simulates: peer process killed, state cleared, restarted with new identity
        tracing::info!("Step 2: Simulating peer A restart (dropping connection and handler)");
        drop(peer_a_conn);
        drop(peer_a);

        // Give time for cleanup (in reality this could be much longer)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Step 3: Create peer B with NEW identity (Y) at the SAME address
        // This simulates the NAT assigning the same external IP:port to the restarted peer
        tracing::info!(
            "Step 3: Creating peer B (NEW identity Y) at SAME address {}",
            peer_addr
        );

        let (peer_b_pub, mut peer_b, _) =
            create_mock_peer_at_addr(Default::default(), peer_addr, channels.clone()).await?;

        tracing::info!("Peer B public key: {:?}", peer_b_pub);

        // Verify this test is actually testing new identity
        assert_ne!(
            peer_a_pub, peer_b_pub,
            "Peer B must have different identity than peer A"
        );

        // Step 4: Peer B (new identity) connects to gateway from same IP:port
        // This is where the bug manifests:
        // - Gateway still has remote_connections entry for peer_addr with peer A's session
        // - Peer B's handshake packets arrive, get sent to peer A's session handler
        // - Decryption fails because peer B is using different keys
        // - Handshake silently fails
        tracing::info!("Step 4: Peer B (new identity) connecting to gateway from same address");

        let gw_task = GlobalExecutor::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(5), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!(
                    "gateway: no inbound connection from peer B (bug #2277)"
                ))?;
            tracing::info!(
                "Gateway received connection from peer B at {}",
                conn.remote_addr()
            );
            Ok::<_, anyhow::Error>(conn)
        });

        let start = tokio::time::Instant::now();
        let peer_b_conn = tokio::time::timeout(
            Duration::from_secs(5),
            peer_b.connect(gw_pub.clone(), gw_addr).await,
        )
        .await??;
        let elapsed = start.elapsed();

        tracing::info!("Peer B connected in {:?}", elapsed);

        let _gw_peer_b_conn = gw_task.await??;

        // Verify the connection works
        assert_eq!(peer_b_conn.remote_addr(), gw_addr);

        // The connection should complete quickly
        assert!(
            elapsed < Duration::from_secs(2),
            "Connection took {:?}, which suggests the gateway failed to recognize the new identity. \
             This is bug #2277: gateway doesn't reset session when peer restarts with new identity.",
            elapsed
        );

        // Cleanup
        drop(peer_b_conn);
        drop(peer_b);
        drop(gw_outbound);

        Ok(())
    }

    /// Wraps a [`MockSocket`] and makes `recv_from` return the queued errors,
    /// one per call, before it delivers any packet. Lets a test drive the
    /// listener's receive-error handling with real OS error codes.
    #[cfg(test)]
    struct RecvErrorInjectingSocket {
        inner: MockSocket,
        errors: std::sync::Mutex<std::collections::VecDeque<std::io::Error>>,
    }

    #[cfg(test)]
    impl Socket for RecvErrorInjectingSocket {
        async fn bind(_addr: SocketAddr) -> Result<Self, std::io::Error> {
            unimplemented!()
        }

        async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            let injected = self.errors.lock().unwrap().pop_front();
            if let Some(err) = injected {
                return Err(err);
            }
            self.inner.recv_from(buf).await
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            self.inner.send_to(buf, target).await
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            self.inner.send_to_blocking(buf, target)
        }
    }

    /// Start a non-gateway listener whose socket first returns `errors`, and
    /// hand back its listen task so the test can see whether it exited.
    #[cfg(test)]
    async fn spawn_listener_with_recv_errors(
        port: u16,
        channels: Channels,
        errors: Vec<std::io::Error>,
    ) -> (
        TransportPublicKey,
        OutboundConnectionHandler<RecvErrorInjectingSocket>,
        tokio::task::JoinHandle<TransportError>,
        SocketAddr,
    ) {
        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
        let socket = RecvErrorInjectingSocket {
            inner: MockSocket::new(Default::default(), Default::default(), addr, channels).await,
            errors: std::sync::Mutex::new(errors.into()),
        };
        let keypair = TransportKeypair::new();
        let public = keypair.public.clone();
        let (handler, _new_connections, listen_handle) =
            OutboundConnectionHandler::config_listener(
                Arc::new(socket),
                keypair,
                false,
                addr,
                UdpTransportConfig::default(),
                None,
            )
            .expect("listener setup");
        (public, handler, listen_handle, addr)
    }

    /// The UDP listener must survive receive errors that say nothing about the
    /// socket itself: a message-size error, a network reset or an unreachable
    /// host/network report (all of which Windows can return from `recv_from`),
    /// and an error kind the classifier has never heard of. Each used to end
    /// the listen task, which force-exits the node. After the
    /// errors the listener must still read packets, shown by completing a
    /// handshake through it.
    #[tokio::test]
    async fn listener_survives_nonfatal_recv_errors_and_keeps_receiving() -> anyhow::Result<()> {
        let channels: Channels = Arc::new(DashMap::new());
        let errors = vec![
            std::io::Error::from_raw_os_error(10040), // WSAEMSGSIZE
            std::io::Error::from_raw_os_error(10052), // WSAENETRESET
            std::io::Error::from_raw_os_error(10065), // WSAEHOSTUNREACH
            std::io::Error::from_raw_os_error(10051), // WSAENETUNREACH
            std::io::Error::from(std::io::ErrorKind::HostUnreachable),
            std::io::Error::from(std::io::ErrorKind::NetworkUnreachable),
            std::io::Error::other("an error the classifier does not know"),
        ];
        let (a_pub, mut peer_a, listen_a, a_addr) =
            spawn_listener_with_recv_errors(47_100, channels.clone(), errors).await;
        let (b_pub, mut peer_b, b_addr) = create_mock_peer(Default::default(), channels).await?;

        let b_task = GlobalExecutor::spawn(async move {
            let conn = peer_b.connect(a_pub, a_addr).await;
            let conn = tokio::time::timeout(Duration::from_secs(10), conn).await??;
            Ok::<_, anyhow::Error>(conn)
        });
        let conn = peer_a.connect(b_pub, b_addr).await;
        let a_to_b = tokio::time::timeout(Duration::from_secs(10), conn).await??;
        let b_to_a = b_task.await??;
        assert_eq!(a_to_b.remote_addr(), b_addr);
        assert_eq!(b_to_a.remote_addr(), a_addr);

        assert!(
            !listen_a.is_finished(),
            "listener must keep running after non-fatal recv errors"
        );
        listen_a.abort();
        Ok(())
    }

    /// The other half of the contract: an error that means the socket itself
    /// is gone must still end the listen task, otherwise the node sits alive
    /// but deaf and is never restarted.
    #[tokio::test]
    async fn listener_exits_on_fatal_recv_error() {
        #[allow(unused_mut)]
        let mut fatal = vec![
            (std::io::Error::from_raw_os_error(10038), "WSAENOTSOCK"),
            (std::io::Error::from_raw_os_error(10058), "WSAESHUTDOWN"),
            (
                std::io::Error::from(std::io::ErrorKind::NotConnected),
                "NotConnected",
            ),
        ];
        #[cfg(unix)]
        fatal.push((std::io::Error::from_raw_os_error(libc::EBADF), "EBADF"));

        for (i, (err, name)) in fatal.into_iter().enumerate() {
            let channels: Channels = Arc::new(DashMap::new());
            let (_pub, _handler, listen, _addr) =
                spawn_listener_with_recv_errors(47_200 + i as u16, channels, vec![err]).await;
            let exited = tokio::time::timeout(Duration::from_secs(5), listen).await;
            assert!(
                matches!(
                    exited,
                    Ok(Ok(TransportError::Udp(UdpError(UdpErrorKind::IO(_)))))
                ),
                "{name}: listener must exit with the IO error, got {exited:?}"
            );
        }
    }

    #[cfg(test)]
    enum RecvStep {
        /// Advance virtual time by the duration, then fail the read.
        Error(Duration, std::io::Error),
        /// Deliver a datagram.
        Packet(SocketAddr, Vec<u8>),
    }

    #[cfg(test)]
    #[derive(Default)]
    struct ScriptProgress {
        /// The listener asked for a read after the last step.
        finished: std::sync::atomic::AtomicBool,
        /// The listener dropped its socket, i.e. the listen task ended.
        dropped: std::sync::atomic::AtomicBool,
    }

    /// A socket whose reads follow a fixed script, advancing a [`VirtualTime`]
    /// as it goes, so a test can put minutes of receive errors in front of the
    /// listener instantly. Sends are discarded.
    #[cfg(test)]
    struct ScriptedRecvSocket {
        script: std::sync::Mutex<std::collections::VecDeque<RecvStep>>,
        time: VirtualTime,
        progress: Arc<ScriptProgress>,
    }

    #[cfg(test)]
    impl Socket for ScriptedRecvSocket {
        async fn bind(_addr: SocketAddr) -> Result<Self, std::io::Error> {
            unimplemented!()
        }

        async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            let step = self.script.lock().unwrap().pop_front();
            match step {
                Some(RecvStep::Error(elapsed, err)) => {
                    self.time.advance(elapsed);
                    Err(err)
                }
                Some(RecvStep::Packet(from, data)) => {
                    buf[..data.len()].copy_from_slice(&data);
                    Ok((data.len(), from))
                }
                None => {
                    self.progress.finished.store(true, Ordering::SeqCst);
                    std::future::pending().await
                }
            }
        }

        async fn send_to(&self, buf: &[u8], _target: SocketAddr) -> std::io::Result<usize> {
            Ok(buf.len())
        }

        fn send_to_blocking(&self, buf: &[u8], _target: SocketAddr) -> std::io::Result<usize> {
            Ok(buf.len())
        }
    }

    #[cfg(test)]
    impl Drop for ScriptedRecvSocket {
        fn drop(&mut self) {
            self.progress.dropped.store(true, Ordering::SeqCst);
        }
    }

    /// Run a VirtualTime listener over `script` and report whether it read
    /// past the end of the script (`true`) or ended its listen task (`false`).
    #[cfg(test)]
    async fn listener_survives_script(script: Vec<RecvStep>) -> bool {
        run_scripted_listener(script).await.0
    }

    /// [`listener_survives_script`], also returning the listener's handler so
    /// a test can see what the listener did with the datagrams it read.
    #[cfg(test)]
    async fn run_scripted_listener(
        script: Vec<RecvStep>,
    ) -> (
        bool,
        OutboundConnectionHandler<ScriptedRecvSocket, VirtualTime>,
    ) {
        run_scripted_listener_on(script, |socket| socket).await
    }

    /// [`run_scripted_listener`] over the scripted socket as wrapped by
    /// `wrap`, e.g. in the `DemuxSocket` the transport binds.
    #[cfg(test)]
    async fn run_scripted_listener_on<S: Socket>(
        script: Vec<RecvStep>,
        wrap: impl FnOnce(ScriptedRecvSocket) -> S,
    ) -> (bool, OutboundConnectionHandler<S, VirtualTime>) {
        let time = VirtualTime::new();
        let progress = Arc::new(ScriptProgress::default());
        let socket = ScriptedRecvSocket {
            script: std::sync::Mutex::new(script.into()),
            time: time.clone(),
            progress: progress.clone(),
        };
        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, 47_300).into();
        let (handler, _new_connections) =
            OutboundConnectionHandler::<_, VirtualTime>::config_listener_with_virtual_time(
                Arc::new(wrap(socket)),
                TransportKeypair::new(),
                false,
                addr,
                None,
                None,
                None,
                time,
                None,
            )
            .expect("listener setup");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let finished = progress.finished.load(Ordering::SeqCst);
            let dropped = progress.dropped.load(Ordering::SeqCst);
            if finished || dropped {
                return (finished && !dropped, handler);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "listener neither finished the script nor exited"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[cfg(test)]
    fn unclassified_errors(count: usize) -> impl Iterator<Item = RecvStep> {
        (0..count).map(|_| {
            RecvStep::Error(
                Duration::from_millis(100),
                std::io::Error::other("scripted"),
            )
        })
    }

    /// A successful read in the listen loop must reset the error run. 700
    /// unclassified errors over 70 s, one datagram, then 400 more: without the
    /// reset the run holds 1000 errors over 100 s and the listener exits.
    #[tokio::test]
    async fn listener_successful_read_resets_recv_error_run() {
        let from: SocketAddr = (Ipv4Addr::LOCALHOST, 47_301).into();
        let script = unclassified_errors(700)
            .chain([RecvStep::Packet(from, vec![0u8; 64])])
            .chain(unclassified_errors(400))
            .collect();
        assert!(
            listener_survives_script(script).await,
            "the datagram between the two error runs must reset the run"
        );
    }

    /// Control for the test above: the same 1100 errors with no datagram
    /// between them do end the listener, so the survival above is due to the
    /// read and not to the script being too short to trigger an exit.
    #[tokio::test]
    async fn listener_exits_on_persistent_unclassified_recv_errors() {
        assert!(
            !listener_survives_script(unclassified_errors(1100).collect()).await,
            "1100 unclassified errors over 110 s with no read must end the listener"
        );
    }

    /// The socket-health signal counts every datagram the socket delivered,
    /// not only those the listener handles: through the `DemuxSocket` the
    /// transport binds, a QUIC-class datagram (dropped, since no QUIC
    /// ingress is enabled) between two unclassified error runs resets the
    /// run as a legacy datagram does. Without the reset the run holds 1000
    /// errors over 100 s and the listener exits.
    #[tokio::test]
    async fn listener_quic_datagram_through_demux_resets_recv_error_run() {
        let from: SocketAddr = (Ipv4Addr::LOCALHOST, 47_303).into();
        let script = unclassified_errors(700)
            .chain([RecvStep::Packet(from, vec![0xC3, 0, 0, 0])])
            .chain(unclassified_errors(400))
            .collect();
        let (survived, _) =
            run_scripted_listener_on(script, |socket| DemuxSocket::wrap(socket, None)).await;
        assert!(
            survived,
            "the QUIC-class datagram between the two error runs must reset the run"
        );
    }

    /// Control for the test above: through the same `DemuxSocket`, 1100
    /// unclassified errors with no datagram between them still end the
    /// listener.
    #[tokio::test]
    async fn listener_through_demux_exits_on_persistent_unclassified_recv_errors() {
        let (survived, _) = run_scripted_listener_on(unclassified_errors(1100).collect(), |s| {
            DemuxSocket::wrap(s, None)
        })
        .await;
        assert!(
            !survived,
            "1100 unclassified errors over 110 s with no read must end the listener"
        );
    }

    /// Transient errors never end the listener and never delay it. 13,000 of
    /// them over 650 s with no successful read, then a datagram, then 7,000
    /// more: the listener must survive all 20,000 and handle the datagram in
    /// between. Each error advances virtual time only when the listener asks
    /// for its next read, so any sleep on this path (which waits on that same
    /// clock) would stall the listener and fail the test, while the yield the
    /// long run triggers does not.
    #[tokio::test]
    async fn listener_reads_through_long_transient_error_run() {
        fn transient(count: usize) -> impl Iterator<Item = RecvStep> {
            (0..count).map(|_| {
                RecvStep::Error(
                    Duration::from_millis(50),
                    std::io::Error::from_raw_os_error(10051), // WSAENETUNREACH
                )
            })
        }
        let from: SocketAddr = (Ipv4Addr::new(127, 0, 0, 9), 47_302).into();
        let script = transient(13_000)
            .chain([RecvStep::Packet(from, vec![0u8; 64])])
            .chain(transient(7_000))
            .collect();
        let (survived, handler) = run_scripted_listener(script).await;
        assert!(survived, "transient errors must never end the listener");
        assert!(
            handler.expected_non_gateway.contains(&from.ip()),
            "the datagram in the middle of the run must have been handled"
        );
    }

    /// The error an AF_INET6 socket bound to `[::1]` returns for every
    /// off-host destination on Linux (measured), and also what a network
    /// that is coming back returns: ENETUNREACH, as a real OS error so the
    /// tests can check its text reaches the log (#5871).
    #[cfg(test)]
    fn enetunreach() -> std::io::Error {
        #[cfg(unix)]
        {
            std::io::Error::from_raw_os_error(libc::ENETUNREACH)
        }
        #[cfg(not(unix))]
        {
            std::io::Error::from_raw_os_error(10051) // WSAENETUNREACH
        }
    }

    /// Wraps a [`MockSocket`] and fails with [`enetunreach`] every send for
    /// which `fail(send_index, packet)` is true. Counts the failed sends.
    #[cfg(test)]
    struct SendErrorInjectingSocket {
        inner: MockSocket,
        fail: fn(usize, &[u8]) -> bool,
        sends: AtomicUsize,
        failures: Arc<AtomicUsize>,
    }

    #[cfg(test)]
    impl Socket for SendErrorInjectingSocket {
        async fn bind(_addr: SocketAddr) -> Result<Self, std::io::Error> {
            unimplemented!()
        }

        async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            self.inner.recv_from(buf).await
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            let index = self.sends.fetch_add(1, Ordering::SeqCst);
            if (self.fail)(index, buf) {
                self.failures.fetch_add(1, Ordering::SeqCst);
                return Err(enetunreach());
            }
            self.inner.send_to(buf, target).await
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            self.inner.send_to_blocking(buf, target)
        }
    }

    #[cfg(test)]
    fn is_intro(buf: &[u8]) -> bool {
        PacketData::<UnknownEncryption, MAX_PACKET_SIZE>::from_buf(buf).is_intro_packet()
    }

    /// Start a listener on `port` whose sends `fail` selects fail.
    #[cfg(test)]
    async fn spawn_listener_with_send_errors(
        port: u16,
        is_gateway: bool,
        channels: Channels,
        fail: fn(usize, &[u8]) -> bool,
    ) -> (
        TransportPublicKey,
        OutboundConnectionHandler<SendErrorInjectingSocket>,
        SocketAddr,
        Arc<AtomicUsize>,
    ) {
        let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
        let failures = Arc::new(AtomicUsize::new(0));
        let socket = SendErrorInjectingSocket {
            inner: MockSocket::new(Default::default(), Default::default(), addr, channels).await,
            fail,
            sends: AtomicUsize::new(0),
            failures: failures.clone(),
        };
        let keypair = TransportKeypair::new();
        let public = keypair.public.clone();
        let (handler, _new_connections, _listen_handle) =
            OutboundConnectionHandler::config_listener(
                Arc::new(socket),
                keypair,
                is_gateway,
                addr,
                UdpTransportConfig::default(),
                None,
            )
            .expect("listener setup");
        (public, handler, addr, failures)
    }

    /// Wait until some captured line satisfies `pred`, or give up after 10 s.
    ///
    /// The capture is thread-local, so the tests using it rely on the default
    /// current-thread `#[tokio::test]` runtime, which polls the listener and
    /// handshake tasks on the test's own thread.
    #[cfg(test)]
    async fn wait_for_log(
        logs: &crate::util::test_log_capture::CapturedLogs,
        pred: impl Fn(&str) -> bool,
    ) -> Option<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(line) = logs.lock().unwrap().iter().find(|l| pred(l)) {
                return Some(line.clone());
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// #5871: when every handshake send fails, the outbound attempt must
    /// end with an error naming the send failure and the destination, not
    /// `ChannelClosed` ("transport handler channel closed, socket likely
    /// closed"), and the OS error itself must reach the log. It must also
    /// keep re-sending until the attempt's deadline, as it does for a lost
    /// datagram, rather than giving up on the first failure.
    #[tokio::test]
    async fn nat_traversal_send_failure_reports_os_error_not_channel_closed() {
        let (logs, guard) = crate::util::test_log_capture::install();
        let channels: Channels = Arc::new(DashMap::new());
        let (_pub, mut peer, _addr, failures) =
            spawn_listener_with_send_errors(47_400, false, channels, |_, _| true).await;
        let remote_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 47_401).into();
        let remote_pub = TransportKeypair::new().public;

        let conn = peer.connect(remote_pub, remote_addr).await;
        let result = tokio::time::timeout(Duration::from_secs(10), conn)
            .await
            .expect("a failing attempt must end at its deadline, not hang");
        let err = match result {
            Ok(_) => panic!("every send failed, so the handshake cannot succeed"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                TransportError::Udp(UdpError(UdpErrorKind::SendFailed(addr, std::io::ErrorKind::NetworkUnreachable)))
                    if addr == remote_addr
            ),
            "expected SendFailed({remote_addr}, NetworkUnreachable), got {err:?}"
        );
        // The user-visible "Failed NAT traversal" line prints this Display.
        assert_eq!(
            err.to_string(),
            format!("send to {remote_addr} failed: network unreachable")
        );
        // NAT_TRAVERSAL_MAX_ATTEMPTS is 10 under cfg(test) at a ~250 ms
        // cadence; 5 shows sustained re-sending without depending on the
        // scheduler delivering all 10 before the deadline.
        assert!(
            failures.load(Ordering::SeqCst) >= 5,
            "a send failure must not end the attempt at once; the intro loop \
             re-sends as for a lost datagram (failures = {})",
            failures.load(Ordering::SeqCst)
        );

        let os_error = format!("os error {}", enetunreach().raw_os_error().unwrap());
        let peer_field = format!("peer_addr={remote_addr}");
        let line = wait_for_log(&logs, |l| {
            l.starts_with("WARN") && l.contains(&os_error) && l.contains(&peer_field)
        })
        .await;
        drop(guard);
        assert!(
            line.is_some(),
            "a WARN naming the OS error and the destination must be logged; captured: {:?}",
            logs.lock().unwrap()
        );
    }

    /// #5871, the other half of the retry decision: a send failure that
    /// clears (a network coming back) must not kill the attempt. The first
    /// two sends fail; the handshake must still complete.
    #[tokio::test]
    async fn nat_traversal_survives_send_failures_that_clear() -> anyhow::Result<()> {
        let channels: Channels = Arc::new(DashMap::new());
        let (a_pub, mut peer_a, a_addr, failures) =
            spawn_listener_with_send_errors(47_410, false, channels.clone(), |i, _| i < 2).await;
        let (b_pub, mut peer_b, b_addr) = create_mock_peer(Default::default(), channels).await?;

        let b_task = GlobalExecutor::spawn(async move {
            let conn = peer_b.connect(a_pub, a_addr).await;
            let conn = tokio::time::timeout(Duration::from_secs(10), conn).await??;
            Ok::<_, anyhow::Error>(conn)
        });
        let conn = peer_a.connect(b_pub, b_addr).await;
        let a_to_b = tokio::time::timeout(Duration::from_secs(10), conn).await??;
        let b_to_a = b_task.await??;
        assert_eq!(a_to_b.remote_addr(), b_addr);
        assert_eq!(b_to_a.remote_addr(), a_addr);
        assert_eq!(
            failures.load(Ordering::SeqCst),
            2,
            "the test only means something if the injected failures happened"
        );
        Ok(())
    }

    /// Run one outbound attempt from a listener whose sends `fail` selects,
    /// at a target that receives every packet and never answers. Returns the
    /// attempt's error, the number of failed sends, and the captured logs.
    #[cfg(test)]
    async fn attempt_against_silent_target(
        port: u16,
        fail: fn(usize, &[u8]) -> bool,
    ) -> (TransportError, usize, Vec<String>) {
        let (logs, guard) = crate::util::test_log_capture::install();
        let channels: Channels = Arc::new(DashMap::new());
        let (_pub, mut peer, _addr, failures) =
            spawn_listener_with_send_errors(port, false, channels.clone(), fail).await;
        let remote_addr: SocketAddr = (Ipv4Addr::LOCALHOST, port + 1).into();
        // Registered in `channels`, so sends to it succeed; nothing reads it.
        let _silent = MockSocket::new(
            Default::default(),
            Default::default(),
            remote_addr,
            channels,
        )
        .await;

        let conn = peer
            .connect(TransportKeypair::new().public, remote_addr)
            .await;
        let result = tokio::time::timeout(Duration::from_secs(10), conn)
            .await
            .expect("the attempt must end at its deadline");
        drop(guard);
        let err = match result {
            Ok(_) => panic!("a target that never answers cannot complete a handshake"),
            Err(err) => err,
        };
        let captured = logs.lock().unwrap().clone();
        (err, failures.load(Ordering::SeqCst), captured)
    }

    /// #5871: sends that fail and then recover, followed by a remote that
    /// never answers, must end as an ordinary unanswered attempt, not as a
    /// send failure (the last send went out). The failures must still be
    /// visible: the final warn carries their count and the last OS error.
    #[tokio::test]
    async fn nat_traversal_recovered_send_failures_end_as_unanswered_attempt() {
        let (err, failures, logs) = attempt_against_silent_target(47_450, |i, _| i < 2).await;
        assert_eq!(failures, 2, "the injected failures must have happened");
        assert!(
            matches!(err, TransportError::ConnectionEstablishmentFailure { .. }),
            "the last send succeeded, so this is not a send failure; got {err:?}"
        );
        assert!(
            !logs
                .iter()
                .any(|l| l.contains("sends to the remote are failing")),
            "must not claim sends are failing after they recovered; captured: {logs:?}"
        );
        let os_error = format!("os error {}", enetunreach().raw_os_error().unwrap());
        assert!(
            logs.iter().any(|l| l.starts_with("WARN")
                && l.contains("max connection attempts reached")
                && l.contains("failed_sends=2")
                && l.contains(&os_error)),
            "the final warn must report the 2 failed sends and the OS error; captured: {logs:?}"
        );
    }

    /// The mirror of the test above: sends that succeed and then start
    /// failing end the attempt with the send error, counted in the warn.
    #[tokio::test]
    async fn nat_traversal_send_failures_at_the_end_report_send_error() {
        let (err, failures, logs) = attempt_against_silent_target(47_460, |i, _| i >= 3).await;
        assert!(failures >= 1, "the injected failures must have happened");
        assert!(
            matches!(
                err,
                TransportError::Udp(UdpError(UdpErrorKind::SendFailed(
                    _,
                    std::io::ErrorKind::NetworkUnreachable
                )))
            ),
            "the last send failed, so the attempt reports it; got {err:?}"
        );
        assert!(
            logs.iter().any(|l| l.starts_with("WARN")
                && l.contains("sends to the remote are failing")
                && l.contains(&format!("failed_sends={failures}"))),
            "the final warn must count the failed sends; captured: {logs:?}"
        );
    }

    /// #5871, the one-shot send at the end of the joiner's handshake: the
    /// joiner's intro reaches the gateway, but its reply to the gateway's ack
    /// fails. There is nothing left in the attempt to re-send it, so the
    /// attempt ends at once, with the send error rather than `ChannelClosed`.
    #[tokio::test]
    async fn joiner_ack_send_failure_reports_send_error() -> anyhow::Result<()> {
        let (logs, guard) = crate::util::test_log_capture::install();
        let channels: Channels = Arc::new(DashMap::new());
        let (gw_pub, (_gw_handler, _gw_conns), gw_addr) =
            create_mock_gateway(Default::default(), channels.clone()).await?;
        let (_pub, mut joiner, _addr, failures) =
            spawn_listener_with_send_errors(47_420, false, channels, |_, buf| !is_intro(buf)).await;

        let conn = joiner.connect(gw_pub, gw_addr).await;
        let result = tokio::time::timeout(Duration::from_secs(10), conn).await?;
        let err = match result {
            Ok(_) => panic!("the joiner's ack could not be sent, so it must not report success"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                TransportError::Udp(UdpError(UdpErrorKind::SendFailed(addr, std::io::ErrorKind::NetworkUnreachable)))
                    if addr == gw_addr
            ),
            "expected SendFailed({gw_addr}, NetworkUnreachable), got {err:?}"
        );
        assert_eq!(failures.load(Ordering::SeqCst), 1);

        // Outbound failures are ours to fix, so they log at WARN (inbound
        // ones, which a spoofed intro can trigger, log at INFO).
        let os_error = format!("os error {}", enetunreach().raw_os_error().unwrap());
        let line = wait_for_log(&logs, |l| {
            l.starts_with("WARN")
                && l.contains("direction=\"outbound\"")
                && l.contains(&os_error)
                && l.contains(&format!("peer_addr={gw_addr}"))
        })
        .await;
        drop(guard);
        assert!(
            line.is_some(),
            "a WARN naming the outbound direction, OS error and gateway must be logged; \
             captured: {:?}",
            logs.lock().unwrap()
        );
        Ok(())
    }

    /// #5871, gateway side: when the gateway's ack to a joiner cannot be
    /// sent, the failed handshake must be logged with the send error and
    /// the OS error, not as `ChannelClosed`.
    #[tokio::test]
    async fn gateway_ack_send_failure_logs_send_error() -> anyhow::Result<()> {
        let (logs, guard) = crate::util::test_log_capture::install();
        let channels: Channels = Arc::new(DashMap::new());
        let (gw_pub, _gw, gw_addr, failures) =
            spawn_listener_with_send_errors(47_430, true, channels.clone(), |_, _| true).await;
        let (_pub, mut joiner, joiner_addr) =
            create_mock_peer(Default::default(), channels).await?;

        let conn = joiner.connect(gw_pub, gw_addr).await;
        let joiner_task =
            GlobalExecutor::spawn(tokio::time::timeout(Duration::from_secs(10), conn));

        let send_failed = format!("send to {joiner_addr} failed");
        let failed_line = wait_for_log(&logs, |l| {
            l.contains("Failed to establish gateway connection")
        })
        .await;
        let os_error = format!("os error {}", enetunreach().raw_os_error().unwrap());
        let info_line = wait_for_log(&logs, |l| {
            l.starts_with("INFO")
                && l.contains(&os_error)
                && l.contains(&format!("peer_addr={joiner_addr}"))
        })
        .await;
        joiner_task.abort();
        drop(guard);

        let failed_line = failed_line.unwrap_or_else(|| {
            panic!(
                "the gateway must log the failed handshake; captured: {:?}",
                logs.lock().unwrap()
            )
        });
        assert!(
            failed_line.contains(&send_failed),
            "the failure must name the send error and the joiner, got: {failed_line}"
        );
        assert!(
            !failed_line.contains("channel closed"),
            "a send error is not a closed channel, got: {failed_line}"
        );
        assert!(
            info_line.is_some(),
            "an INFO naming the OS error and the joiner must be logged; captured: {:?}",
            logs.lock().unwrap()
        );
        assert!(failures.load(Ordering::SeqCst) >= 1);
        Ok(())
    }

    /// #5871, gateway side, version mismatch: the `ack_error` that tells an
    /// incompatible joiner why it was refused is best effort. If it cannot
    /// be sent, the handshake must still end as a version mismatch (which is
    /// what makes the listener remember the peer as outdated), and the send
    /// error must be logged with the OS error, not reported as
    /// `ChannelClosed`.
    #[tokio::test]
    async fn gateway_version_mismatch_ack_send_failure_still_reports_mismatch() {
        let (logs, guard) = crate::util::test_log_capture::install();
        let channels: Channels = Arc::new(DashMap::new());
        let (gw_pub, _gw, gw_addr, failures) =
            spawn_listener_with_send_errors(47_440, true, channels.clone(), |_, _| true).await;

        // An old-format 0.0.1 version, below every build's min_compatible.
        let joiner_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 47_441).into();
        let raw = MockSocket::new(
            Default::default(),
            Default::default(),
            joiner_addr,
            channels,
        )
        .await;
        let old_version: [u8; 8] = [0, 0, 1, 0, 0, 0, 0, 0];
        assert!(version_cmp::is_compatible(&PROTOC_VERSION, &old_version).is_err());
        let mut intro = [0u8; 8 + 16];
        intro[..8].copy_from_slice(&old_version);
        let packet = PacketData::<_, MAX_PACKET_SIZE>::encrypt_with_pubkey(&intro, &gw_pub);
        raw.send_to(packet.data(), gw_addr).await.unwrap();

        let failed_line = wait_for_log(&logs, |l| {
            l.contains("Failed to establish gateway connection")
        })
        .await;
        let os_error = format!("os error {}", enetunreach().raw_os_error().unwrap());
        let info_line = wait_for_log(&logs, |l| {
            l.starts_with("INFO")
                && l.contains(&os_error)
                && l.contains(&format!("peer_addr={joiner_addr}"))
        })
        .await;
        drop(guard);

        let failed_line = failed_line.unwrap_or_else(|| {
            panic!(
                "the gateway must log the refused handshake; captured: {:?}",
                logs.lock().unwrap()
            )
        });
        assert!(
            failed_line.contains("Version incompatibility"),
            "an unsendable ack_error must not hide the version mismatch, got: {failed_line}"
        );
        assert!(
            info_line.is_some(),
            "an INFO naming the OS error and the joiner must be logged; captured: {:?}",
            logs.lock().unwrap()
        );
        assert_eq!(failures.load(Ordering::SeqCst), 1);
    }

    /// Completion of a hole-punch handshake on the side that answers the
    /// remote's intro (`traverse_nat`'s `RemoteInbound` arm): it completes
    /// only on a packet encrypted under that attempt's inbound session key,
    /// and still interoperates with peers that complete on any
    /// non-intro packet.
    #[cfg(test)]
    mod handshake_completion_tests {
        use super::*;
        use crate::simulation::VirtualTime;
        use crate::transport::packet_data::MAX_PACKET_SIZE;

        /// A remote driven by hand: it holds a keypair and an address on
        /// `channels`, reads what the peer under test sends it, and sends
        /// packets of the test's choosing.
        struct ScriptedRemote {
            keypair: TransportKeypair,
            addr: SocketAddr,
            inbound: mpsc::UnboundedReceiver<MockDatagram>,
            channels: Channels,
        }

        impl ScriptedRemote {
            fn new(channels: &Channels) -> Self {
                let port = MOCK_PEER_PORT.fetch_add(1, Ordering::SeqCst);
                let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
                let (tx, inbound) = mpsc::unbounded_channel();
                channels.insert(addr, tx);
                Self {
                    keypair: TransportKeypair::new(),
                    addr,
                    inbound,
                    channels: channels.clone(),
                }
            }

            fn send(&self, to: SocketAddr, data: &[u8]) {
                self.channels
                    .get(&to)
                    .expect("the peer under test is listening")
                    .send(MockDatagram {
                        from: self.addr,
                        data: data.to_vec(),
                        available_at_nanos: 0,
                        order: SEND_ORDER.fetch_add(1, Ordering::Relaxed),
                    })
                    .expect("the peer under test is listening");
            }

            /// The next datagram the peer under test sent us. Bounded in
            /// wall-clock time only so a broken handshake fails the test
            /// instead of hanging it; virtual time never advances here.
            async fn recv(&mut self) -> Vec<u8> {
                tokio::time::timeout(Duration::from_secs(10), self.inbound.recv())
                    .await
                    .expect("the peer under test sent nothing")
                    .expect("channel open")
                    .data
            }
        }

        impl Drop for ScriptedRemote {
            fn drop(&mut self) {
                self.channels.remove(&self.addr);
            }
        }

        fn cipher(key: [u8; 16]) -> Aes128Gcm {
            Aes128Gcm::new(&key.into())
        }

        /// Drive a peer under test into the `RemoteInbound` arm: it dials the
        /// scripted remote, the remote reads the peer's inbound key from its
        /// intro and answers with an intro of its own, and the peer's ack
        /// (under the remote's key) confirms the arm was reached.
        ///
        /// Returns the pending connection, the peer's address, the peer's
        /// inbound key and the remote's own key. Virtual time is never
        /// advanced, so the attempt cannot reach its deadline: it moves only
        /// when a packet arrives, which makes every step deterministic.
        async fn peer_waiting_for_completion(
            remote: &mut ScriptedRemote,
            channels: &Channels,
        ) -> anyhow::Result<(
            tokio::task::JoinHandle<
                Result<PeerConnection<MockSocket, VirtualTime>, TransportError>,
            >,
            SocketAddr,
            OutboundConnectionHandler<MockSocket, VirtualTime>,
            [u8; 16],
            [u8; 16],
        )> {
            peer_waiting_for_completion_at(remote, channels, VirtualTime::new()).await
        }

        /// [`peer_waiting_for_completion`] on a caller-owned clock.
        async fn peer_waiting_for_completion_at(
            remote: &mut ScriptedRemote,
            channels: &Channels,
            vt: VirtualTime,
        ) -> anyhow::Result<(
            tokio::task::JoinHandle<
                Result<PeerConnection<MockSocket, VirtualTime>, TransportError>,
            >,
            SocketAddr,
            OutboundConnectionHandler<MockSocket, VirtualTime>,
            [u8; 16],
            [u8; 16],
        )> {
            let (peer_pub, mut peer, peer_addr) = create_mock_peer_with_virtual_time(
                PacketDropPolicy::ReceiveAll,
                PacketDelayPolicy::NoDelay,
                channels.clone(),
                vt,
            )
            .await?;
            let conn = peer
                .connect(remote.keypair.public().clone(), remote.addr)
                .await;
            let conn = tokio::spawn(conn);

            // The peer's intro: protocol version + its inbound key, to our
            // public key.
            let intro = remote.recv().await;
            let decrypted = PacketData::<_, MAX_PACKET_SIZE>::from_buf(&intro)
                .try_decrypt_asym(remote.keypair.secret())
                .expect("the first packet is the peer's intro");
            let (_, peer_inbound_key) =
                split_asym_intro_payload(decrypted.data()).expect("well-formed intro");

            let mut remote_key = [0u8; 16];
            GlobalRng::fill_bytes(&mut remote_key);
            let mut payload = PROTOC_VERSION.to_vec();
            payload.extend_from_slice(&remote_key);
            let our_intro =
                PacketData::<_, MAX_PACKET_SIZE>::encrypt_with_pubkey(&payload, &peer_pub);
            remote.send(peer_addr, our_intro.data());

            // The peer acks under OUR key once it has read our intro.
            loop {
                let packet = PacketData::<_, MAX_PACKET_SIZE>::from_buf(remote.recv().await);
                if packet.is_intro_packet() {
                    continue;
                }
                let ack = packet
                    .try_decrypt_sym(&cipher(remote_key))
                    .expect("the peer's ack is under the key from our intro");
                let msg = SymmetricMessage::deser(ack.data())?;
                assert!(
                    ack_ok_parts(&msg.payload).is_some(),
                    "the peer answered our intro with a success ack"
                );
                break;
            }
            Ok((conn, peer_addr, peer, peer_inbound_key, remote_key))
        }

        /// Waits (wall clock) until `remote`'s dropped-packet count reaches
        /// `n`. The attempt only moves when a packet arrives, so once the
        /// count is reached nothing else is in flight.
        async fn wait_for_drops(remote: SocketAddr, n: usize) {
            let metrics = &crate::transport::TRANSPORT_METRICS;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while metrics.handshake_unauthenticated_dropped_from(remote) < n {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "expected {n} dropped handshake packet(s) from {remote}, saw {}",
                    metrics.handshake_unauthenticated_dropped_from(remote)
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        async fn established(
            conn: tokio::task::JoinHandle<
                Result<PeerConnection<MockSocket, VirtualTime>, TransportError>,
            >,
        ) -> PeerConnection<MockSocket, VirtualTime> {
            tokio::time::timeout(Duration::from_secs(10), conn)
                .await
                .expect("the handshake completes")
                .expect("task")
                .expect("the handshake completes successfully")
        }

        /// The ordinary case: the remote's ack, under the peer's inbound key,
        /// completes the handshake and nothing is dropped.
        #[tokio::test]
        async fn handshake_completes_on_an_authenticated_ack() -> anyhow::Result<()> {
            let channels: Channels = Arc::new(DashMap::new());
            let mut remote = ScriptedRemote::new(&channels);
            let (conn, peer_addr, _peer, peer_key, remote_key) =
                peer_waiting_for_completion(&mut remote, &channels).await?;

            let ack = SymmetricMessage::ack_ok(&cipher(peer_key), remote_key, peer_addr)?;
            remote.send(peer_addr, ack.data());

            let conn = established(conn).await;
            assert_eq!(conn.remote_addr(), remote.addr);
            assert!(completed_in_remote_inbound(peer_addr, remote.addr));
            assert_eq!(
                crate::transport::TRANSPORT_METRICS
                    .handshake_unauthenticated_dropped_from(remote.addr),
                0
            );
            Ok(())
        }

        /// Packets not encrypted under the peer's inbound key (one under
        /// another key, one of arbitrary bytes) do not complete the
        /// handshake: each is dropped and counted, the attempt stays pending,
        /// and a later ack under the right key still completes it.
        #[tokio::test]
        async fn unauthenticated_packet_does_not_complete_the_handshake() -> anyhow::Result<()> {
            let channels: Channels = Arc::new(DashMap::new());
            let mut remote = ScriptedRemote::new(&channels);
            let (conn, peer_addr, _peer, peer_key, remote_key) =
                peer_waiting_for_completion(&mut remote, &channels).await?;

            let mut other_key = [0u8; 16];
            GlobalRng::fill_bytes(&mut other_key);
            assert_ne!(other_key, peer_key);
            let wrong_key_ack =
                SymmetricMessage::ack_ok(&cipher(other_key), remote_key, peer_addr)?;
            remote.send(peer_addr, wrong_key_ack.data());
            wait_for_drops(remote.addr, 1).await;
            assert!(
                !conn.is_finished(),
                "a packet under another key must not complete the handshake"
            );

            let mut junk = vec![0u8; 64];
            GlobalRng::fill_bytes(&mut junk);
            junk[0] = crate::transport::crypto::PACKET_TYPE_SYMMETRIC;
            remote.send(peer_addr, &junk);
            wait_for_drops(remote.addr, 2).await;
            assert!(
                !conn.is_finished(),
                "arbitrary bytes must not complete the handshake"
            );

            let ack = SymmetricMessage::ack_ok(&cipher(peer_key), remote_key, peer_addr)?;
            remote.send(peer_addr, ack.data());
            let conn = established(conn).await;
            assert_eq!(conn.remote_addr(), remote.addr);
            assert!(completed_in_remote_inbound(peer_addr, remote.addr));
            assert_eq!(
                crate::transport::TRANSPORT_METRICS
                    .handshake_unauthenticated_dropped_from(remote.addr),
                2,
                "the ack under the right key is not counted as dropped"
            );
            Ok(())
        }

        /// Dropped packets do not cause sends: after each one the attempt
        /// goes back to receiving, so a burst of them neither uses up the
        /// attempt's send budget nor sends acks outside the normal pacing.
        /// A later packet under the right key still completes it.
        #[tokio::test]
        async fn dropped_packets_do_not_cause_extra_sends() -> anyhow::Result<()> {
            const JUNK: usize = 20;
            let channels: Channels = Arc::new(DashMap::new());
            let mut remote = ScriptedRemote::new(&channels);
            let (conn, peer_addr, _peer, peer_key, remote_key) =
                peer_waiting_for_completion(&mut remote, &channels).await?;

            for i in 0..JUNK {
                let mut junk = vec![0u8; 64];
                GlobalRng::fill_bytes(&mut junk);
                junk[0] = crate::transport::crypto::PACKET_TYPE_SYMMETRIC;
                remote.send(peer_addr, &junk);
                wait_for_drops(remote.addr, i + 1).await;
            }
            // Give a send that followed the last drop time to arrive. Virtual
            // time does not move, so no paced send is due.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut sent_after_drops = 0;
            while remote.inbound.try_recv().is_ok() {
                sent_after_drops += 1;
            }
            assert_eq!(
                sent_after_drops, 0,
                "{JUNK} dropped packets caused {sent_after_drops} sends"
            );
            assert!(!conn.is_finished());

            let ack = SymmetricMessage::ack_ok(&cipher(peer_key), remote_key, peer_addr)?;
            remote.send(peer_addr, ack.data());
            let conn = established(conn).await;
            assert_eq!(conn.remote_addr(), remote.addr);
            assert!(completed_in_remote_inbound(peer_addr, remote.addr));
            Ok(())
        }

        /// A steady trickle of dropped packets, each arriving before the
        /// current 200 ms receive window ends, does not postpone the paced
        /// acks: they still go out at the baseline interval (window plus
        /// `INITIAL_INTERVAL`, about 250 ms). A drop must not restart the
        /// window.
        #[tokio::test]
        async fn dropped_packet_trickle_does_not_postpone_paced_acks() -> anyhow::Result<()> {
            const STEP: Duration = Duration::from_millis(50);
            const STEPS: usize = 12; // 600 ms of virtual time
            let vt = VirtualTime::new();
            let channels: Channels = Arc::new(DashMap::new());
            let mut remote = ScriptedRemote::new(&channels);
            let (conn, peer_addr, _peer, peer_key, remote_key) =
                peer_waiting_for_completion_at(&mut remote, &channels, vt.clone()).await?;

            let mut acks_at_ms = Vec::new();
            for step in 0..STEPS {
                let mut junk = vec![0u8; 64];
                GlobalRng::fill_bytes(&mut junk);
                junk[0] = crate::transport::crypto::PACKET_TYPE_SYMMETRIC;
                remote.send(peer_addr, &junk);
                // Let the peer take the packet before time moves.
                tokio::time::sleep(Duration::from_millis(10)).await;
                vt.advance(STEP);
                // Let any send that is now due happen.
                tokio::time::sleep(Duration::from_millis(10)).await;
                while let Ok(packet) = remote.inbound.try_recv() {
                    let packet = PacketData::<_, MAX_PACKET_SIZE>::from_buf(packet.data);
                    if !packet.is_intro_packet()
                        && packet.try_decrypt_sym(&cipher(remote_key)).is_ok()
                    {
                        acks_at_ms.push((step + 1) * STEP.as_millis() as usize);
                    }
                }
            }
            wait_for_drops(remote.addr, STEPS).await;
            // Paced acks are due at about 250 ms and 500 ms after the first.
            assert!(
                acks_at_ms.len() >= 2,
                "dropped packets every {STEP:?} postponed the paced acks; acks at {acks_at_ms:?} ms"
            );
            assert!(
                acks_at_ms[0] <= 300 && acks_at_ms[1] <= 550,
                "paced acks were late: {acks_at_ms:?} ms"
            );
            assert!(!conn.is_finished());

            let ack = SymmetricMessage::ack_ok(&cipher(peer_key), remote_key, peer_addr)?;
            remote.send(peer_addr, ack.data());
            let _conn = established(conn).await;
            Ok(())
        }

        /// Marks `addr` as using the earlier completion rule (complete on any
        /// non-intro packet) until dropped.
        struct LegacyCompletion(SocketAddr);

        impl LegacyCompletion {
            fn new(addr: SocketAddr) -> Self {
                LEGACY_HANDSHAKE_COMPLETION.insert(addr);
                Self(addr)
            }
        }

        impl Drop for LegacyCompletion {
            fn drop(&mut self) {
                LEGACY_HANDSHAKE_COMPLETION.remove(&self.0);
            }
        }

        /// The earlier rule, which the interop tests below run one peer
        /// with: a packet under another key completes the handshake. This is
        /// what `unauthenticated_packet_does_not_complete_the_handshake` rules
        /// out, and it shows `LegacyCompletion` really selects that rule.
        #[tokio::test]
        async fn legacy_rule_completes_on_an_unauthenticated_packet() -> anyhow::Result<()> {
            let channels: Channels = Arc::new(DashMap::new());
            let mut remote = ScriptedRemote::new(&channels);
            let (conn, peer_addr, _peer, peer_key, remote_key) =
                peer_waiting_for_completion(&mut remote, &channels).await?;
            let _legacy = LegacyCompletion::new(peer_addr);

            let mut other_key = [0u8; 16];
            GlobalRng::fill_bytes(&mut other_key);
            assert_ne!(other_key, peer_key);
            let wrong_key_ack =
                SymmetricMessage::ack_ok(&cipher(other_key), remote_key, peer_addr)?;
            remote.send(peer_addr, wrong_key_ack.data());
            let conn = established(conn).await;
            assert_eq!(conn.remote_addr(), remote.addr);
            Ok(())
        }

        /// Which peer must complete in the `RemoteInbound` arm.
        #[derive(Clone, Copy)]
        enum InRemoteInbound {
            Checked,
            Legacy,
            Either,
        }

        /// Connects a peer with the check to a peer with the earlier rule,
        /// each dialing the other, under the given drop policies. Both must
        /// connect, and `expect` names the peer that must complete in the
        /// `RemoteInbound` arm.
        async fn checked_and_legacy_peers_connect(
            checked_policy: PacketDropPolicy,
            legacy_policy: PacketDropPolicy,
            expect: InRemoteInbound,
        ) -> anyhow::Result<()> {
            let channels = Arc::new(DashMap::new());
            let (checked_pub, mut checked, checked_addr) =
                create_mock_peer(checked_policy, channels.clone()).await?;
            let (legacy_pub, mut legacy, legacy_addr) =
                create_mock_peer(legacy_policy, channels).await?;
            let _legacy = LegacyCompletion::new(legacy_addr);

            let to_legacy = GlobalExecutor::spawn(async move {
                let conn = checked.connect(legacy_pub, legacy_addr).await;
                let conn = tokio::time::timeout(Duration::from_secs(30), conn).await??;
                Ok::<_, anyhow::Error>(conn.remote_addr())
            });
            let to_checked = GlobalExecutor::spawn(async move {
                let conn = legacy.connect(checked_pub, checked_addr).await;
                let conn = tokio::time::timeout(Duration::from_secs(30), conn).await??;
                Ok::<_, anyhow::Error>(conn.remote_addr())
            });
            let (a, b) = tokio::try_join!(to_legacy, to_checked)?;
            assert_eq!(a?, legacy_addr);
            assert_eq!(b?, checked_addr);
            let checked_arm = completed_in_remote_inbound(checked_addr, legacy_addr);
            let legacy_arm = completed_in_remote_inbound(legacy_addr, checked_addr);
            match expect {
                InRemoteInbound::Checked => assert!(
                    checked_arm,
                    "the checked peer must complete in the RemoteInbound arm"
                ),
                InRemoteInbound::Legacy => assert!(
                    legacy_arm,
                    "the legacy peer must complete in the RemoteInbound arm"
                ),
                InRemoteInbound::Either => {}
            }
            assert_eq!(
                crate::transport::TRANSPORT_METRICS
                    .handshake_unauthenticated_dropped_from(legacy_addr),
                0,
                "every completing packet a legacy peer sends is under the right key"
            );
            Ok(())
        }

        /// Interop: the checked peer completes in the `RemoteInbound` arm on
        /// a packet from a peer with the earlier rule.
        #[tokio::test]
        async fn checked_peer_completes_against_legacy_peer_in_remote_inbound() -> anyhow::Result<()>
        {
            // The checked peer's intro is dropped, so it reads the legacy
            // peer's intro first, acks it, and completes in the
            // `RemoteInbound` arm (the arm the check is in) on the legacy
            // peer's ack.
            checked_and_legacy_peers_connect(
                PacketDropPolicy::Ranges(vec![0..1]),
                PacketDropPolicy::ReceiveAll,
                InRemoteInbound::Checked,
            )
            .await
        }

        /// Interop: a peer with the earlier rule completes in the
        /// `RemoteInbound` arm against a checked peer.
        #[tokio::test]
        async fn legacy_peer_completes_against_checked_peer_in_remote_inbound() -> anyhow::Result<()>
        {
            // The legacy peer's intro is dropped, so the legacy peer is the
            // one in the `RemoteInbound` arm and the checked peer completes
            // on the ack path.
            checked_and_legacy_peers_connect(
                PacketDropPolicy::ReceiveAll,
                PacketDropPolicy::Ranges(vec![0..1]),
                InRemoteInbound::Legacy,
            )
            .await
        }

        /// Interop with no loss: both peers race, and whichever arms they
        /// take, they connect.
        #[tokio::test]
        async fn checked_and_legacy_peers_connect_without_loss() -> anyhow::Result<()> {
            checked_and_legacy_peers_connect(
                PacketDropPolicy::ReceiveAll,
                PacketDropPolicy::ReceiveAll,
                InRemoteInbound::Either,
            )
            .await
        }
    }
}
