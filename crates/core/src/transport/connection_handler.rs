use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{GlobalExecutor, GlobalRng, PCK_VERSION};
use crate::transport::crypto::TransportSecretKey;
use crate::transport::packet_data::UnknownEncryption;
use crate::transport::symmetric_message::OutboundConnection;
use crate::util::backoff::ExponentialBackoff;
use aes_gcm::{Aes128Gcm, KeyInit};
use dashmap::DashSet;
use futures::{
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
    Future, FutureExt, TryFutureExt,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
    task_local,
};
use tracing::{span, Instrument};
use version_cmp::PROTOC_VERSION;

use super::{
    congestion_control::CongestionControlConfig,
    crypto::{TransportKeypair, TransportPublicKey},
    fast_channel::{self, FastSender},
    global_bandwidth::GlobalBandwidthManager,
    packet_data::{PacketData, SymmetricAES, MAX_PACKET_SIZE},
    peer_connection::{PeerConnection, RemoteConnection},
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
    token_bucket::TokenBucket,
    Socket, TransportError,
};
use crate::simulation::{RealTime, TimeSource};

// Constants for interval increase
const INITIAL_INTERVAL: Duration = Duration::from_millis(50);

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
/// * `listen_host` - IP address to bind to (use 0.0.0.0 for all interfaces)
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
) -> Result<(OutboundConnectionHandler<S>, InboundConnectionHandler<S>), TransportError> {
    // Bind the UDP socket to the specified port with retry for transient failures
    let bind_addr: SocketAddr = (listen_host, listen_port).into();
    tracing::debug!(
        target: "freenet_core::transport::send_debug",
        bind_addr = %bind_addr,
        is_gateway,
        "Binding UDP socket"
    );

    let socket = bind_socket_with_retry::<S>(bind_addr, SOCKET_BIND_MAX_RETRIES).await?;

    tracing::info!(
        target: "freenet_core::transport::send_debug",
        bind_addr = %bind_addr,
        is_gateway,
        "UDP socket bound successfully"
    );
    let (och, new_connection_notifier) = OutboundConnectionHandler::config_listener(
        Arc::new(socket),
        keypair,
        is_gateway,
        (listen_host, listen_port).into(),
        bandwidth_limit,
        global_bandwidth,
        ledbat_min_ssthresh,
        congestion_config,
    )?;
    Ok((
        och,
        InboundConnectionHandler {
            new_connection_notifier,
        },
    ))
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

    Err(last_err
        .map(TransportError::from)
        .unwrap_or_else(|| TransportError::IO(std::io::ErrorKind::AddrInUse.into())))
}

/// Receives  new inbound connections from the network.
pub struct InboundConnectionHandler<S = UdpSocket, TS: TimeSource = RealTime> {
    new_connection_notifier: mpsc::Receiver<PeerConnection<S, TS>>,
}

impl<S, TS: TimeSource> InboundConnectionHandler<S, TS> {
    pub async fn next_connection(&mut self) -> Option<PeerConnection<S, TS>> {
        self.new_connection_notifier.recv().await
    }
}

/// Requests a new outbound connection to a remote peer.
pub struct OutboundConnectionHandler<S = UdpSocket, TS: TimeSource = RealTime> {
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent<S, TS>)>,
    expected_non_gateway: Arc<DashSet<IpAddr>>,
}

impl<S, TS: TimeSource> Clone for OutboundConnectionHandler<S, TS> {
    fn clone(&self) -> Self {
        Self {
            send_queue: self.send_queue.clone(),
            expected_non_gateway: self.expected_non_gateway.clone(),
        }
    }
}

#[allow(private_bounds)]
impl<S: Socket> OutboundConnectionHandler<S> {
    #[allow(clippy::too_many_arguments)]
    fn config_listener(
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
        socket_addr: SocketAddr,
        bandwidth_limit: Option<usize>,
        global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
        ledbat_min_ssthresh: Option<usize>,
        congestion_config: Option<CongestionControlConfig>,
    ) -> Result<(Self, mpsc::Receiver<PeerConnection<S>>), TransportError> {
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(100);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(10);
        let expected_non_gateway = Arc::new(DashSet::new());

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
            time_source,
            congestion_config: Some(congestion_config.unwrap_or_default()),
        };
        let connection_handler = OutboundConnectionHandler {
            send_queue: conn_handler_sender,
            expected_non_gateway,
        };

        // Packets are now sent directly to socket from each connection,
        // bypassing the centralized rate limiter that was causing serialization bottlenecks.
        // Per-connection rate limiting is handled by TokenBucket and LEDBAT in RemoteConnection.
        GlobalExecutor::spawn(RANDOM_U64.scope(
            {
                // Use GlobalRng for deterministic simulation
                let seed = GlobalRng::random_u64();
                let mut rng = StdRng::seed_from_u64(seed);
                rng.random()
            },
            transport.listen(),
        ));

        Ok((connection_handler, new_connection_notifier))
    }

    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn new_test(
        socket_addr: SocketAddr,
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
    ) -> Result<(Self, mpsc::Receiver<PeerConnection<S>>), TransportError> {
        Self::config_listener(
            socket,
            keypair,
            is_gateway,
            socket_addr,
            None,
            None,
            None,
            None,
        )
    }

    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn new_test_with_bandwidth(
        socket_addr: SocketAddr,
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
        bandwidth_limit: Option<usize>,
    ) -> Result<(Self, mpsc::Receiver<PeerConnection<S>>), TransportError> {
        Self::config_listener(
            socket,
            keypair,
            is_gateway,
            socket_addr,
            bandwidth_limit,
            None,
            None,
            None,
        )
    }

    pub async fn connect(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
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

/// A handle for registering expected inbound connections.
///
/// This is a type-erased wrapper around the expected inbound IP set, allowing
/// `EventListenerState` to register expected connections without being generic
/// over the socket type.
#[derive(Clone)]
pub struct ExpectedInboundTracker(Arc<DashSet<IpAddr>>);

impl ExpectedInboundTracker {
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
            time_source,
            congestion_config,
        };
        let connection_handler = OutboundConnectionHandler {
            send_queue: conn_handler_sender,
            expected_non_gateway,
        };

        // Packets are now sent directly to socket from each connection,
        // bypassing the centralized rate limiter that was causing serialization bottlenecks.
        // Per-connection rate limiting is handled by TokenBucket and LEDBAT in RemoteConnection.
        GlobalExecutor::spawn(RANDOM_U64.scope(
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

    /// Connect to a remote peer using VirtualTime.
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
    time_source: T,
    /// Congestion control configuration for new connections.
    /// When None, uses the default configuration (BBR).
    congestion_config: Option<CongestionControlConfig>,
}

type OngoingConnectionResult<S, T> = Option<
    Result<
        Result<(RemoteConnection<S, T>, InboundRemoteConnection), (TransportError, SocketAddr)>,
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
        packet_sender: FastSender<PacketData<UnknownEncryption>>,
    },
    /// Outbound NAT traversal handshake in progress.
    NatTraversal {
        packet_sender: FastSender<PacketData<UnknownEncryption>>,
        result_sender: oneshot::Sender<Result<RemoteConnection<S, T>, TransportError>>,
    },
    /// Connection fully established with symmetric encryption.
    Established {
        inbound_packet_sender: FastSender<PacketData<UnknownEncryption>>,
    },
    /// Connection recently closed - drains in-flight packets.
    /// Allows new intro packets for reconnection.
    RecentlyClosed { closed_at_nanos: u64 },
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
            match inbound_packet_sender.try_send(packet) {
                Ok(()) => Ok(true),
                Err(fast_channel::TrySendError::Full(_)) => Ok(false),
                Err(fast_channel::TrySendError::Disconnected(_)) => {
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
                Err(fast_channel::TrySendError::Full(_)) => Ok(false),
                Err(fast_channel::TrySendError::Disconnected(_)) => Ok(false), // Handshake completing
            }
        } else {
            Err(())
        }
    }

    /// Try to send a packet to an ongoing NAT traversal.
    /// Returns Ok(true) if sent, Ok(false) if channel closed.
    async fn send_nat_traversal(
        &self,
        addr: &SocketAddr,
        packet: PacketData<UnknownEncryption>,
    ) -> Result<bool, ()> {
        if let Some(ConnectionState::NatTraversal { packet_sender, .. }) = self.states.get(addr) {
            match packet_sender.send_async(packet).await {
                Ok(()) => Ok(true),
                Err(_) => Ok(false),
            }
        } else {
            Err(())
        }
    }

    /// Start a gateway handshake. Only succeeds if no active state exists.
    fn start_gateway_handshake(
        &mut self,
        addr: SocketAddr,
        packet_sender: FastSender<PacketData<UnknownEncryption>>,
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
        inbound_packet_sender: FastSender<PacketData<UnknownEncryption>>,
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
        packet_sender: FastSender<PacketData<UnknownEncryption>>,
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
        inbound_packet_sender: FastSender<PacketData<UnknownEncryption>>,
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
                let _ = result_sender.send(Err(TransportError::ConnectionClosed(*addr)));
            }
            _ => {}
        }
    }

    /// Remove established connection and return the packet sender (for detection purposes).
    fn remove_established(
        &mut self,
        addr: &SocketAddr,
    ) -> Option<FastSender<PacketData<UnknownEncryption>>> {
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

impl<S: Socket, T: TimeSource> UdpPacketsListener<S, T> {
    #[tracing::instrument(level = "debug", name = "transport_listener", fields(peer = %self.this_peer_keypair.public), skip_all)]
    async fn listen(mut self) -> Result<(), TransportError> {
        tracing::debug!(
            bind_addr = %self.this_addr,
            "Listening for packets"
        );
        let mut buf = [0u8; MAX_PACKET_SIZE];
        let mut connection_tasks = FuturesUnordered::new();
        let mut gw_connection_tasks = FuturesUnordered::new();
        let mut outdated_peer: HashMap<SocketAddr, u64> = HashMap::new();
        let mut last_cleanup_nanos = self.time_source.now_nanos();

        'outer: loop {
            // DST: Use deterministic_select! for fair but deterministic branch ordering
            crate::deterministic_select! {
                recv_result = self.socket_listener.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, remote_addr)) => {
                            if let Some(time_nanos) = outdated_peer.get(&remote_addr) {
                                let now_nanos = self.time_source.now_nanos();
                                if now_nanos.saturating_sub(*time_nanos) < Duration::from_secs(60 * 10).as_nanos() as u64 {
                                    let packet_data = PacketData::<_, MAX_PACKET_SIZE>::from_buf(&buf[..size]);
                                    if packet_data.is_intro_packet() {
                                        if self.connections.is_rate_limited(&remote_addr) {
                                            continue;
                                        }
                                        self.connections.record_asym_attempt(remote_addr);
                                        if let Ok(decrypted) = packet_data.try_decrypt_asym(&self.this_peer_keypair.secret) {
                                            let decrypted_data = decrypted.data();
                                            let proto_len = PROTOC_VERSION.len();
                                            if decrypted_data.len() >= proto_len + 16
                                                && decrypted_data[..proto_len] == PROTOC_VERSION
                                            {
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
                                        // Check for new identity (peer restart)
                                        let is_new_identity = if self.is_gateway && packet_data.is_intro_packet() {
                                            if self.connections.is_rate_limited(&remote_addr) {
                                                false
                                            } else {
                                                self.connections.record_asym_attempt(remote_addr);
                                                match packet_data.try_decrypt_asym(&self.this_peer_keypair.secret) {
                                                    Ok(decrypted) => {
                                                        let decrypted_data = decrypted.data();
                                                        let proto_len = PROTOC_VERSION.len();
                                                        decrypted_data.len() >= proto_len + 16
                                                            && decrypted_data[..proto_len] == PROTOC_VERSION
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
                                                "Detected new peer identity. Resetting session."
                                            );
                                            self.connections.clear_rate_limit(&remote_addr);
                                            self.connections.remove_established(&remote_addr);
                                            // Fall through to gateway handler for fresh session
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
                                                    tracing::warn!(peer_addr = %remote_addr, "Connection closed");
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
                                        let _ = self.connections.send_nat_traversal(&remote_addr, packet_data).await;
                                        continue;
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

                                // Rate limit intro attempts
                                if self.connections.is_rate_limited(&remote_addr) {
                                    tracing::trace!(peer_addr = %remote_addr, "Rate limiting gateway intro attempt");
                                    continue;
                                }
                                self.connections.record_asym_attempt(remote_addr);

                                // Only process intro packets
                                if !packet_data.is_intro_packet() {
                                    tracing::trace!(peer_addr = %remote_addr, "Dropping non-intro packet from unknown address");
                                    continue;
                                }

                                let inbound_key_bytes = key_from_addr(&remote_addr);
                                let (gw_ongoing_connection, packets_sender) = self.gateway_connection(packet_data, remote_addr, inbound_key_bytes);
                                if !self.connections.start_gateway_handshake(remote_addr, packets_sender) {
                                    continue; // State transition failed (e.g., RecentlyClosed still draining)
                                }
                                let task = GlobalExecutor::spawn(gw_ongoing_connection
                                    .instrument(tracing::span!(tracing::Level::DEBUG, "gateway_connection"))
                                    .map_err(move |error| {
                                        tracing::warn!(peer_addr = %remote_addr, error = %error, "Gateway connection error");
                                        (error, remote_addr)
                                    }));
                                gw_connection_tasks.push(task);
                                continue;
                            } else {
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
                            tracing::error!(
                                error = ?e,
                                "Failed to receive UDP packet"
                            );
                            return Err(e.into());
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
                                    break 'outer Err(TransportError::ConnectionClosed(self.this_addr));
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
                            tracing::error!(error = %error, peer_addr = %remote_addr, "Failed to establish gateway connection");
                            if matches!(error, TransportError::ProtocolVersionMismatch { .. }) {
                                outdated_peer.insert(remote_addr, self.time_source.now_nanos());
                                crate::transport::signal_version_mismatch();
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
                        Ok((outbound_remote_conn, inbound_remote_connection)) => {
                            let remote_addr = outbound_remote_conn.remote_addr;
                            self.expected_non_gateway.remove(&remote_addr.ip());

                            // Atomic transition: NatTraversal -> Established
                            if let Some(result_sender) = self.connections.complete_nat_traversal(
                                remote_addr,
                                inbound_remote_connection.inbound_packet_sender,
                            ) {
                                tracing::info!(peer_addr = %remote_addr, "NAT traversal connection established");
                                let _ = result_sender.send(Ok(outbound_remote_conn));
                            } else {
                                tracing::debug!(peer_addr = %remote_addr, "Connection state changed during NAT traversal");
                            }
                        }
                        // Failed NAT traversal
                        Err((error, remote_addr)) => {
                            match &error {
                                TransportError::ProtocolVersionMismatch { .. } => {
                                    tracing::warn!(%error);
                                    crate::transport::signal_version_mismatch();
                                }
                                _ => {
                                    tracing::error!(error = %error, peer_addr = %remote_addr, "Failed NAT traversal");
                                }
                            }
                            self.expected_non_gateway.remove(&remote_addr.ip());
                            if let Some(result_sender) = self.connections.fail_nat_traversal(&remote_addr) {
                                let _ = result_sender.send(Err(error));
                            }
                        }
                    }
                },
                // Handle new connection requests
                connection_event = self.connection_handler.recv() => {
                    let Some((remote_addr, event)) = connection_event else {
                        tracing::debug!(bind_addr = %self.this_addr, "Connection handler closed");
                        return Ok(());
                    };
                    tracing::debug!(peer_addr = %remote_addr, "Received connection event");
                    let ConnectionEvent::ConnectionStart { remote_public_key, open_connection } = event;

                    // Check for existing NAT traversal in progress
                    if self.connections.has_nat_traversal(&remote_addr) {
                        tracing::debug!(peer_addr = %remote_addr, "NAT traversal already in progress");
                        let _ = open_connection.send(Err(TransportError::ConnectionEstablishmentFailure {
                            cause: "connection attempt already in progress".into(),
                        }));
                        continue;
                    }

                    tracing::info!(peer_addr = %remote_addr, "Starting NAT traversal");
                    let (ongoing_connection, packets_sender) = self.traverse_nat(remote_addr, remote_public_key.clone());

                    if !self.connections.start_nat_traversal(remote_addr, packets_sender, open_connection) {
                        continue;
                    }

                    self.expected_non_gateway.insert(remote_addr.ip());
                    let task = GlobalExecutor::spawn(ongoing_connection
                        .map_err(move |err| (err, remote_addr))
                        .instrument(span!(tracing::Level::DEBUG, "traverse_nat"))
                    );
                    connection_tasks.push(task);
                },
                _ = async {}, if {
                    let now_nanos = self.time_source.now_nanos();
                    now_nanos.saturating_sub(last_cleanup_nanos) >= RATE_LIMIT_CLEANUP_INTERVAL.as_nanos() as u64
                } => {
                    self.connections.cleanup_expired();
                    last_cleanup_nanos = self.time_source.now_nanos();
                }
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn gateway_connection(
        &mut self,
        remote_intro_packet: PacketData<UnknownEncryption>,
        remote_addr: SocketAddr,
        inbound_key_bytes: [u8; 16],
    ) -> (
        GatewayConnectionFuture<S, T>,
        FastSender<PacketData<UnknownEncryption>>,
    ) {
        let secret = self.this_peer_keypair.secret.clone();
        let bandwidth_limit = self.bandwidth_limit;
        let global_bandwidth = self.global_bandwidth.clone();
        let _ledbat_min_ssthresh = self.ledbat_min_ssthresh;
        let socket = self.socket_listener.clone();
        let time_source = self.time_source.clone();
        let congestion_config = self.congestion_config.clone();

        let (inbound_from_remote, next_inbound) =
            fast_channel::bounded::<PacketData<UnknownEncryption>>(100);
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
            if protoc != PROTOC_VERSION {
                let packet = SymmetricMessage::ack_error(&outbound_key)?;
                socket
                    .send_to(&packet.prepared_send(), remote_addr)
                    .await
                    .map_err(|_| TransportError::ChannelClosed)?;
                return Err(TransportError::ProtocolVersionMismatch {
                    expected: PCK_VERSION.to_string(),
                    actual: PCK_VERSION, // We expected our version, they sent something else
                });
            }

            let inbound_key = Aes128Gcm::new(&inbound_key_bytes.into());
            let outbound_ack_packet =
                SymmetricMessage::ack_ok(&outbound_key, inbound_key_bytes, remote_addr)?;

            tracing::trace!(
                peer_addr = %remote_addr,
                direction = "inbound",
                packet_len = outbound_ack_packet.data().len(),
                "Sending outbound ack packet"
            );

            socket
                .send_to(&outbound_ack_packet.clone().prepared_send(), remote_addr)
                .await
                .map_err(|_| TransportError::ChannelClosed)?;

            // wait until the remote sends the ack packet
            let timeout_result = crate::deterministic_select! {
                result = next_inbound.recv_async() => Some(result),
                _ = time_source.sleep(Duration::from_secs(5)) => None,
            };
            match timeout_result {
                Some(Ok(packet)) => {
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
                }
                Some(Err(_)) => {
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

            // Initialize congestion controller (BBR by default, configurable for benchmarks)
            let congestion_controller = congestion_config
                .clone()
                .unwrap_or_default()
                .build_arc_with_time_source(time_source.clone());

            // Initialize token bucket for smooth packet pacing
            // Use global bandwidth manager if configured, otherwise fall back to per-connection limit
            let initial_rate = if let Some(ref global) = global_bandwidth {
                global.register_connection()
            } else {
                bandwidth_limit.unwrap_or(10_000_000) // 10 MB/s default
            };
            let token_bucket = Arc::new(TokenBucket::new_with_time_source(
                1_000_000, // capacity = 1 MB burst (prevents token starvation on localhost)
                initial_rate,
                time_source.clone(),
            ));

            let (inbound_packet_tx, inbound_packet_rx) = fast_channel::bounded(1000);

            // Issue #2395: Drain any packets that arrived during the handshake.
            // The peer may send application-level messages (like ConnectRequest) immediately
            // after its side of the handshake completes, but before our handshake finishes.
            // These packets are buffered in `next_inbound` but only the ACK packet is read above.
            // Forward any remaining packets to the new connection channel so they're not lost.
            let mut forwarded_count = 0;
            while let Ok(packet) = next_inbound.try_recv() {
                if inbound_packet_tx.try_send(packet).is_err() {
                    tracing::warn!(
                        peer_addr = %remote_addr,
                        "Failed to forward handshake-buffered packet to connection channel (channel full)"
                    );
                    break;
                }
                forwarded_count += 1;
            }
            if forwarded_count > 0 {
                tracing::debug!(
                    peer_addr = %remote_addr,
                    forwarded_count,
                    "Forwarded packets buffered during handshake to connection channel"
                );
            }

            let remote_conn = RemoteConnection {
                outbound_symmetric_key: outbound_key,
                remote_addr,
                sent_tracker: sent_tracker.clone(),
                last_packet_id: Arc::new(AtomicU32::new(0)),
                inbound_packet_recv: inbound_packet_rx,
                inbound_symmetric_key: inbound_key,
                inbound_symmetric_key_bytes: inbound_key_bytes,
                global_bandwidth: global_bandwidth.clone(),
                my_address: None,
                transport_secret_key: secret,
                congestion_controller,
                token_bucket,
                socket,
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
        FastSender<PacketData<UnknownEncryption>>,
    ) {
        tracing::debug!(
            peer_addr = %remote_addr,
            direction = "outbound",
            "Starting NAT traversal"
        );

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
        ) -> Result<(), ()> {
            if let Ok(decrypted_intro_packet) = packet.try_decrypt_asym(transport_secret_key) {
                tracing::debug!(
                    peer_addr = %remote_addr,
                    direction = "outbound",
                    "Received intro packet"
                );
                let protoc = &decrypted_intro_packet.data()[..PROTOC_VERSION.len()];
                if protoc != PROTOC_VERSION {
                    tracing::debug!(
                        peer_addr = %remote_addr,
                        remote_protoc = ?protoc,
                        this_protoc = ?PROTOC_VERSION,
                        direction = "outbound",
                        "Remote is using a different protocol version"
                    );
                    return Err(());
                }
                let outbound_key_bytes =
                    &decrypted_intro_packet.data()[PROTOC_VERSION.len()..PROTOC_VERSION.len() + 16];
                let outbound_key =
                    Aes128Gcm::new_from_slice(outbound_key_bytes).expect("correct length");
                *outbound_sym_key = Some(outbound_key.clone());
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
        let (inbound_from_remote, next_inbound) =
            fast_channel::bounded::<PacketData<UnknownEncryption>>(100);
        let this_addr = self.this_addr;
        let f = async move {
            tracing::info!(
                peer_addr = %remote_addr,
                direction = "outbound",
                "Starting outbound handshake (NAT traversal)"
            );
            let mut state = HandshakePhase::StartOutbound;
            let mut attempts = 0usize;
            let start_time_nanos = time_source.now_nanos();
            let overall_deadline = Duration::from_secs(3);

            // Generate symmetric key using GlobalRng for deterministic simulation
            let mut inbound_sym_key_bytes = [0u8; 16];
            GlobalRng::fill_bytes(&mut inbound_sym_key_bytes);
            let inbound_sym_key = Aes128Gcm::new(&inbound_sym_key_bytes.into());

            let mut outbound_sym_key: Option<Aes128Gcm> = None;
            let outbound_intro_packet = {
                let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
                data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
                data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
                PacketData::<_, MAX_PACKET_SIZE>::encrypt_with_pubkey(&data, &remote_public_key)
            };

            let mut sent_tracker = SentPacketTracker::new_with_time_source(time_source.clone());

            // NAT traversal: keep sending packets throughout the deadline to maintain NAT bindings.
            // The acceptor may start hole-punching well before the joiner, so we must keep
            // sending until the deadline expires, not just until we've sent MAX_ATTEMPTS packets.
            while time_source.now_nanos().saturating_sub(start_time_nanos)
                < overall_deadline.as_nanos() as u64
            {
                // Send a packet if we haven't exceeded the max attempts
                if attempts < NAT_TRAVERSAL_MAX_ATTEMPTS {
                    match state {
                        HandshakePhase::StartOutbound => {
                            tracing::trace!(
                                peer_addr = %remote_addr,
                                direction = "outbound",
                                "Sending protocol version and inbound key"
                            );
                            socket
                                .send_to(outbound_intro_packet.data(), remote_addr)
                                .await
                                .map_err(|_| TransportError::ChannelClosed)?;
                            attempts += 1;
                        }
                        HandshakePhase::RemoteInbound => {
                            tracing::trace!(
                                peer_addr = %remote_addr,
                                direction = "outbound",
                                "Sending back protocol version and inbound key to remote"
                            );
                            let our_inbound = SymmetricMessage::ack_ok(
                                outbound_sym_key.as_ref().expect("should be set"),
                                inbound_sym_key_bytes,
                                remote_addr,
                            )?;
                            socket
                                .send_to(our_inbound.data(), remote_addr)
                                .await
                                .map_err(|_| TransportError::ChannelClosed)?;
                            sent_tracker.report_sent_packet(
                                SymmetricMessage::FIRST_PACKET_ID,
                                our_inbound.data().into(),
                            );
                            attempts += 1;
                        }
                    }
                }
                let next_inbound_result = crate::deterministic_select! {
                    result = next_inbound.recv_async() => Some(result),
                    _ = time_source.sleep(Duration::from_millis(200)) => None,
                };
                match next_inbound_result {
                    Some(Ok(packet)) => {
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

                                    match symmetric_message.payload {
                                        SymmetricMessagePayload::AckConnection {
                                            result:
                                                Ok(OutboundConnection {
                                                    key,
                                                    remote_addr: my_address,
                                                }),
                                        } => {
                                            let outbound_sym_key = Aes128Gcm::new_from_slice(&key)
                                                .map_err(|_| {
                                                    TransportError::ConnectionEstablishmentFailure {
                                                        cause: "invalid symmetric key".into(),
                                                    }
                                                })?;
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
                                            socket
                                                .send_to(ack_packet.data(), remote_addr)
                                                .await
                                                .map_err(|_| TransportError::ChannelClosed)?;
                                            let (inbound_sender, inbound_recv) =
                                                fast_channel::bounded(1000);

                                            // Initialize congestion controller (BBR by default, configurable for benchmarks)
                                            let congestion_controller = congestion_config
                                                .clone()
                                                .unwrap_or_default()
                                                .build_arc_with_time_source(time_source.clone());

                                            // Initialize token bucket
                                            // Use global bandwidth manager if configured
                                            let initial_rate =
                                                if let Some(ref global) = global_bandwidth {
                                                    global.register_connection()
                                                } else {
                                                    bandwidth_limit.unwrap_or(10_000_000)
                                                };
                                            let token_bucket =
                                                Arc::new(TokenBucket::new_with_time_source(
                                                    1_000_000, // capacity = 1 MB burst (prevents token starvation on localhost)
                                                    initial_rate,
                                                    time_source.clone(),
                                                ));

                                            tracing::info!(
                                                peer_addr = %remote_addr,
                                                attempts,
                                                direction = "outbound",
                                                "Outbound handshake completed (ack path)"
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
                                                    transport_secret_key: transport_secret_key
                                                        .clone(),
                                                    congestion_controller,
                                                    token_bucket,
                                                    socket: socket.clone(),
                                                    global_bandwidth: global_bandwidth.clone(),
                                                    time_source: time_source.clone(),
                                                },
                                                InboundRemoteConnection {
                                                    inbound_packet_sender: inbound_sender,
                                                },
                                            ));
                                        }
                                        SymmetricMessagePayload::AckConnection {
                                            result: Err(err),
                                        } => {
                                            return Err(handle_ack_connection_error(err));
                                        }
                                        _ => {
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
                                // if is not an intro packet, the connection is successful and we can proceed
                                let (inbound_sender, inbound_recv) = fast_channel::bounded(1000);

                                // Initialize congestion controller (BBR by default, configurable for benchmarks)
                                let congestion_controller = congestion_config
                                    .clone()
                                    .unwrap_or_default()
                                    .build_arc_with_time_source(time_source.clone());

                                // Initialize token bucket
                                // Use global bandwidth manager if configured
                                let initial_rate = if let Some(ref global) = global_bandwidth {
                                    global.register_connection()
                                } else {
                                    bandwidth_limit.unwrap_or(10_000_000)
                                };
                                let token_bucket = Arc::new(TokenBucket::new_with_time_source(
                                    1_000_000, // capacity = 1 MB burst (prevents token starvation on localhost)
                                    initial_rate,
                                    time_source.clone(),
                                ));

                                tracing::info!(
                                    peer_addr = %remote_addr,
                                    attempts,
                                    direction = "outbound",
                                    "Outbound handshake completed (inbound ack path)"
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
                                        transport_secret_key: transport_secret_key.clone(),
                                        congestion_controller,
                                        token_bucket,
                                        socket: socket.clone(),
                                        global_bandwidth: global_bandwidth.clone(),
                                        time_source: time_source.clone(),
                                    },
                                    InboundRemoteConnection {
                                        inbound_packet_sender: inbound_sender,
                                    },
                                ));
                            }
                        }
                    }
                    Some(Err(_)) => {
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

            let elapsed_nanos = time_source.now_nanos().saturating_sub(start_time_nanos);
            let elapsed_ms = elapsed_nanos / 1_000_000;
            tracing::warn!(
                peer_addr = %remote_addr,
                attempts,
                elapsed_ms,
                direction = "outbound",
                "Outbound handshake failed: max connection attempts reached"
            );
            Err(TransportError::ConnectionEstablishmentFailure {
                cause: "max connection attempts reached".into(),
            })
        };
        (f.boxed(), inbound_from_remote)
    }
}

fn handle_ack_connection_error(err: Cow<'static, str>) -> TransportError {
    if let Some(expected) = err.split("expected version").nth(1) {
        TransportError::ProtocolVersionMismatch {
            expected: expected.trim().to_string(),
            actual: PCK_VERSION,
        }
    } else {
        TransportError::ConnectionEstablishmentFailure { cause: err }
    }
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
    },
}

struct InboundRemoteConnection {
    inbound_packet_sender: FastSender<PacketData<UnknownEncryption>>,
}

mod version_cmp {
    use crate::config::PCK_VERSION;

    pub(super) const PROTOC_VERSION: [u8; 8] = parse_version_with_flags(PCK_VERSION);

    const fn parse_version_with_flags(version: &str) -> [u8; 8] {
        let mut major = 0u8;
        let mut minor = 0u8;
        let mut patch = 0u8;
        let mut flags = 0u32;
        let mut state = 0; // 0: major, 1: minor, 2: patch

        let bytes = version.as_bytes();
        let mut i = 0;

        // Parse major.minor.patch
        while i < bytes.len() {
            let c = bytes[i];
            if c == b'.' {
                state += 1;
            } else if c >= b'0' && c <= b'9' {
                let digit = c - b'0';
                match state {
                    0 => major = major * 10 + digit,
                    1 => minor = minor * 10 + digit,
                    2 => patch = patch * 10 + digit,
                    _ => {}
                }
            } else {
                break; // Move to flags processing.
            }
            i += 1;
        }

        // Parse flags (pre-release only)
        if i < bytes.len() && bytes[i] == b'-' {
            i += 1; // Skip the '-'

            flags = get_flag_hash(bytes, i);
        }

        // Encode into [u8; 8]
        [
            major,
            minor,
            patch,
            (flags >> 24) as u8,
            (flags >> 16) as u8,
            (flags >> 8) as u8,
            flags as u8,
            0, // Reserved for future use
        ]
    }

    const fn get_flag_hash(bytes: &[u8], start_index: usize) -> u32 {
        let mut hash = 0u32;
        let mut i = start_index;

        while i < bytes.len() {
            let byte = bytes[i];
            hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
            i += 1;
        }
        hash
    }

    #[cfg(test)]
    #[test]
    fn test_parse_version_with_flags() {
        fn decode_version_from_bytes(bytes: [u8; 8]) -> String {
            let major = bytes[0];
            let minor = bytes[1];
            let patch = bytes[2];

            let flags = u32::from_be_bytes([bytes[3], bytes[4], bytes[5], bytes[6]]);

            let flags_str = match flags {
                _ if flags == get_flag_hash(b"alpha", 0) => "alpha",
                _ if flags == get_flag_hash(b"beta2", 0) => "beta2",
                _ if flags == get_flag_hash(b"rc1", 0) => "rc1",
                _ if flags == get_flag_hash(b"rc2", 0) => "rc2",
                _ => "",
            };

            if !flags_str.is_empty() {
                format!("{major}.{minor}.{patch}-{flags_str}")
            } else {
                format!("{major}.{minor}.{patch}")
            }
        }

        let test_cases = vec![
            "1.2.3",
            "1.2.3-alpha",
            "255.255.255",
            "10.20.30-beta2",
            "1.2.3-rc1",
            "1.2.3-rc2",
        ];

        for version_str in test_cases {
            // Step 1: Encode the version string into bytes
            let encoded = parse_version_with_flags(version_str);

            // Step 2: Decode the bytes back into a version string
            let decoded = decode_version_from_bytes(encoded);

            // Step 3: Compare the decoded string with the original version string
            assert_eq!(
                decoded, version_str,
                "Failed for version string '{version_str}', decoded as '{decoded}'"
            );
        }

        let rc1 = parse_version_with_flags("0.1.0-rc1");
        let rc2 = parse_version_with_flags("0.1.0-rc2");
        assert_ne!(rc1, rc2, "rc1 and rc2 should have different flags");
    }

    /// Verifies that handshake completion is atomic - state transitions directly
    /// from GatewayHandshake to Established with no intermediate None state.
    #[test]
    fn test_atomic_handshake_completion_no_packet_loss() {
        use super::{fast_channel, ConnectionStateManager};
        use crate::simulation::VirtualTime;
        use crate::transport::packet_data::{PacketData, UnknownEncryption};
        use std::net::SocketAddr;
        use tokio::net::UdpSocket;

        let time = VirtualTime::new();
        let mut manager: ConnectionStateManager<UdpSocket, VirtualTime> =
            ConnectionStateManager::new(time);
        let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        // Create a channel for the handshake
        let (tx, _rx) = fast_channel::bounded::<PacketData<UnknownEncryption>>(16);

        // Start gateway handshake
        let started = manager.start_gateway_handshake(addr, tx);
        assert!(started, "Should start gateway handshake");
        assert!(
            manager.has_gateway_handshake(&addr),
            "Should be in GatewayHandshake state"
        );

        // Create channel for established connection
        let (established_tx, _established_rx) =
            fast_channel::bounded::<PacketData<UnknownEncryption>>(16);

        // Complete the handshake atomically
        let completed = manager.complete_gateway_handshake(addr, established_tx);
        assert!(completed, "Should complete gateway handshake");

        // Verify state is Established (not None) - proves no gap
        assert!(
            manager.is_established(&addr),
            "State must be Established immediately after completion - proves no race gap"
        );
        assert!(
            manager.get_state(&addr).is_some(),
            "get_state() must return Some immediately after transition"
        );
    }

    /// Proves RecentlyClosed state prevents misrouting packets to asymmetric decryption.
    ///
    /// Without RecentlyClosed, packets arriving after connection close would fall through
    /// to the asymmetric decryption handler (expensive and wrong).
    #[test]
    fn test_recently_closed_prevents_asymmetric_decryption() {
        use super::{
            fast_channel, ConnectionState, ConnectionStateManager, RECENTLY_CLOSED_DURATION,
        };
        use crate::simulation::VirtualTime;
        use crate::transport::packet_data::{PacketData, UnknownEncryption};
        use std::net::SocketAddr;
        use std::time::Duration;
        use tokio::net::UdpSocket;

        let time = VirtualTime::new();
        let mut manager: ConnectionStateManager<UdpSocket, VirtualTime> =
            ConnectionStateManager::new(time.clone());
        let addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();

        // Create an established connection
        let (tx, _rx) = fast_channel::bounded::<PacketData<UnknownEncryption>>(16);
        manager.start_gateway_handshake(addr, tx);
        let (established_tx, _established_rx) =
            fast_channel::bounded::<PacketData<UnknownEncryption>>(16);
        manager.complete_gateway_handshake(addr, established_tx);
        assert!(manager.is_established(&addr));

        // Close the connection
        manager.mark_closed(&addr);

        // Verify state is RecentlyClosed (not removed)
        let state = manager.get_state(&addr);
        assert!(
            state.is_some(),
            "State should exist as RecentlyClosed, not be removed"
        );
        assert!(
            matches!(state, Some(ConnectionState::RecentlyClosed { .. })),
            "State must be RecentlyClosed - prevents packets from falling through"
        );

        // Verify it's not treated as unknown (which would trigger asymmetric decryption)
        assert!(!manager.is_established(&addr), "Should not be Established");
        assert!(
            manager.get_state(&addr).is_some(),
            "Should NOT return None - that would trigger asymmetric handler"
        );

        // Advance time past expiration and cleanup
        time.advance(RECENTLY_CLOSED_DURATION + Duration::from_millis(1));
        manager.cleanup_expired();

        // Now the state should be gone
        assert!(
            manager.get_state(&addr).is_none(),
            "State should be cleaned up after expiration"
        );
    }
}

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
    use futures::{stream::FuturesOrdered, TryStreamExt};
    use rand::{Rng, SeedableRng};
    use serde::{de::DeserializeOwned, Serialize};
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
            _ => panic!("Expected ProtocolVersionMismatch error"),
        }

        let err_msg_no_version = "remote is using a different protocol version".to_string();
        match handle_ack_connection_error(err_msg_no_version.clone().into()) {
            TransportError::ConnectionEstablishmentFailure { cause } => {
                assert_eq!(cause, err_msg_no_version);
            }
            _ => panic!("Expected ConnectionEstablishmentFailure error"),
        }
    }

    /// Channel mapping for mock socket communication between peers.
    /// Packet format: (source_addr, data, available_at_nanos)
    /// available_at_nanos = VirtualTime when packet becomes available (0 = immediate)
    pub type Channels = Arc<DashMap<SocketAddr, mpsc::UnboundedSender<(SocketAddr, Vec<u8>, u64)>>>;

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
        inbound: Mutex<mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>, u64)>>,
        this: SocketAddr,
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        num_packets_sent: AtomicUsize,
        rng: std::sync::Mutex<rand::rngs::SmallRng>,
        channels: Channels,
        /// Optional VirtualTime for instant delay simulation in benchmarks.
        /// When set, packet delivery waits until VirtualTime reaches available_at_nanos.
        virtual_time: Option<crate::simulation::VirtualTime>,
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
            }
        }
    }

    impl Socket for MockSocket {
        async fn bind(_addr: SocketAddr) -> Result<Self, std::io::Error> {
            unimplemented!()
        }

        async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            // Wait for a packet from the channel
            let Some((remote, packet, available_at_nanos)) =
                self.inbound.try_lock().unwrap().recv().await
            else {
                tracing::error!(this = %self.this, "connection closed");
                return Err(std::io::ErrorKind::ConnectionAborted.into());
            };

            // Wait until the packet is available (simulates transit delay)
            // This registers a VirtualTime wakeup that auto-advance will process
            if let Some(ref vt) = self.virtual_time {
                if available_at_nanos > 0 {
                    vt.sleep_until(available_at_nanos).await;
                }
            }

            // Deliver the packet
            buf[..packet.len()].copy_from_slice(&packet[..]);
            Ok((packet.len(), remote))
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
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
            let (_delay, available_at_nanos) = self.compute_delay_and_timestamp();
            self.send_packet_internal(buf, target, available_at_nanos)
        }
    }

    impl MockSocket {
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
                .send((self.this, buf.to_vec(), available_at_nanos))
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
        )
        .await
    }

    async fn create_mock_peer_internal(
        packet_drop_policy: PacketDropPolicy,
        packet_delay_policy: PacketDelayPolicy,
        gateway: bool,
        channels: Channels,
        bandwidth_limit: Option<usize>,
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
        static PORT: AtomicU16 = AtomicU16::new(25000);

        let peer_keypair = TransportKeypair::new();
        let peer_pub = peer_keypair.public.clone();
        let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let socket = Arc::new(
            MockSocket::new(
                packet_drop_policy,
                packet_delay_policy,
                (Ipv4Addr::LOCALHOST, port).into(),
                channels,
            )
            .await,
        );
        let (peer_conn, inbound_conn) = OutboundConnectionHandler::new_test_with_bandwidth(
            (Ipv4Addr::LOCALHOST, port).into(),
            socket,
            peer_keypair,
            gateway,
            bandwidth_limit,
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
        static PORT: AtomicU16 = AtomicU16::new(25000);

        let peer_keypair = TransportKeypair::new();
        let peer_pub = peer_keypair.public.clone();
        let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Use provided time source or create a new one
        let vt = time_source.unwrap_or_default();

        let socket = Arc::new(
            MockSocket::with_time_source(
                packet_drop_policy,
                packet_delay_policy,
                (Ipv4Addr::LOCALHOST, port).into(),
                channels,
                Some(vt.clone()),
            )
            .await,
        );

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
    /// This function supports selecting the congestion control algorithm (BBR or LEDBAT)
    /// for benchmarking purposes. Pass `None` for `congestion_config` to use the default (BBR).
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
        )
        .await
        .map(|(pk, (o, _), s)| (pk, o, s))
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
                        let _ = peer_conn.send(test_gen_cp.gen_msg()).await;

                        tracing::info!(%peer_addr, "finished");
                        let _ = tokio::time::timeout(extra_wait, peer_conn.recv()).await;
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

        let peer_b = GlobalExecutor::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
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
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
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
            let _ = tokio::time::timeout(Duration::from_secs(3), conn.recv()).await;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            let _ = tokio::time::timeout(Duration::from_secs(3), conn.recv()).await;
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
                tracing::error!("Test timeout: connection detection took longer than 10s - investigate if this causes test failures");
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
            let _ = tokio::time::timeout(Duration::from_secs(10), conn.recv()).await;
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
            let _ = conn.send("complete").await.inspect_err(|error| {
                tracing::error!(%error, "error while sending 3rd message");
            });
            Ok::<_, anyhow::Error>(conn)
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        let _ = a?;
        let _ = b?;

        Ok(())
    }

    #[tokio::test]
    async fn simulate_gateway_connection() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            create_mock_gateway(Default::default(), channels).await?;

        let gw = GlobalExecutor::spawn(async move {
            let gw_conn = gw_conn.recv();
            let _ = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(gw_pub, gw_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(60), peer_b_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, gw)?;
        a?;
        b?;
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
            let _ = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(gw_pub, gw_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
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
            let _ = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(gw_pub, gw_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(10), peer_b_conn).await??;
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
            let _ = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = GlobalExecutor::spawn(async move {
            let peer_b_conn = peer_a.connect(gw_pub, gw_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(10), peer_b_conn).await??;
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
    /// We test with a 1400-byte payload to verify short message handling near the boundary.
    #[tokio::test]
    async fn simulate_send_max_short_message() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(Default::default(), channels).await?;

        // Use a large payload that will be sent as a short message
        // Vec<u8> of 1400 bytes should be well within short message limits
        let test_data: Vec<u8> = (0..1400).map(|i| (i % 256) as u8).collect();
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
            let _ = tokio::time::timeout(Duration::from_secs(1), conn.recv()).await??;
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
        // If the bug exists, this would take multiple retries (16+ attempts at 200ms each = 3+ seconds)
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
}
