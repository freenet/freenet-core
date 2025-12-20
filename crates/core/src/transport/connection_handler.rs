use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::PCK_VERSION;
use crate::transport::crypto::TransportSecretKey;
use crate::transport::packet_data::{AssymetricRSA, UnknownEncryption};
use crate::transport::symmetric_message::OutboundConnection;
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
    task, task_local,
};
use tracing::{span, Instrument};
use version_cmp::PROTOC_VERSION;

use super::{
    crypto::{TransportKeypair, TransportPublicKey},
    fast_channel::{self, FastSender},
    global_bandwidth::GlobalBandwidthManager,
    ledbat::LedbatController,
    packet_data::{PacketData, SymmetricAES, MAX_PACKET_SIZE},
    peer_connection::{PeerConnection, RemoteConnection},
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
    token_bucket::TokenBucket,
    Socket, TransportError,
};

// Constants for interval increase
const INITIAL_INTERVAL: Duration = Duration::from_millis(50);

/// Size of RSA-2048 encrypted intro packets (PKCS#1 v1.5 padding).
/// RSA-2048 produces 256-byte ciphertext for any input up to 245 bytes.
/// Used to detect new peer identities from existing addresses (issue #2277).
const RSA_INTRO_PACKET_SIZE: usize = 256;

/// Minimum interval between RSA decryption attempts for the same address.
/// Prevents CPU exhaustion from attackers sending 256-byte packets.
const RSA_DECRYPTION_RATE_LIMIT: Duration = Duration::from_secs(1);

pub type SerializedMessage = Vec<u8>;

type GatewayConnectionFuture<S> = BoxFuture<
    'static,
    Result<
        (
            RemoteConnection<S>,
            InboundRemoteConnection,
            PacketData<SymmetricAES>,
        ),
        TransportError,
    >,
>;

type TraverseNatFuture<S> =
    BoxFuture<'static, Result<(RemoteConnection<S>, InboundRemoteConnection), TransportError>>;

/// Maximum retries for socket binding in case of transient port conflicts.
/// This handles race conditions in test environments where ports are released
/// and rebound quickly.
const SOCKET_BIND_MAX_RETRIES: usize = 5;
const SOCKET_BIND_RETRY_DELAY_MS: u64 = 50;

pub(crate) async fn create_connection_handler<S: Socket>(
    keypair: TransportKeypair,
    listen_host: IpAddr,
    listen_port: u16,
    is_gateway: bool,
    bandwidth_limit: Option<usize>,
    global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
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
                tracing::debug!(
                    bind_addr = %addr,
                    attempt = attempt + 1,
                    max_retries,
                    "Socket bind failed with AddrInUse, retrying after delay"
                );
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(
                    SOCKET_BIND_RETRY_DELAY_MS * (1 << attempt), // exponential backoff
                ))
                .await;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err(last_err
        .map(TransportError::from)
        .unwrap_or_else(|| TransportError::IO(std::io::ErrorKind::AddrInUse.into())))
}

/// Receives  new inbound connections from the network.
pub struct InboundConnectionHandler<S = UdpSocket> {
    new_connection_notifier: mpsc::Receiver<PeerConnection<S>>,
}

impl<S> InboundConnectionHandler<S> {
    pub async fn next_connection(&mut self) -> Option<PeerConnection<S>> {
        self.new_connection_notifier.recv().await
    }
}

/// Requests a new outbound connection to a remote peer.
pub struct OutboundConnectionHandler<S = UdpSocket> {
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent<S>)>,
    expected_non_gateway: Arc<DashSet<IpAddr>>,
}

impl<S> Clone for OutboundConnectionHandler<S> {
    fn clone(&self) -> Self {
        Self {
            send_queue: self.send_queue.clone(),
            expected_non_gateway: self.expected_non_gateway.clone(),
        }
    }
}

#[allow(private_bounds)]
impl<S: Socket> OutboundConnectionHandler<S> {
    fn config_listener(
        socket: Arc<S>,
        keypair: TransportKeypair,
        is_gateway: bool,
        socket_addr: SocketAddr,
        bandwidth_limit: Option<usize>,
        global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
    ) -> Result<(Self, mpsc::Receiver<PeerConnection<S>>), TransportError> {
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(100);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(10);
        let expected_non_gateway = Arc::new(DashSet::new());

        let transport = UdpPacketsListener {
            is_gateway,
            socket_listener: socket,
            this_peer_keypair: keypair,
            remote_connections: BTreeMap::new(),
            connection_handler: conn_handler_receiver,
            new_connection_notifier: new_connection_sender,
            this_addr: socket_addr,
            dropped_packets: HashMap::new(),
            last_drop_warning: Instant::now(),
            bandwidth_limit,
            global_bandwidth,
            expected_non_gateway: expected_non_gateway.clone(),
            last_rsa_attempt: HashMap::new(),
        };
        let connection_handler = OutboundConnectionHandler {
            send_queue: conn_handler_sender,
            expected_non_gateway,
        };

        // Packets are now sent directly to socket from each connection,
        // bypassing the centralized rate limiter that was causing serialization bottlenecks.
        // Per-connection rate limiting is handled by TokenBucket and LEDBAT in RemoteConnection.
        task::spawn(RANDOM_U64.scope(
            {
                let mut rng = StdRng::seed_from_u64(rand::random());
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
        Self::config_listener(socket, keypair, is_gateway, socket_addr, None, None)
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
}

/// Handles UDP transport internally.
struct UdpPacketsListener<S = UdpSocket> {
    socket_listener: Arc<S>,
    remote_connections: BTreeMap<SocketAddr, InboundRemoteConnection>,
    connection_handler: mpsc::Receiver<(SocketAddr, ConnectionEvent<S>)>,
    this_peer_keypair: TransportKeypair,
    is_gateway: bool,
    new_connection_notifier: mpsc::Sender<PeerConnection<S>>,
    this_addr: SocketAddr,
    dropped_packets: HashMap<SocketAddr, u64>,
    last_drop_warning: Instant,
    bandwidth_limit: Option<usize>,
    /// Global bandwidth manager for fair sharing across all connections.
    /// When set, per-connection rates are derived from total_limit / active_connections.
    global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
    expected_non_gateway: Arc<DashSet<IpAddr>>,
    /// Rate limiting for RSA decryption attempts to prevent DoS (issue #2277).
    last_rsa_attempt: HashMap<SocketAddr, Instant>,
}

type OngoingConnection<S> = (
    FastSender<PacketData<UnknownEncryption>>,
    oneshot::Sender<Result<RemoteConnection<S>, TransportError>>,
);

type OngoingConnectionResult<S> = Option<
    Result<
        Result<(RemoteConnection<S>, InboundRemoteConnection), (TransportError, SocketAddr)>,
        tokio::task::JoinError,
    >,
>;

type GwOngoingConnectionResult<S> = Option<
    Result<
        Result<
            (
                RemoteConnection<S>,
                InboundRemoteConnection,
                PacketData<SymmetricAES>,
            ),
            (TransportError, SocketAddr),
        >,
        tokio::task::JoinError,
    >,
>;

#[cfg(any(test, feature = "bench"))]
impl<T> Drop for UdpPacketsListener<T> {
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

impl<S: Socket> UdpPacketsListener<S> {
    #[tracing::instrument(level = "debug", name = "transport_listener", fields(peer = %self.this_peer_keypair.public), skip_all)]
    async fn listen(mut self) -> Result<(), TransportError> {
        tracing::debug!(
            bind_addr = %self.this_addr,
            "Listening for packets"
        );
        let mut buf = [0u8; MAX_PACKET_SIZE];
        let mut ongoing_connections: BTreeMap<SocketAddr, OngoingConnection<S>> = BTreeMap::new();
        let mut ongoing_gw_connections: BTreeMap<
            SocketAddr,
            FastSender<PacketData<UnknownEncryption>>,
        > = BTreeMap::new();
        let mut connection_tasks = FuturesUnordered::new();
        let mut gw_connection_tasks = FuturesUnordered::new();
        let mut outdated_peer: HashMap<SocketAddr, Instant> = HashMap::new();

        'outer: loop {
            tokio::select! {
                recv_result = self.socket_listener.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, remote_addr)) => {
                            if let Some(time) = outdated_peer.get(&remote_addr) {
                                if time.elapsed() < Duration::from_secs(60 * 10) {
                                    continue;
                                } else {
                                    outdated_peer.remove(&remote_addr);
                                }
                            }
                            let packet_data = PacketData::from_buf(&buf[..size]);

                            tracing::trace!(
                                peer_addr = %remote_addr,
                                packet_size = size,
                                has_remote_conn = self.remote_connections.contains_key(&remote_addr),
                                has_ongoing_gw = ongoing_gw_connections.contains_key(&remote_addr),
                                has_ongoing_conn = ongoing_connections.contains_key(&remote_addr),
                                "Received packet from remote"
                            );

                            if let Some(remote_conn) = self.remote_connections.remove(&remote_addr) {
                                // Issue #2277: Check if this is a new intro packet from a peer that
                                // restarted with a new identity. RSA intro packets are exactly 256 bytes.
                                // If we can decrypt it as an intro, a new peer is connecting from the
                                // same IP:port (e.g., NAT assigned the same mapping after restart).
                                let is_new_identity = if self.is_gateway
                                    && size == RSA_INTRO_PACKET_SIZE
                                {
                                    // Rate limit RSA decryption attempts to prevent DoS
                                    let now = Instant::now();
                                    let rate_limited = self
                                        .last_rsa_attempt
                                        .get(&remote_addr)
                                        .is_some_and(|last| now.duration_since(*last) < RSA_DECRYPTION_RATE_LIMIT);

                                    if rate_limited {
                                        false
                                    } else {
                                        self.last_rsa_attempt.insert(remote_addr, now);
                                        // Try RSA decryption and validate intro packet structure
                                        match packet_data.try_decrypt_asym(&self.this_peer_keypair.secret) {
                                            Ok(decrypted) => {
                                                // Validate intro packet structure:
                                                // 1. Protocol version (PROTOC_VERSION.len() bytes)
                                                // 2. Outbound symmetric key (16 bytes)
                                                let decrypted_data = decrypted.data();
                                                let proto_len = PROTOC_VERSION.len();
                                                if decrypted_data.len() >= proto_len + 16
                                                    && decrypted_data[..proto_len] == PROTOC_VERSION
                                                {
                                                    true
                                                } else {
                                                    tracing::debug!(
                                                        peer_addr = %remote_addr,
                                                        "256-byte packet decrypted but not valid intro structure"
                                                    );
                                                    false
                                                }
                                            }
                                            Err(_) => false, // Not an RSA intro packet for us
                                        }
                                    }
                                } else {
                                    false
                                };

                                if is_new_identity {
                                    tracing::info!(
                                        peer_addr = %remote_addr,
                                        "Detected new peer identity from existing address (issue #2277). \
                                         Peer likely restarted with new identity. Resetting session."
                                    );
                                    // Clean up rate-limit tracking for the old peer
                                    self.last_rsa_attempt.remove(&remote_addr);
                                    // Don't reinsert - let the packet fall through to gateway_connection
                                    // which will establish a fresh session with the new peer
                                } else {
                                    // Forward packet to existing connection
                                    match remote_conn.inbound_packet_sender.try_send(packet_data) {
                                        Ok(_) => {
                                            self.remote_connections.insert(remote_addr, remote_conn);
                                            continue;
                                        }
                                        Err(fast_channel::TrySendError::Full(_)) => {
                                            // Channel full, reinsert connection
                                            self.remote_connections.insert(remote_addr, remote_conn);

                                            // Track dropped packets and log warnings periodically
                                            let dropped_count = self.dropped_packets.entry(remote_addr).or_insert(0);
                                            *dropped_count += 1;

                                            // Log warning every 10 seconds if packets are being dropped
                                            let now = Instant::now();
                                            if now.duration_since(self.last_drop_warning) > Duration::from_secs(10) {
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
                                                self.last_drop_warning = now;
                                            }

                                            // Drop the packet instead of falling through - prevents symmetric packets
                                            // from being sent to RSA decryption handlers
                                            continue;
                                        }
                                        Err(fast_channel::TrySendError::Disconnected(_)) => {
                                            // Channel closed, connection is dead
                                            tracing::warn!(
                                                peer_addr = %remote_addr,
                                                "Connection closed, removing from active connections"
                                            );
                                            // Clean up rate-limit tracking
                                            self.last_rsa_attempt.remove(&remote_addr);
                                            // Don't reinsert - connection is truly dead
                                            continue;
                                        }
                                    }
                                }
                            }

                            if let Some(inbound_packet_sender) = ongoing_gw_connections.get(&remote_addr) {
                                // Issue #2292: Filter out duplicate RSA intro packets during gateway handshake.
                                // The peer sends multiple RSA intro packets for NAT traversal reliability.
                                // After we detect a new identity and create a gateway_connection, subsequent
                                // intro packets (256 bytes) should be ignored - we're waiting for a symmetric
                                // ACK response which is not 256 bytes.
                                if size == RSA_INTRO_PACKET_SIZE {
                                    tracing::debug!(
                                        peer_addr = %remote_addr,
                                        direction = "inbound",
                                        packet_len = size,
                                        "Ignoring duplicate RSA intro packet during gateway handshake"
                                    );
                                    continue;
                                }

                                match inbound_packet_sender.try_send(packet_data) {
                                    Ok(_) => continue,
                                    Err(fast_channel::TrySendError::Full(_)) => {
                                        // Channel full, drop packet to prevent misrouting
                                        tracing::debug!(
                                            peer_addr = %remote_addr,
                                            direction = "inbound",
                                            "Ongoing gateway connection channel full, dropping packet"
                                        );
                                        continue;
                                    }
                                    Err(fast_channel::TrySendError::Disconnected(_)) => {
                                        // Channel closed, remove the connection
                                        ongoing_gw_connections.remove(&remote_addr);
                                        tracing::debug!(
                                            peer_addr = %remote_addr,
                                            direction = "inbound",
                                            "Ongoing gateway connection channel closed, removing"
                                        );
                                        continue;
                                    }
                                }
                            }

                            if let Some((packets_sender, open_connection)) = ongoing_connections.remove(&remote_addr) {
                                if packets_sender.send_async(packet_data).await.inspect_err(|err| {
                                    tracing::warn!(
                                        peer_addr = %remote_addr,
                                        error = %err,
                                        "Failed to send packet to remote"
                                    );
                                }).is_ok() {
                                    ongoing_connections.insert(remote_addr, (packets_sender, open_connection));
                                }
                                continue;
                            }

                            if self.is_gateway {
                                // Handle gateway-intro packets (peer -> gateway)
                                // Note: We only enter this path when this node IS a gateway.
                                // Previously, `is_known_gateway` was also checked here, but that
                                // caused NAT peers to misroute gateway ACKs to this handler instead
                                // of the traverse_nat handler, breaking peer->gateway connections.

                                // Check if we already have a gateway connection in progress.
                                // Note: This guard prevents CREATING duplicate connections. Packets for
                                // EXISTING ongoing connections are routed above at line 504, where we
                                // filter duplicate RSA intro packets (issue #2292).
                                if ongoing_gw_connections.contains_key(&remote_addr) {
                                    tracing::debug!(
                                        peer_addr = %remote_addr,
                                        direction = "inbound",
                                        "Gateway connection already in progress, ignoring duplicate packet"
                                    );
                                    continue;
                                }

                                // Issue #2235: Clean up stale CLOSED connections from the same IP but different port.
                                // When a peer reconnects (e.g., after restart), it may get a new ephemeral port.
                                // The old connection's crypto state is stale and will cause AEAD decryption
                                // failures if we try to reuse it.
                                //
                                // IMPORTANT: We only remove connections where the channel is closed, to avoid
                                // breaking scenarios where multiple legitimate peers are behind the same NAT
                                // (sharing the same public IP but using different source ports).
                                let remote_ip = remote_addr.ip();
                                let stale_addrs: Vec<_> = self.remote_connections
                                    .iter()
                                    .filter(|(addr, conn)| {
                                        addr.ip() == remote_ip
                                            && **addr != remote_addr
                                            && conn.inbound_packet_sender.is_closed()
                                    })
                                    .map(|(addr, _)| *addr)
                                    .collect();
                                for stale_addr in stale_addrs {
                                    self.remote_connections.remove(&stale_addr);
                                    // Clean up rate-limit tracking for the stale connection
                                    self.last_rsa_attempt.remove(&stale_addr);
                                    tracing::debug!(
                                        stale_peer_addr = %stale_addr,
                                        new_peer_addr = %remote_addr,
                                        "Removed stale closed connection from same IP"
                                    );
                                }

                                let inbound_key_bytes = key_from_addr(&remote_addr);
                                let (gw_ongoing_connection, packets_sender) = self.gateway_connection(packet_data, remote_addr, inbound_key_bytes);
                                let task = tokio::spawn(gw_ongoing_connection
                                    .instrument(tracing::span!(tracing::Level::DEBUG, "gateway_connection"))
                                    .map_err(move |error| {
                                        tracing::warn!(
                                            peer_addr = %remote_addr,
                                            error = %error,
                                            direction = "inbound",
                                            "Gateway connection error"
                                        );
                                        (error, remote_addr)
                                    }));
                                ongoing_gw_connections.insert(remote_addr, packets_sender);
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
                gw_connection_handshake = gw_connection_tasks.next(), if !gw_connection_tasks.is_empty() => {
                    let Some(res): GwOngoingConnectionResult<S> = gw_connection_handshake else {
                        unreachable!("gw_connection_tasks.next() should only return None if empty, which is guarded");
                    };
                    match res.expect("task shouldn't panic") {
                        Ok((outbound_remote_conn, inbound_remote_connection, outbound_ack_packet)) => {
                            let remote_addr = outbound_remote_conn.remote_addr;
                            ongoing_gw_connections.remove(&remote_addr);
                            let sent_tracker = outbound_remote_conn.sent_tracker.clone();

                            self.remote_connections.insert(remote_addr, inbound_remote_connection);

                            if self.new_connection_notifier
                                .send(PeerConnection::new(outbound_remote_conn))
                                .await
                                .is_err() {
                                tracing::error!(
                                    peer_addr = %remote_addr,
                                    direction = "inbound",
                                    "Gateway connection established but failed to notify new connection"
                                );
                                break 'outer Err(TransportError::ConnectionClosed(self.this_addr));
                            }

                            sent_tracker.lock().report_sent_packet(
                                SymmetricMessage::FIRST_PACKET_ID,
                                outbound_ack_packet.prepared_send(),
                            );
                        }
                        Err((error, remote_addr)) => {
                            tracing::error!(
                                error = %error,
                                peer_addr = %remote_addr,
                                direction = "inbound",
                                "Failed to establish gateway connection"
                            );
                            // Only block peers with incompatible protocol versions, not other errors.
                            // Issue #2292: Previously used fragile string matching on error messages.
                            // Now we use proper typed error matching.
                            if matches!(error, TransportError::ProtocolVersionMismatch { .. }) {
                                outdated_peer.insert(remote_addr, Instant::now());
                            }
                            ongoing_gw_connections.remove(&remote_addr);
                            ongoing_connections.remove(&remote_addr);
                        }
                    }
                }
                connection_handshake = connection_tasks.next(), if !connection_tasks.is_empty() => {
                    let Some(res): OngoingConnectionResult<S> = connection_handshake else {
                        unreachable!("connection_tasks.next() should only return None if empty, which is guarded");
                    };
                    match res.expect("task shouldn't panic") {
                        Ok((outbound_remote_conn, inbound_remote_connection)) => {
                            if let Some((_, result_sender)) = ongoing_connections.remove(&outbound_remote_conn.remote_addr) {
                                if self
                                    .expected_non_gateway
                                    .remove(&outbound_remote_conn.remote_addr.ip())
                                    .is_some()
                                {
                                    tracing::debug!(
                                        peer_addr = %outbound_remote_conn.remote_addr,
                                        direction = "outbound",
                                        "Cleared expected handshake flag after successful connection"
                                    );
                                }
                                tracing::info!(
                                    peer_addr = %outbound_remote_conn.remote_addr,
                                    direction = "outbound",
                                    "Connection established"
                                );
                                self.remote_connections.insert(outbound_remote_conn.remote_addr, inbound_remote_connection);
                                let _ = result_sender.send(Ok(outbound_remote_conn)).map_err(|_| {
                                    tracing::error!(
                                        "Failed sending back peer connection"
                                    );
                                });
                            } else {
                                tracing::error!(
                                    peer_addr = %outbound_remote_conn.remote_addr,
                                    direction = "outbound",
                                    "Connection established but no ongoing connection found"
                                );
                            }
                        }
                        Err((error, remote_addr)) => {
                            // Only log non-version-mismatch errors at error level
                            // Version mismatches have their own user-friendly error message
                            match &error {
                                TransportError::ProtocolVersionMismatch { .. } => {
                                    tracing::warn!(
                                        peer_addr = %remote_addr,
                                        error = %error,
                                        direction = "outbound",
                                        "Connection failed due to version mismatch"
                                    );
                                }
                                _ => {
                                    tracing::error!(
                                        error = %error,
                                        peer_addr = %remote_addr,
                                        direction = "outbound",
                                        "Failed to establish connection"
                                    );
                                }
                            }
                            if let Some((_, result_sender)) = ongoing_connections.remove(&remote_addr) {
                                if self
                                    .expected_non_gateway
                                    .remove(&remote_addr.ip())
                                    .is_some()
                                {
                                    tracing::debug!(
                                        peer_addr = %remote_addr,
                                        direction = "outbound",
                                        "Cleared expected handshake flag after failed connection"
                                    );
                                }
                                let _ = result_sender.send(Err(error));
                            }
                        }
                    }
                }
                // Handling of connection events
                connection_event = self.connection_handler.recv() => {
                    let Some((remote_addr, event)) = connection_event else {
                        tracing::debug!(
                            bind_addr = %self.this_addr,
                            "Connection handler closed"
                        );
                        return Ok(());
                    };
                    tracing::debug!(
                        peer_addr = %remote_addr,
                        "Received connection event"
                    );
                    let ConnectionEvent::ConnectionStart { remote_public_key, open_connection } = event;

                    // Check if we already have an active connection
                    if let Some(existing_conn) = self.remote_connections.get(&remote_addr) {
                        // Check if the existing connection is still alive
                        if existing_conn.inbound_packet_sender.is_closed() {
                            // Connection is dead, remove it
                            self.remote_connections.remove(&remote_addr);
                            // Clean up rate-limit tracking
                            self.last_rsa_attempt.remove(&remote_addr);
                            tracing::warn!(
                                peer_addr = %remote_addr,
                                direction = "outbound",
                                "Removing closed connection"
                            );
                        } else {
                            // Connection is still alive, reject new connection attempt
                            tracing::debug!(
                                peer_addr = %remote_addr,
                                direction = "outbound",
                                "Connection already established and healthy, rejecting new attempt"
                            );
                            let _ = open_connection.send(Err(TransportError::ConnectionEstablishmentFailure {
                                cause: "connection already exists".into(),
                            }));
                            continue;
                        }
                    }

                    // Also check if a connection attempt is already in progress
                    if ongoing_connections.contains_key(&remote_addr) {
                        // Duplicate connection attempt - just reject this one
                        // The first attempt is still in progress and will complete
                        tracing::debug!(
                            peer_addr = %remote_addr,
                            direction = "outbound",
                            "Connection attempt already in progress, rejecting duplicate"
                        );
                        let _ = open_connection.send(Err(TransportError::ConnectionEstablishmentFailure {
                            cause: "connection attempt already in progress".into(),
                        }));
                        continue;
                    }
                    tracing::info!(
                        peer_addr = %remote_addr,
                        direction = "outbound",
                        "Attempting to establish connection"
                    );
                    let (ongoing_connection, packets_sender) = self.traverse_nat(
                        remote_addr,
                        remote_public_key.clone(),
                    );
                    self.expected_non_gateway.insert(remote_addr.ip());
                    let task = tokio::spawn(ongoing_connection
                        .map_err(move |err| (err, remote_addr))
                        .instrument(span!(tracing::Level::DEBUG, "traverse_nat"))
                    );
                    connection_tasks.push(task);
                    ongoing_connections.insert(remote_addr, (packets_sender, open_connection));
                },
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
        GatewayConnectionFuture<S>,
        FastSender<PacketData<UnknownEncryption>>,
    ) {
        let secret = self.this_peer_keypair.secret.clone();
        let bandwidth_limit = self.bandwidth_limit;
        let global_bandwidth = self.global_bandwidth.clone();
        let socket = self.socket_listener.clone();

        let (inbound_from_remote, next_inbound) =
            fast_channel::bounded::<PacketData<UnknownEncryption>>(100);
        let f = async move {
            let decrypted_intro_packet =
                secret.decrypt(remote_intro_packet.data()).map_err(|err| {
                    tracing::debug!(
                        peer_addr = %remote_addr,
                        error = %err,
                        direction = "inbound",
                        "Failed to decrypt intro packet"
                    );
                    err
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
            let timeout = tokio::time::timeout(Duration::from_secs(5), next_inbound.recv_async());
            match timeout.await {
                Ok(Ok(packet)) => {
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
                Ok(Err(_)) => {
                    return Err(TransportError::ConnectionEstablishmentFailure {
                        cause: "connection closed".into(),
                    });
                }
                Err(_) => {
                    return Err(TransportError::ConnectionEstablishmentFailure {
                        cause: "connection timed out waiting for response".into(),
                    });
                }
            }

            let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));

            // Initialize LEDBAT congestion controller with slow start (RFC 6817)
            // Uses IW26 (26 * MSS = 38,000 bytes) for fast ramp-up
            let ledbat = Arc::new(LedbatController::new_with_config(
                crate::transport::ledbat::LedbatConfig::default(),
            ));

            // Initialize token bucket for smooth packet pacing
            // Use global bandwidth manager if configured, otherwise fall back to per-connection limit
            let initial_rate = if let Some(ref global) = global_bandwidth {
                global.register_connection()
            } else {
                bandwidth_limit.unwrap_or(10_000_000) // 10 MB/s default
            };
            let token_bucket = Arc::new(TokenBucket::new(
                10_000, // capacity = 10 KB burst
                initial_rate,
            ));

            let (inbound_packet_tx, inbound_packet_rx) = fast_channel::bounded(1000);
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
                ledbat,
                token_bucket,
                socket,
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
        TraverseNatFuture<S>,
        FastSender<PacketData<UnknownEncryption>>,
    ) {
        tracing::debug!(
            peer_addr = %remote_addr,
            direction = "outbound",
            "Starting NAT traversal"
        );
        #[allow(clippy::large_enum_variant)]
        enum ConnectionState {
            /// Initial state of the joiner
            StartOutbound,
            /// Initial state of the joinee, at this point NAT has been already traversed
            RemoteInbound {
                /// Encrypted intro packet for comparison
                intro_packet: PacketData<AssymetricRSA>,
            },
        }

        fn decrypt_asym(
            remote_addr: SocketAddr,
            packet: &PacketData<UnknownEncryption>,
            transport_secret_key: &TransportSecretKey,
            outbound_sym_key: &mut Option<Aes128Gcm>,
            state: &mut ConnectionState,
        ) -> Result<(), ()> {
            // probably the first packet to punch through the NAT
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
                *state = ConnectionState::RemoteInbound {
                    intro_packet: packet.assert_assymetric(),
                };
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
        let socket = self.socket_listener.clone();
        let (inbound_from_remote, next_inbound) =
            fast_channel::bounded::<PacketData<UnknownEncryption>>(100);
        let this_addr = self.this_addr;
        let f = async move {
            tracing::info!(
                peer_addr = %remote_addr,
                direction = "outbound",
                "Starting outbound handshake (NAT traversal)"
            );
            let mut state = ConnectionState::StartOutbound {};
            let mut attempts = 0usize;
            let start_time = Instant::now();
            let overall_deadline = Duration::from_secs(3);
            let mut resend_tick = tokio::time::interval(INITIAL_INTERVAL);
            resend_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            let inbound_sym_key_bytes = rand::random::<[u8; 16]>();
            let inbound_sym_key = Aes128Gcm::new(&inbound_sym_key_bytes.into());

            let mut outbound_sym_key: Option<Aes128Gcm> = None;
            let outbound_intro_packet = {
                let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
                data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
                data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
                PacketData::<_, MAX_PACKET_SIZE>::encrypt_with_pubkey(&data, &remote_public_key)
            };

            let mut sent_tracker = SentPacketTracker::new();

            // NAT traversal: keep sending packets throughout the deadline to maintain NAT bindings.
            // The acceptor may start hole-punching well before the joiner, so we must keep
            // sending until the deadline expires, not just until we've sent MAX_ATTEMPTS packets.
            while start_time.elapsed() < overall_deadline {
                // Send a packet if we haven't exceeded the max attempts
                if attempts < NAT_TRAVERSAL_MAX_ATTEMPTS {
                    match state {
                        ConnectionState::StartOutbound => {
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
                        ConnectionState::RemoteInbound { .. } => {
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
                let next_inbound_result =
                    tokio::time::timeout(Duration::from_millis(200), next_inbound.recv_async());
                match next_inbound_result.await {
                    Ok(Ok(packet)) => {
                        tracing::trace!(
                            peer_addr = %remote_addr,
                            direction = "outbound",
                            "Received packet after sending it"
                        );
                        match state {
                            ConnectionState::StartOutbound => {
                                // at this point it's either the remote sending us an intro packet or a symmetric packet
                                // cause is the first packet that passes through the NAT
                                tracing::trace!(
                                    peer_addr = %remote_addr,
                                    direction = "outbound",
                                    packet_len = packet.data().len(),
                                    "Received packet from remote"
                                );
                                if let Ok(decrypted_packet) =
                                    packet.try_decrypt_sym(&inbound_sym_key)
                                {
                                    // the remote got our inbound key, so we know that they are at least at the RemoteInbound state
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

                                            // Initialize LEDBAT congestion controller with slow start (RFC 6817)
                                            // Uses IW26 (26 * MSS = 38,000 bytes) for fast ramp-up
                                            let ledbat =
                                                Arc::new(LedbatController::new_with_config(
                                                    crate::transport::ledbat::LedbatConfig::default(
                                                    ),
                                                ));

                                            // Initialize token bucket
                                            // Use global bandwidth manager if configured
                                            let initial_rate =
                                                if let Some(ref global) = global_bandwidth {
                                                    global.register_connection()
                                                } else {
                                                    bandwidth_limit.unwrap_or(10_000_000)
                                                };
                                            let token_bucket = Arc::new(TokenBucket::new(
                                                10_000, // capacity = 10 KB burst
                                                initial_rate,
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
                                                    ledbat,
                                                    token_bucket,
                                                    socket: socket.clone(),
                                                    global_bandwidth: global_bandwidth.clone(),
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
                            ConnectionState::RemoteInbound {
                                // this is the packet encrypted with out RSA pub key
                                ref intro_packet,
                                ..
                            } => {
                                // next packet should be an acknowledgement packet, but might also be a repeated
                                // intro packet so we need to handle that
                                if packet.is_intro_packet(intro_packet) {
                                    tracing::trace!(
                                        peer_addr = %remote_addr,
                                        direction = "outbound",
                                        "Received intro packet"
                                    );
                                    continue;
                                }
                                // if is not an intro packet, the connection is successful and we can proceed
                                let (inbound_sender, inbound_recv) = fast_channel::bounded(1000);

                                // Initialize LEDBAT congestion controller with slow start (RFC 6817)
                                // Uses IW26 (26 * MSS = 38,000 bytes) for fast ramp-up
                                let ledbat = Arc::new(LedbatController::new_with_config(
                                    crate::transport::ledbat::LedbatConfig::default(),
                                ));

                                // Initialize token bucket
                                // Use global bandwidth manager if configured
                                let initial_rate = if let Some(ref global) = global_bandwidth {
                                    global.register_connection()
                                } else {
                                    bandwidth_limit.unwrap_or(10_000_000)
                                };
                                let token_bucket = Arc::new(TokenBucket::new(
                                    10_000, // capacity = 10 KB burst
                                    initial_rate,
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
                                            SentPacketTracker::new(),
                                        )),
                                        last_packet_id: Arc::new(AtomicU32::new(0)),
                                        inbound_packet_recv: inbound_recv,
                                        inbound_symmetric_key: inbound_sym_key,
                                        inbound_symmetric_key_bytes: inbound_sym_key_bytes,
                                        my_address: None,
                                        transport_secret_key: transport_secret_key.clone(),
                                        ledbat,
                                        token_bucket,
                                        socket: socket.clone(),
                                        global_bandwidth: global_bandwidth.clone(),
                                    },
                                    InboundRemoteConnection {
                                        inbound_packet_sender: inbound_sender,
                                    },
                                ));
                            }
                        }
                    }
                    Ok(Err(_)) => {
                        tracing::debug!(
                            bind_addr = %this_addr,
                            peer_addr = %remote_addr,
                            direction = "outbound",
                            "Connection closed"
                        );
                        return Err(TransportError::ConnectionClosed(remote_addr));
                    }
                    Err(_) => {
                        tracing::trace!(
                            bind_addr = %this_addr,
                            peer_addr = %remote_addr,
                            direction = "outbound",
                            "Failed to receive UDP response in time, retrying"
                        );
                    }
                }

                resend_tick.tick().await;
            }

            tracing::warn!(
                peer_addr = %remote_addr,
                attempts,
                elapsed_ms = start_time.elapsed().as_millis(),
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

pub(crate) enum ConnectionEvent<S = UdpSocket> {
    ConnectionStart {
        remote_public_key: TransportPublicKey,
        open_connection: oneshot::Sender<Result<RemoteConnection<S>, TransportError>>,
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
    pub type Channels = Arc<DashMap<SocketAddr, mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>>>;

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
        inbound: Mutex<mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>>,
        this: SocketAddr,
        packet_drop_policy: PacketDropPolicy,
        #[allow(dead_code)] // Used in send_to method
        packet_delay_policy: PacketDelayPolicy,
        num_packets_sent: AtomicUsize,
        rng: std::sync::Mutex<rand::rngs::SmallRng>,
        channels: Channels,
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
            }
        }
    }

    impl Socket for MockSocket {
        async fn bind(_addr: SocketAddr) -> Result<Self, std::io::Error> {
            unimplemented!()
        }

        async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            // tracing::trace!(this = %self.this, "waiting for packet");
            let Some((remote, packet)) = self.inbound.try_lock().unwrap().recv().await else {
                tracing::error!(this = %self.this, "connection closed");
                return Err(std::io::ErrorKind::ConnectionAborted.into());
            };
            // tracing::trace!(?remote, this = %self.this, "receiving packet from remote");
            buf[..packet.len()].copy_from_slice(&packet[..]);
            Ok((packet.len(), remote))
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            // Inject artificial delay to simulate network latency
            match &self.packet_delay_policy {
                PacketDelayPolicy::NoDelay => {}
                PacketDelayPolicy::Fixed(delay) => {
                    tokio::time::sleep(*delay).await;
                }
                PacketDelayPolicy::Uniform { min, max } => {
                    let range = max.as_nanos().saturating_sub(min.as_nanos());
                    let delay = if range == 0 {
                        *min // If min == max, use min (avoids division by zero)
                    } else {
                        let random_nanos = {
                            let mut rng = self.rng.try_lock().unwrap();
                            rng.random::<u128>() % range
                        };
                        *min + Duration::from_nanos((random_nanos) as u64)
                    };
                    tokio::time::sleep(delay).await;
                }
            }
            self.send_to_blocking(buf, target)
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
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
            // tracing::trace!(?target, ?self.this, "sending packet to remote");
            sender
                .send((self.this, buf.to_vec()))
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            // tracing::trace!(?target, ?self.this, "packet sent to remote");
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
            let peer = tokio::spawn(async move {
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
                        let start = std::time::Instant::now();
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

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
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

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
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

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            let _ = tokio::time::timeout(Duration::from_secs(3), conn.recv()).await;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
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

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(2), peer_a_conn).await??;
            conn.send("some data").await.inspect_err(|error| {
                tracing::error!(%error, "error while sending message to peer a");
            })?;
            tracing::info!("Dropping peer b");
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
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
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(PacketDropPolicy::Ranges(vec![0..1, 3..9]), channels).await?;

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(2), peer_a_conn)
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
            let _ = tokio::time::timeout(Duration::from_secs(3), conn.recv()).await;
            Ok::<_, anyhow::Error>(conn)
        });

        let peer_a = tokio::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(2), peer_b_conn)
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

        let gw = tokio::spawn(async move {
            let gw_conn = gw_conn.recv();
            let _ = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
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

        let gw = tokio::spawn(async move {
            let gw_conn = gw_conn.recv();
            let _ = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
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

        let gw = tokio::spawn(async move {
            let gw_conn = gw_conn.recv();
            let _ = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
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

        let gw = tokio::spawn(async move {
            let gw_conn = gw_conn.recv();
            let _ = tokio::time::timeout(Duration::from_secs(10), gw_conn)
                .await?
                .ok_or(anyhow::anyhow!("no connection"))?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
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

    /// This one is the maximum size (1324 currently) of a short message from user side
    /// by using public send API can be directly sent
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simulate_send_max_short_message() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(Default::default(), channels).await?;

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(5), peer_a_conn).await??;
            let data = vec![0u8; 1324];
            let data = tokio::task::spawn_blocking(move || bincode::serialize(&data).unwrap())
                .await
                .unwrap();
            conn.send(data).await?;
            Ok::<_, anyhow::Error>(())
        });

        let peer_a = tokio::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(5), peer_b_conn).await??;
            let msg = tokio::time::timeout(Duration::from_secs(10), conn.recv()).await??;
            assert!(msg.len() <= MAX_DATA_SIZE);
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test]
    async fn simulate_send_max_short_message_plus_1() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            create_mock_peer(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            create_mock_peer(Default::default(), channels).await?;

        let peer_a = tokio::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr).await;
            let mut conn = tokio::time::timeout(Duration::from_secs(1), peer_b_conn).await??;
            let _ = tokio::time::timeout(Duration::from_secs(1), conn.recv()).await??;
            Ok::<_, anyhow::Error>(())
        });

        let peer_b = tokio::spawn(async move {
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

                let now = std::time::Instant::now();
                tests.push_back(tokio::spawn(
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
        let gw_task = tokio::spawn(async move {
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

        let gw_task = tokio::spawn(async move {
            let conn = tokio::time::timeout(Duration::from_secs(5), gw_conn.recv())
                .await?
                .ok_or(anyhow::anyhow!("gateway: no inbound connection for peer B"))?;
            tracing::info!(
                "Gateway received connection from {} (peer B)",
                conn.remote_addr()
            );
            Ok::<_, anyhow::Error>(conn)
        });

        let start = std::time::Instant::now();
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
        let gw_task_a = tokio::spawn(async move {
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
        let gw_task_b = tokio::spawn(async move {
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
        let gw_task = tokio::spawn(async move {
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

        let gw_task = tokio::spawn(async move {
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

        let start = std::time::Instant::now();
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
