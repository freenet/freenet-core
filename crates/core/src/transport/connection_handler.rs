use std::borrow::Cow;
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use crate::transport::crypto::TransportSecretKey;
use crate::transport::packet_data::{AssymetricRSA, UnknownEncryption};
use crate::transport::symmetric_message::OutboundConnection;
use aes_gcm::{Aes128Gcm, KeyInit};
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
    packet_data::{PacketData, SymmetricAES, MAX_PACKET_SIZE},
    peer_connection::{PeerConnection, RemoteConnection},
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
    Socket, TransportError,
};

// Constants for interval increase
const INITIAL_INTERVAL: Duration = Duration::from_millis(200);
const INTERVAL_INCREASE_FACTOR: u64 = 2;
const MAX_INTERVAL: Duration = Duration::from_millis(5000); // Maximum interval limit

const DEFAULT_BW_TRACKER_WINDOW_SIZE: Duration = Duration::from_secs(10);
const BANDWITH_LIMIT: usize = 1024 * 1024 * 10; // 10 MB/s

pub type SerializedMessage = Vec<u8>;

type GatewayConnectionFuture = BoxFuture<
    'static,
    Result<
        (
            RemoteConnection,
            InboundRemoteConnection,
            PacketData<SymmetricAES>,
        ),
        TransportError,
    >,
>;

type TraverseNatFuture =
    BoxFuture<'static, Result<(RemoteConnection, InboundRemoteConnection), TransportError>>;

pub(crate) async fn create_connection_handler<S: Socket>(
    keypair: TransportKeypair,
    listen_host: IpAddr,
    listen_port: u16,
    is_gateway: bool,
) -> Result<(OutboundConnectionHandler, InboundConnectionHandler), TransportError> {
    // Bind the UDP socket to the specified port
    let socket = S::bind((listen_host, listen_port).into()).await?;
    let (och, new_connection_notifier) = OutboundConnectionHandler::config_listener(
        Arc::new(socket),
        keypair,
        is_gateway,
        (listen_host, listen_port).into(),
    )?;
    Ok((
        och,
        InboundConnectionHandler {
            new_connection_notifier,
        },
    ))
}

/// Receives  new inbound connections from the network.
pub(crate) struct InboundConnectionHandler {
    new_connection_notifier: mpsc::Receiver<PeerConnection>,
}

#[cfg(test)]
impl InboundConnectionHandler {
    pub fn new(new_connection_notifier: mpsc::Receiver<PeerConnection>) -> Self {
        InboundConnectionHandler {
            new_connection_notifier,
        }
    }
}

impl InboundConnectionHandler {
    pub async fn next_connection(&mut self) -> Option<PeerConnection> {
        self.new_connection_notifier.recv().await
    }
}

/// Requests a new outbound connection to a remote peer.
#[derive(Clone)]
pub(crate) struct OutboundConnectionHandler {
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent)>,
}

#[cfg(test)]
impl OutboundConnectionHandler {
    pub fn new(send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent)>) -> Self {
        OutboundConnectionHandler { send_queue }
    }
}

impl OutboundConnectionHandler {
    fn config_listener(
        socket: Arc<impl Socket>,
        keypair: TransportKeypair,
        is_gateway: bool,
        socket_addr: SocketAddr,
    ) -> Result<(Self, mpsc::Receiver<PeerConnection>), TransportError> {
        // Channel buffer is one so senders will await until the receiver is ready, important for bandwidth limiting
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(100);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(100);

        // Channel buffer is one so senders will await until the receiver is ready, important for bandwidth limiting
        let (outbound_sender, outbound_recv) = mpsc::channel(1);
        let transport = UdpPacketsListener {
            is_gateway,
            socket_listener: socket.clone(),
            this_peer_keypair: keypair,
            remote_connections: BTreeMap::new(),
            connection_handler: conn_handler_receiver,
            new_connection_notifier: new_connection_sender,
            outbound_packets: outbound_sender,
            this_addr: socket_addr,
        };
        let bw_tracker = super::rate_limiter::PacketRateLimiter::new(
            DEFAULT_BW_TRACKER_WINDOW_SIZE,
            outbound_recv,
        );
        let connection_handler = OutboundConnectionHandler {
            send_queue: conn_handler_sender,
        };

        task::spawn(bw_tracker.rate_limiter(BANDWITH_LIMIT, socket));
        task::spawn(RANDOM_U64.scope(StdRng::from_entropy().gen(), transport.listen()));

        Ok((connection_handler, new_connection_notifier))
    }

    #[cfg(test)]
    fn new_test(
        socket_addr: SocketAddr,
        socket: Arc<impl Socket>,
        keypair: TransportKeypair,
        is_gateway: bool,
    ) -> Result<(Self, mpsc::Receiver<PeerConnection>), TransportError> {
        Self::config_listener(socket, keypair, is_gateway, socket_addr)
    }

    pub async fn connect(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Result<PeerConnection, TransportError>> + Send>> {
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
}

/// Handles UDP transport internally.
struct UdpPacketsListener<S = UdpSocket> {
    socket_listener: Arc<S>,
    remote_connections: BTreeMap<SocketAddr, InboundRemoteConnection>,
    connection_handler: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
    this_peer_keypair: TransportKeypair,
    is_gateway: bool,
    new_connection_notifier: mpsc::Sender<PeerConnection>,
    outbound_packets: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    this_addr: SocketAddr,
}

type OngoingConnection = (
    mpsc::Sender<PacketData<UnknownEncryption>>,
    oneshot::Sender<Result<RemoteConnection, TransportError>>,
);

type OngoingConnectionResult = Option<
    Result<
        Result<(RemoteConnection, InboundRemoteConnection), (TransportError, SocketAddr)>,
        tokio::task::JoinError,
    >,
>;

type GwOngoingConnectionResult = Option<
    Result<
        Result<
            (
                RemoteConnection,
                InboundRemoteConnection,
                PacketData<SymmetricAES>,
            ),
            (TransportError, SocketAddr),
        >,
        tokio::task::JoinError,
    >,
>;

#[cfg(test)]
impl<T> Drop for UdpPacketsListener<T> {
    fn drop(&mut self) {
        tracing::info!(%self.this_addr, "Dropping UdpPacketsListener");
    }
}
task_local! {
    static RANDOM_U64: [u8; 8];
}

#[cfg(not(test))]
pub(super) const NAT_TRAVERSAL_MAX_ATTEMPTS: usize = 40;
#[cfg(test)]
pub(super) const NAT_TRAVERSAL_MAX_ATTEMPTS: usize = 10;

impl<S: Socket> UdpPacketsListener<S> {
    #[tracing::instrument(level = "debug", name = "transport_listener", fields(peer = %self.this_peer_keypair.public), skip_all)]
    async fn listen(mut self) -> Result<(), TransportError> {
        tracing::debug!(%self.this_addr, "listening for packets");
        let mut buf = [0u8; MAX_PACKET_SIZE];
        let mut ongoing_connections: BTreeMap<SocketAddr, OngoingConnection> = BTreeMap::new();
        let mut ongoing_gw_connections: BTreeMap<
            SocketAddr,
            mpsc::Sender<PacketData<UnknownEncryption>>,
        > = BTreeMap::new();
        let mut connection_tasks = FuturesUnordered::new();
        let mut gw_connection_tasks = FuturesUnordered::new();
        let mut pending_connections = vec![];

        'outer: loop {
            'inner: loop {
                if pending_connections.len() > 10 {
                    tracing::error!(%self.this_addr, "too many pending connections");
                    break 'outer Err(TransportError::ConnectionClosed(self.this_addr));
                }
                if let Some(pending) = pending_connections.pop() {
                    match self.new_connection_notifier.try_send(pending) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(pending)) => {
                            tracing::error!(%self.this_addr, "channel is full");
                            pending_connections.push(pending);
                            break 'inner;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            tracing::error!(%self.this_addr, "failed to notify new connection");
                            break 'outer Err(TransportError::ConnectionClosed(self.this_addr));
                        }
                    }
                } else {
                    break 'inner;
                }
            }
            tokio::select! {
                // Handling of inbound packets
                recv_result = self.socket_listener.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, remote_addr)) => {
                            let packet_data = PacketData::from_buf(&buf[..size]);
                            tracing::debug!(%remote_addr, %size, "received packet from remote");
                            if let Some(remote_conn) = self.remote_connections.remove(&remote_addr){
                                if remote_conn.inbound_packet_sender.send(packet_data)
                                    .await
                                    .inspect_err(|err| {
                                        tracing::debug!(%remote_addr, %err, "failed to receive packet from remote");
                                    })
                                    .is_ok()
                                {
                                    self.remote_connections.insert(remote_addr, remote_conn);
                                }
                                continue;
                            }

                            if let Some(inbound_packet_sender) = ongoing_gw_connections.remove(&remote_addr){
                                if inbound_packet_sender.send(packet_data).await.inspect_err(|err| {
                                    tracing::debug!(%remote_addr, %err, "failed to receive packet from remote");
                                }).is_ok() {
                                    ongoing_gw_connections.insert(remote_addr, inbound_packet_sender);
                                }
                                continue;
                            }

                            if let Some((packets_sender, open_connection)) = ongoing_connections.remove(&remote_addr) {
                                if packets_sender.send(packet_data).await.is_err() {
                                    // it can happen that the connection is established but the channel is closed because the task completed
                                    // but we still haven't polled the result future
                                    tracing::debug!(%remote_addr, "failed to send packet to remote");
                                }
                                ongoing_connections.insert(remote_addr, (packets_sender, open_connection));
                                continue;
                            }

                            if !self.is_gateway {
                                tracing::trace!(%remote_addr, "unexpected packet from remote");
                                continue;
                            }
                            let packet_data = PacketData::from_buf(&buf[..size]);
                            let inbound_key_bytes = key_from_addr(&remote_addr);
                            let (gw_ongoing_connection, packets_sender) = self.gateway_connection(packet_data, remote_addr, inbound_key_bytes);
                            let task = tokio::spawn(gw_ongoing_connection
                                .instrument(tracing::span!(tracing::Level::DEBUG, "gateway_connection"))
                                .map_err(move |error| {
                                (error, remote_addr)
                            }));
                            ongoing_gw_connections.insert(remote_addr, packets_sender);
                            gw_connection_tasks.push(task);
                        }
                        Err(e) => {
                            // TODO: this should panic and be propagate to the main task or retry and eventually fail
                            tracing::error!("Failed to receive UDP packet: {:?}", e);
                            return Err(e.into());
                        }
                    }
                },
                gw_connection_handshake = gw_connection_tasks.next(), if !gw_connection_tasks.is_empty() => {
                    let Some(res): GwOngoingConnectionResult = gw_connection_handshake else {
                        unreachable!();
                    };
                    match res.expect("task shouldn't panic") {
                        Ok((outbound_remote_conn, inbound_remote_connection, outbound_ack_packet)) => {
                            let remote_addr = outbound_remote_conn.remote_addr;
                            ongoing_gw_connections.remove(&remote_addr);
                            let sent_tracker = outbound_remote_conn.sent_tracker.clone();

                            self.remote_connections.insert(remote_addr, inbound_remote_connection);

                            match self.new_connection_notifier
                            .try_send(PeerConnection::new(outbound_remote_conn)) {
                                Ok(_) => {}
                                Err(mpsc::error::TrySendError::Full(pending_conn)) => {
                                    tracing::error!(%remote_addr, "gateway connection established but channel is full");
                                    pending_connections.push(pending_conn);
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    tracing::error!(%remote_addr, "gateway connection established but failed to notify new connection");
                                    break 'outer Err(TransportError::ConnectionClosed(self.this_addr));
                                }
                            }

                            sent_tracker.lock().report_sent_packet(
                                SymmetricMessage::FIRST_PACKET_ID,
                                outbound_ack_packet.prepared_send(),
                            );
                        }
                        Err((error, remote_addr)) => {
                            tracing::error!(%error, ?remote_addr, "Failed to establish gateway connection");
                            ongoing_gw_connections.remove(&remote_addr);
                            ongoing_connections.remove(&remote_addr);
                        }
                    }
                }
                connection_handshake = connection_tasks.next(), if !connection_tasks.is_empty() => {
                    let Some(res): OngoingConnectionResult = connection_handshake else {
                        unreachable!();
                    };
                    match res.expect("task shouldn't panic") {
                        Ok((outbound_remote_conn, inbound_remote_connection)) => {
                            if let Some((_, result_sender)) = ongoing_connections.remove(&outbound_remote_conn.remote_addr) {
                                tracing::debug!(remote_addr = %outbound_remote_conn.remote_addr, "connection established");
                                self.remote_connections.insert(outbound_remote_conn.remote_addr, inbound_remote_connection);
                                let _ = result_sender.send(Ok(outbound_remote_conn)).map_err(|_| {
                                    tracing::error!("failed sending back peer connection");
                                });
                            } else {
                                tracing::error!(remote_addr = %outbound_remote_conn.remote_addr, "connection established but no ongoing connection found");
                            }
                        }
                        Err((error, remote_addr)) => {
                            tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                            if let Some((_, result_sender)) = ongoing_connections.remove(&remote_addr) {
                                let _ = result_sender.send(Err(error));
                            }
                        }
                    }
                }
                // Handling of connection events
                connection_event = self.connection_handler.recv() => {
                    let Some((remote_addr, event)) = connection_event else {
                        tracing::debug!(%self.this_addr, "connection handler closed");
                        return Ok(());
                    };
                    tracing::debug!(%remote_addr, "received connection event");
                    if let Some(_conn) = self.remote_connections.remove(&remote_addr) {
                        tracing::warn!(%remote_addr, "connection already established, dropping old connection");
                    }
                    let ConnectionEvent::ConnectionStart { remote_public_key, open_connection } = event;
                    tracing::debug!(%remote_addr, "attempting to establish connection");
                    let (ongoing_connection, packets_sender) = self.traverse_nat(
                        remote_addr,  remote_public_key,
                    );
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
        GatewayConnectionFuture,
        mpsc::Sender<PacketData<UnknownEncryption>>,
    ) {
        let secret = self.this_peer_keypair.secret.clone();
        let outbound_packets = self.outbound_packets.clone();

        let (inbound_from_remote, mut next_inbound) =
            mpsc::channel::<PacketData<UnknownEncryption>>(1);
        let f = async move {
            let decrypted_intro_packet =
                secret.decrypt(remote_intro_packet.data()).map_err(|err| {
                    tracing::debug!(%remote_addr, %err, "Failed to decrypt intro packet");
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
                outbound_packets
                    .send((remote_addr, packet.prepared_send()))
                    .await
                    .map_err(|_| TransportError::ChannelClosed)?;
                return Err(TransportError::ConnectionEstablishmentFailure {
                    cause: format!(
                        "remote is using a different protocol version: {:?}",
                        String::from_utf8_lossy(protoc)
                    )
                    .into(),
                });
            }

            let inbound_key = Aes128Gcm::new(&inbound_key_bytes.into());
            let outbound_ack_packet =
                SymmetricMessage::ack_ok(&outbound_key, inbound_key_bytes, remote_addr)?;

            tracing::debug!(%remote_addr, "Sending outbound ack packet: {:?}", outbound_ack_packet.data());

            outbound_packets
                .send((remote_addr, outbound_ack_packet.clone().prepared_send()))
                .await
                .map_err(|_| TransportError::ChannelClosed)?;

            // wait until the remote sends the ack packet
            let timeout = tokio::time::timeout(Duration::from_secs(5), next_inbound.recv());
            match timeout.await {
                Ok(Some(packet)) => {
                    let _ = packet.try_decrypt_sym(&inbound_key).map_err(|_| {
                            tracing::debug!(%remote_addr, "Failed to decrypt packet with inbound key: {:?}", packet.data());
                            TransportError::ConnectionEstablishmentFailure {
                                cause: "invalid symmetric key".into(),
                            }
                        })?;
                }
                Ok(None) => {
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

            let (inbound_packet_tx, inbound_packet_rx) = mpsc::channel(100);
            let remote_conn = RemoteConnection {
                outbound_packets,
                outbound_symmetric_key: outbound_key,
                remote_addr,
                sent_tracker: sent_tracker.clone(),
                last_packet_id: Arc::new(AtomicU32::new(0)),
                inbound_packet_recv: inbound_packet_rx,
                inbound_symmetric_key: inbound_key,
                inbound_symmetric_key_bytes: inbound_key_bytes,
                my_address: None,
            };

            let inbound_conn = InboundRemoteConnection {
                inbound_packet_sender: inbound_packet_tx,
            };

            tracing::debug!("returning connection at gw");
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
        TraverseNatFuture,
        mpsc::Sender<PacketData<UnknownEncryption>>,
    ) {
        tracing::debug!(
            addr = ?remote_addr,
            "Starting NAT traversal"
        );
        // Constants for exponential backoff
        const INITIAL_TIMEOUT: Duration = Duration::from_millis(600);
        const TIMEOUT_MULTIPLIER: f64 = 1.2;
        #[cfg(not(test))]
        const MAX_TIMEOUT: Duration = Duration::from_secs(60); // Maximum timeout limit
        #[cfg(test)]
        const MAX_TIMEOUT: Duration = Duration::from_secs(10); // Maximum timeout limit

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
                tracing::debug!(%remote_addr, "received intro packet");
                let protoc = &decrypted_intro_packet.data()[..PROTOC_VERSION.len()];
                if protoc != PROTOC_VERSION {
                    tracing::debug!(%remote_addr, ?protoc, this_protoc = ?PROTOC_VERSION, "remote is using a different protocol version");
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
            tracing::debug!(%remote_addr, "failed to decrypt packet");
            Err(())
        }

        let outbound_packets = self.outbound_packets.clone();
        let transport_secret_key = self.this_peer_keypair.secret.clone();
        let (inbound_from_remote, mut next_inbound) =
            mpsc::channel::<PacketData<UnknownEncryption>>(1);
        let this_addr = self.this_addr;
        let f = async move {
            let mut state = ConnectionState::StartOutbound {};
            // Initialize timeout and interval
            let mut timeout = INITIAL_TIMEOUT;
            let mut interval_duration = INITIAL_INTERVAL;
            let mut tick = tokio::time::interval(interval_duration);

            let mut failures = 0;

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

            while failures < NAT_TRAVERSAL_MAX_ATTEMPTS {
                match state {
                    ConnectionState::StartOutbound { .. } => {
                        tracing::debug!(%remote_addr, "sending protocol version and inbound key");
                        outbound_packets
                            .send((remote_addr, outbound_intro_packet.data().into()))
                            .await
                            .map_err(|_| TransportError::ChannelClosed)?;
                    }
                    ConnectionState::RemoteInbound { .. } => {
                        tracing::debug!(%remote_addr, "sending back protocol version and inbound key to remote");
                        let our_inbound = SymmetricMessage::ack_ok(
                            outbound_sym_key.as_ref().expect("should be set"),
                            inbound_sym_key_bytes,
                            remote_addr,
                        )?;
                        outbound_packets
                            .send((remote_addr, our_inbound.data().into()))
                            .await
                            .map_err(|_| TransportError::ChannelClosed)?;
                        sent_tracker.report_sent_packet(
                            SymmetricMessage::FIRST_PACKET_ID,
                            our_inbound.data().into(),
                        );
                    }
                }
                let next_inbound = tokio::time::timeout(timeout, next_inbound.recv());
                match next_inbound.await {
                    Ok(Some(packet)) => {
                        tracing::debug!(%remote_addr, "received packet after sending it");
                        match state {
                            ConnectionState::StartOutbound { .. } => {
                                // at this point it's either the remote sending us an intro packet or a symmetric packet
                                // cause is the first packet that passes through the NAT
                                tracing::debug!(%remote_addr, "received packet from remote: {:?}", packet.data());
                                if let Ok(decrypted_packet) =
                                    packet.try_decrypt_sym(&inbound_sym_key)
                                {
                                    // the remote got our inbound key, so we know that they are at least at the RemoteInbound state
                                    let symmetric_message =
                                        SymmetricMessage::deser(decrypted_packet.data())?;

                                    #[cfg(test)]
                                    {
                                        tracing::debug!(%remote_addr, ?symmetric_message.payload, "received symmetric packet");
                                    }
                                    #[cfg(not(test))]
                                    {
                                        tracing::trace!(%remote_addr, "received symmetric packet");
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
                                            tracing::debug!(%remote_addr, "Sending back ack connection: {:?}", key);
                                            outbound_packets
                                                .send((
                                                    remote_addr,
                                                    SymmetricMessage::ack_ok(
                                                        &outbound_sym_key,
                                                        inbound_sym_key_bytes,
                                                        remote_addr,
                                                    )?
                                                    .data()
                                                    .into(),
                                                ))
                                                .await
                                                .map_err(|_| TransportError::ChannelClosed)?;
                                            let (inbound_sender, inbound_recv) = mpsc::channel(100);
                                            tracing::debug!(%remote_addr, "connection established");
                                            return Ok((
                                                RemoteConnection {
                                                    outbound_packets: outbound_packets.clone(),
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
                                            tracing::debug!(%remote_addr, "unexpected packet from remote");
                                            failures += 1;
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

                                failures += 1;
                                tracing::debug!("Failed to decrypt packet");
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
                                    tracing::debug!(%remote_addr, "received intro packet");
                                    // we add to the number of failures so we are not stuck in a loop retrying
                                    failures += 1;
                                    continue;
                                }
                                // if is not an intro packet, the connection is successful and we can proceed
                                let (inbound_sender, inbound_recv) = mpsc::channel(1);
                                return Ok((
                                    RemoteConnection {
                                        outbound_packets: outbound_packets.clone(),
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
                                    },
                                    InboundRemoteConnection {
                                        inbound_packet_sender: inbound_sender,
                                    },
                                ));
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::debug!(%this_addr, %remote_addr, "debug: connection closed");
                        return Err(TransportError::ConnectionClosed(remote_addr));
                    }
                    Err(_) => {
                        failures += 1;
                        tracing::debug!(%this_addr, %remote_addr, "failed to receive UDP response in time, retrying");
                    }
                }

                // We have retried for a while, so return an error
                if timeout >= MAX_TIMEOUT {
                    tracing::error!(%this_addr, %remote_addr, "failed to establish connection after multiple attempts, max timeout reached");
                    break;
                }

                // Update timeout using exponential backoff, capped at MAX_TIMEOUT
                timeout = std::cmp::min(
                    Duration::from_millis(
                        ((timeout.as_millis()) as f64 * TIMEOUT_MULTIPLIER) as u64,
                    ),
                    MAX_TIMEOUT,
                );

                // Update interval, capped at MAX_INTERVAL
                if interval_duration < MAX_INTERVAL {
                    interval_duration = std::cmp::min(
                        Duration::from_millis(
                            interval_duration.as_millis() as u64 * INTERVAL_INCREASE_FACTOR,
                        ),
                        MAX_INTERVAL,
                    );
                    tick = tokio::time::interval(interval_duration);
                }

                tick.tick().await;
            }

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
            actual: version_cmp::VERSION,
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

pub(crate) enum ConnectionEvent {
    ConnectionStart {
        remote_public_key: TransportPublicKey,
        open_connection: oneshot::Sender<Result<RemoteConnection, TransportError>>,
    },
}

struct InboundRemoteConnection {
    inbound_packet_sender: mpsc::Sender<PacketData<UnknownEncryption>>,
}

mod version_cmp {
    pub(super) const VERSION: &str = env!("CARGO_PKG_VERSION");

    pub(super) const PROTOC_VERSION: [u8; 8] = parse_version_with_flags(VERSION);

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
                format!("{}.{}.{}-{}", major, minor, patch, flags_str)
            } else {
                format!("{}.{}.{}", major, minor, patch)
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
                "Failed for version string '{}', decoded as '{}'",
                version_str, decoded
            );
        }
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::single_range_in_vec_init)]

    use std::{
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
    use crate::transport::packet_data::MAX_DATA_SIZE;

    #[test]
    fn test_handle_ack_connection_error() {
        let err_msg =
            "remote is using a different protocol version, expected version 1.2.3".to_string();
        match handle_ack_connection_error(err_msg.into()) {
            TransportError::ProtocolVersionMismatch { expected, actual } => {
                assert_eq!(expected, "1.2.3");
                assert_eq!(actual, version_cmp::VERSION);
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

    type Channels = Arc<DashMap<SocketAddr, mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>>>;

    #[derive(Default, Clone)]
    enum PacketDropPolicy {
        /// Receive all packets without dropping
        #[default]
        ReceiveAll,
        /// Drop the packets randomly based on the factor
        Factor(f64),
        /// Drop packets with the given ranges
        Ranges(Vec<Range<usize>>),
    }

    struct MockSocket {
        inbound: Mutex<mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>>,
        this: SocketAddr,
        packet_drop_policy: PacketDropPolicy,
        num_packets_sent: AtomicUsize,
        rng: std::sync::Mutex<rand::rngs::SmallRng>,
        channels: Channels,
    }

    impl MockSocket {
        async fn test_config(
            packet_drop_policy: PacketDropPolicy,
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
            let packet_idx = self.num_packets_sent.fetch_add(1, Ordering::Release);
            match &self.packet_drop_policy {
                PacketDropPolicy::ReceiveAll => {}
                PacketDropPolicy::Factor(factor) => {
                    if *factor > self.rng.try_lock().unwrap().gen::<f64>() {
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

    async fn set_peer_connection(
        packet_drop_policy: PacketDropPolicy,
        channels: Channels,
    ) -> anyhow::Result<(TransportPublicKey, OutboundConnectionHandler, SocketAddr)> {
        set_peer_connection_in(packet_drop_policy, false, channels)
            .await
            .map(|(pk, (o, _), s)| (pk, o, s))
    }

    async fn set_gateway_connection(
        packet_drop_policy: PacketDropPolicy,
        channels: Channels,
    ) -> Result<
        (
            TransportPublicKey,
            (OutboundConnectionHandler, mpsc::Receiver<PeerConnection>),
            SocketAddr,
        ),
        anyhow::Error,
    > {
        set_peer_connection_in(packet_drop_policy, true, channels).await
    }

    async fn set_peer_connection_in(
        packet_drop_policy: PacketDropPolicy,
        gateway: bool,
        channels: Channels,
    ) -> Result<
        (
            TransportPublicKey,
            (OutboundConnectionHandler, mpsc::Receiver<PeerConnection>),
            SocketAddr,
        ),
        anyhow::Error,
    > {
        static PORT: AtomicU16 = AtomicU16::new(25000);

        let peer_keypair = TransportKeypair::new();
        let peer_pub = peer_keypair.public.clone();
        let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let socket = Arc::new(
            MockSocket::test_config(
                packet_drop_policy,
                (Ipv4Addr::LOCALHOST, port).into(),
                channels,
            )
            .await,
        );
        let (peer_conn, inbound_conn) = OutboundConnectionHandler::new_test(
            (Ipv4Addr::LOCALHOST, port).into(),
            socket,
            peer_keypair,
            gateway,
        )
        .expect("failed to create peer");
        Ok((
            peer_pub,
            (peer_conn, inbound_conn),
            (Ipv4Addr::LOCALHOST, port).into(),
        ))
    }

    trait TestFixture: Clone + Send + Sync + 'static {
        type Message: DeserializeOwned + Serialize + Send + 'static;
        fn expected_iterations(&self) -> usize;
        fn gen_msg(&mut self) -> Self::Message;
        fn assert_message_ok(&self, peer_idx: usize, msg: Self::Message) -> bool;
    }

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
                wait_time: Duration::from_secs(2),
            }
        }
    }

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
                set_peer_connection(config.packet_drop_policy.clone(), channels.clone()).await?;
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
                        Duration::from_secs(2),
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
            set_peer_connection(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_peer_connection(Default::default(), channels).await?;

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
    async fn simulate_nat_traversal_drop_first_packets_for_all() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            set_peer_connection(PacketDropPolicy::Ranges(vec![0..1]), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_peer_connection(PacketDropPolicy::Ranges(vec![0..1]), channels).await?;

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
            set_peer_connection(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_peer_connection(PacketDropPolicy::Ranges(vec![0..1]), channels).await?;

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
            set_peer_connection(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) = set_peer_connection(
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
            let res = conn.recv().await;
            assert!(res.is_err());
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
            set_peer_connection(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_peer_connection(PacketDropPolicy::Ranges(vec![0..1, 3..9]), channels).await?;

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
            set_peer_connection(Default::default(), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            set_gateway_connection(Default::default(), channels).await?;

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

    #[tokio::test]
    async fn simulate_gateway_connection_drop_first_packets_of_gateway() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            set_peer_connection(Default::default(), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            set_gateway_connection(PacketDropPolicy::Ranges(vec![0..1]), channels.clone()).await?;

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

    #[tokio::test]
    async fn simulate_gateway_connection_drop_first_packets_for_all() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (_peer_a_pub, mut peer_a, _peer_a_addr) =
            set_peer_connection(PacketDropPolicy::Ranges(vec![0..1]), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            set_gateway_connection(PacketDropPolicy::Ranges(vec![0..1]), channels).await?;

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
            set_peer_connection(PacketDropPolicy::Ranges(vec![0..1]), channels.clone()).await?;
        let (gw_pub, (_oc, mut gw_conn), gw_addr) =
            set_gateway_connection(Default::default(), channels).await?;

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
                ..Default::default()
            },
            Vec::from_iter((0..10).map(|_| TestData("foo"))),
        )
        .await
    }

    /// This one is the maximum size (1324 currently) of a short message from user side
    /// by using public send API can be directly sent
    #[tokio::test]
    async fn simulate_send_max_short_message() -> anyhow::Result<()> {
        let channels = Arc::new(DashMap::new());
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            set_peer_connection(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_peer_connection(Default::default(), channels).await?;

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
            let msg = conn.recv().await?;
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
            set_peer_connection(Default::default(), channels.clone()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_peer_connection(Default::default(), channels).await?;

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
            conn.outbound_short_message(data).await?;
            Ok::<_, anyhow::Error>(())
        });

        let (a, b) = tokio::join!(peer_a, peer_b);
        assert!(a?.is_err());
        assert!(b.is_err());
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
                .map(|_| rng.gen::<f64>())
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
}
