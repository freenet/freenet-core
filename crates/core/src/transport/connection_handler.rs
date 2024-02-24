use aes_gcm::{Aes128Gcm, KeyInit};
use futures::FutureExt;
use std::collections::{BTreeMap, LinkedList};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::vec::Vec;
use std::{borrow::Cow, time::Duration};
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::{mpsc, oneshot};
use tokio::task;

use super::{
    crypto::{TransportKeypair, TransportPublicKey},
    packet_data::MAX_PACKET_SIZE,
    peer_connection::{PeerConnection, RemoteConnection},
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
    BytesPerSecond, PacketData,
};

const PROTOC_VERSION: [u8; 2] = 1u16.to_le_bytes();

// Constants for exponential backoff
const INITIAL_TIMEOUT: Duration = Duration::from_secs(5);
const TIMEOUT_MULTIPLIER: u64 = 2;
const MAX_TIMEOUT: Duration = Duration::from_secs(60); // Maximum timeout limit

// Constants for interval increase
const INITIAL_INTERVAL: Duration = Duration::from_millis(200);
const INTERVAL_INCREASE_FACTOR: u64 = 2;
const MAX_INTERVAL: Duration = Duration::from_millis(5000); // Maximum interval limit

const DEFAULT_BW_TRACKER_WINDOW_SIZE: Duration = Duration::from_secs(10);
const BANDWITH_LIMIT: usize = 1024 * 1024 * 10; // 10 MB/s

type ConnectionHandlerMessage = (SocketAddr, Vec<u8>);
pub type SerializedMessage = Vec<u8>;
type PeerChannel = (
    mpsc::Sender<SerializedMessage>,
    mpsc::Receiver<SymmetricMessagePayload>,
);

struct OutboundMessage {
    remote_addr: SocketAddr,
    msg: SerializedMessage,
    recv: mpsc::Receiver<SerializedMessage>,
}

pub(crate) struct ConnectionHandler {
    max_upstream_rate: Arc<arc_swap::ArcSwap<BytesPerSecond>>,
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent)>,
    new_connection_notifier: mpsc::Receiver<PeerConnection>,
    // bw_tracker: Arc<Mutex<bw::PacketBWTracker<InstantTimeSrc>>>,
}

impl ConnectionHandler {
    pub async fn new<S: Socket>(
        keypair: TransportKeypair,
        listen_port: u16,
        is_gateway: bool,
        max_upstream_rate: BytesPerSecond,
    ) -> Result<Self, TransportError> {
        // Bind the UDP socket to the specified port
        let socket = Arc::new(S::bind(("0.0.0.0", listen_port)).await?);

        // Channel buffer is one so senders will await until the receiver is ready, important for bandwidth limiting
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(100);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(100);

        let max_upstream_rate = Arc::new(arc_swap::ArcSwap::from_pointee(max_upstream_rate));
        let (outbound_sender, outbound_recv) = mpsc::channel(1);
        let transport = UdpPacketsListener {
            is_gateway,
            socket_listener: socket.clone(),
            this_peer_keypair: keypair,
            remote_connections: BTreeMap::new(),
            connection_handler: conn_handler_receiver,
            max_upstream_rate: max_upstream_rate.clone(),
            new_connection_notifier: new_connection_sender,
            outbound_packets: outbound_sender,
            pending_inbound_queue: LinkedList::new(),
        };
        let bw_tracker = super::rate_limiter::PacketRateLimiter::new(
            DEFAULT_BW_TRACKER_WINDOW_SIZE,
            outbound_recv,
        );
        let connection_handler = ConnectionHandler {
            max_upstream_rate,
            send_queue: conn_handler_sender,
            new_connection_notifier,
        };

        task::spawn(bw_tracker.rate_limiter(BANDWITH_LIMIT, socket));
        task::spawn(transport.listen());

        Ok(connection_handler)
    }

    pub async fn connect(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
        remote_is_gateway: bool,
    ) -> Result<PeerConnection, TransportError> {
        if !remote_is_gateway {
            let (open_connection, recv_connection) = oneshot::channel();
            self.send_queue
                .send((
                    remote_addr,
                    ConnectionEvent::ConnectionStart {
                        remote_public_key,
                        open_connection,
                    },
                ))
                .await
                .map_err(|_| TransportError::ChannelClosed)?;
            let outbound_conn = recv_connection.await.map_err(|e| anyhow::anyhow!(e))??;
            Ok(PeerConnection::new(outbound_conn))
        } else {
            todo!("establish connection with a gateway")
        }
    }

    pub async fn new_connection(&mut self) -> Option<PeerConnection> {
        self.new_connection_notifier.recv().await
    }

    fn update_max_upstream_rate(&mut self, max_upstream_rate: BytesPerSecond) {
        self.max_upstream_rate.store(Arc::new(max_upstream_rate));
    }
}

pub enum Message {
    Short(Vec<u8>),
    Long(Vec<u8>, mpsc::Receiver<LongMessageFragment>),
}

pub struct LongMessageFragment {
    pub fragment_number: u32,
    pub fragment: Vec<u8>,
}

/// Make connection handler more testable
pub(super) trait Socket: Sized + Send + Sync + 'static {
    fn bind<A: ToSocketAddrs + Send>(addr: A) -> impl Future<Output = io::Result<Self>> + Send;
    fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send;
    fn send_to<A: ToSocketAddrs + Send>(
        &self,
        buf: &[u8],
        target: A,
    ) -> impl Future<Output = io::Result<usize>> + Send;
}

impl Socket for UdpSocket {
    async fn bind<A: ToSocketAddrs + Send>(addr: A) -> io::Result<Self> {
        Self::bind(addr).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }

    async fn send_to<A: ToSocketAddrs + Send>(&self, buf: &[u8], target: A) -> io::Result<usize> {
        self.send_to(buf, target).await
    }
}

/// Handles UDP transport internally.
struct UdpPacketsListener<S = UdpSocket> {
    socket_listener: Arc<S>,
    remote_connections: BTreeMap<SocketAddr, InboundRemoteConnection>,
    connection_handler: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
    this_peer_keypair: TransportKeypair,
    max_upstream_rate: Arc<arc_swap::ArcSwap<BytesPerSecond>>,
    is_gateway: bool,
    new_connection_notifier: mpsc::Sender<PeerConnection>,
    outbound_packets: mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
    /// using linked list cause is unbounded and we are only using it as a queue
    pending_inbound_queue: LinkedList<(SocketAddr, PacketData)>,
}

impl<S: Socket> UdpPacketsListener<S> {
    async fn listen(mut self) -> Result<(), TransportError> {
        loop {
            let mut buf = [0u8; MAX_PACKET_SIZE];
            while let Some((remote, packet)) = self.pending_inbound_queue.pop_front() {
                let remote_conn = self.remote_connections.remove(&remote);
                match remote_conn {
                    Some(remote_conn) => {
                        let _ = remote_conn.inbound_packet_sender.send(packet).await;
                        self.remote_connections.insert(remote, remote_conn);
                    }
                    None => {
                        // TODO: handle in case of gateway
                    }
                }
            }
            tokio::select! {
                // Handling of inbound packets
                recv_result = self.socket_listener.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, remote_addr)) => {
                            let remote_conn = self.remote_connections.remove(&remote_addr);
                            match remote_conn {
                                Some(remote_conn) => {
                                    let packet_data = PacketData::from(&buf[..size]);
                                    let _ = remote_conn.inbound_packet_sender.send(packet_data).await;
                                    self.remote_connections.insert(remote_addr, remote_conn);
                                }
                                None => {
                                    let packet_data = PacketData::from(std::mem::replace(&mut buf, [0; MAX_PACKET_SIZE]));
                                    // TODO: this branch is only possible for gateways,
                                    // since if we are trying to establish a connection we should be inside the `traverse_nat` function
                                    // otherwise
                                }
                            }
                        }
                        Err(e) => {
                            // TODO: this should panic and be propagate to the main task or retry and eventually fail
                            tracing::error!("Failed to receive UDP packet: {:?}", e);
                            return Err(e.into());
                        }
                    }
                },
                // Handling of connection events
                connection_event = self.connection_handler.recv() => {
                    let Some((remote_addr, event)) = connection_event else { return Ok(()); };
                    let ConnectionEvent::ConnectionStart { remote_public_key, open_connection } = event;

                    match self.traverse_nat(
                        remote_addr,  remote_public_key,
                    ).await {
                        Err(error) => {
                            tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                            let _ = open_connection.send(Err(error));
                        }
                        Ok((outbound_remote_connection, inbound_remote_connection)) => {
                            self.remote_connections.insert(remote_addr, inbound_remote_connection);
                            let _ = open_connection.send(Ok(outbound_remote_connection));
                        }
                    }
                },
            }
        }
    }

    const NAT_TRAVERSAL_MAX_ATTEMPTS: usize = 20;

    async fn traverse_nat(
        &mut self,
        remote_addr: SocketAddr,
        remote_public_key: TransportPublicKey,
    ) -> Result<(RemoteConnection, InboundRemoteConnection), TransportError> {
        #[allow(clippy::large_enum_variant)]
        enum ConnectionState {
            /// Initial state of the joiner
            StartOutbound {},
            /// Initial state of the joinee, at this point NAT has been already traversed
            RemoteInbound {
                /// Encrypted intro packet for comparison
                intro_packet: PacketData,
            },
            /// Second state of the joiner, acknowledging their connection
            AckConnectionOutbound,
        }

        let mut state = ConnectionState::StartOutbound {};
        // Initialize timeout and interval
        let mut timeout = INITIAL_TIMEOUT;
        let mut interval_duration = INITIAL_INTERVAL;
        let mut tick = tokio::time::interval(interval_duration);

        let mut failures = 0;
        let mut packet = [0u8; MAX_PACKET_SIZE];

        let inbound_sym_key_bytes = rand::random::<[u8; 16]>();
        let inbound_sym_key = Aes128Gcm::new(&inbound_sym_key_bytes.into());
        let mut inbound_intro_packet: Option<PacketData> = None;

        let mut outbound_sym_key: Option<Aes128Gcm> = None;
        let outbound_intro_packet = {
            let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
            data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
            data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
            PacketData::<MAX_PACKET_SIZE>::encrypt_with_pubkey(&data, &remote_public_key)
        };

        while failures < Self::NAT_TRAVERSAL_MAX_ATTEMPTS {
            match state {
                ConnectionState::StartOutbound { .. } => {
                    tracing::debug!("Sending protocol version and inbound key to remote");
                    if self
                        .outbound_packets
                        .send((remote_addr, outbound_intro_packet.data().into()))
                        .await
                        .is_err()
                    {
                        failures += 1;
                        if failures == Self::NAT_TRAVERSAL_MAX_ATTEMPTS {
                            return Err(TransportError::ConnectionClosed);
                        }
                        tick.tick().await;
                        continue;
                    }
                }
                ConnectionState::AckConnectionOutbound => {
                    let acknowledgment =
                        SymmetricMessage::ack_ok(outbound_sym_key.as_mut().unwrap())?;
                    let _ = self
                        .outbound_packets
                        .send((remote_addr, acknowledgment.data().into()))
                        .await;
                    let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
                    sent_tracker.lock().report_sent_packet(
                        SymmetricMessage::FIRST_MESSAGE_ID,
                        acknowledgment.data().into(),
                    );
                    // we are connected to the remote and we just send the pub key to them
                    // if they fail to receive it, they will re-request the packet through
                    // the regular error control mechanism
                    let (inbound_sender, inbound_recv) = mpsc::channel(100);
                    return Ok((
                        RemoteConnection {
                            outbound_packets: self.outbound_packets.clone(),
                            outbound_symmetric_key: outbound_sym_key
                                .expect("should be set at this stage"),
                            remote_is_gateway: false,
                            remote_addr,
                            sent_tracker,
                            last_message_id: Arc::new(AtomicU32::new(0)),
                            inbound_packet_recv: inbound_recv,
                            inbound_symmetric_key: inbound_sym_key,
                        },
                        InboundRemoteConnection {
                            inbound_packet_sender: inbound_sender,
                            inbound_intro_packet: inbound_intro_packet.take(),
                            inbound_checked_times: 0,
                        },
                    ));
                }
                ConnectionState::RemoteInbound { .. } => {
                    // the other peer, which is at the Start state, will receive our inbound key (see below)
                    tracing::debug!("Sending back protocol version and inbound key to remote");
                    if self
                        .outbound_packets
                        .send((remote_addr, outbound_intro_packet.data().into()))
                        .await
                        .is_err()
                    {
                        failures += 1;
                        if failures == Self::NAT_TRAVERSAL_MAX_ATTEMPTS {
                            return Err(TransportError::ConnectionClosed);
                        }
                        tick.tick().await;
                        continue;
                    }
                }
            }
            let next_inbound = {
                // TODO: if a message is received from a different remote, reduce the timeout
                // by the passed time since it doesn't count
                tokio::time::timeout(timeout, self.socket_listener.recv_from(&mut packet)).boxed()
            };
            match next_inbound.await {
                Ok(Ok((size, response_remote))) => {
                    if response_remote != remote_addr {
                        if let Some(remote) = self.remote_connections.remove(&remote_addr) {
                            let _ = remote
                                .inbound_packet_sender
                                .send(PacketData::from(&packet[..size]))
                                .await;
                        } else {
                            self.pending_inbound_queue
                                .push_back((response_remote, PacketData::from(&packet[..size])));
                        }
                        continue;
                    }
                    match state {
                        ConnectionState::StartOutbound { .. } => {
                            // at this point it's either the remote sending us an intro packet or a symmetric packet
                            // cause is the first packet that passes through the NAT

                            let packet = PacketData::from(&packet[..size]);
                            if let Ok(decrypted_packet) = packet.decrypt(&inbound_sym_key) {
                                // the other peer initially received our intro packet and encrypted with our inbound_key
                                // so decrypting with our key should work
                                // means that at this point the NAT has been traversed and they are already receiving our messages
                                let key = Aes128Gcm::new_from_slice(
                                    &decrypted_packet.data()[PROTOC_VERSION.len()..],
                                )
                                .map_err(|_| {
                                    TransportError::ConnectionEstablishmentFailure {
                                        cause: "invalid symmetric key".into(),
                                    }
                                })?;
                                let protocol_version =
                                    &decrypted_packet.data()[..PROTOC_VERSION.len()];
                                if protocol_version != PROTOC_VERSION {
                                    let packet = SymmetricMessage::ack_error(&key)?;
                                    let _ = self
                                        .outbound_packets
                                        .send((remote_addr, packet.into()))
                                        .await;
                                    return Err(TransportError::ConnectionEstablishmentFailure {
                                        cause: format!(
                                            "remote is using a different protocol version: {:?}",
                                            String::from_utf8_lossy(protocol_version)
                                        )
                                        .into(),
                                    });
                                }
                                outbound_sym_key = Some(key);
                                // now we need to send back a packet with our asymetric pub key for the remote to have
                                // so it can enroute others to us if necessary
                                state = ConnectionState::AckConnectionOutbound;
                                continue;
                            }

                            // probably the first packet to punch through the NAT
                            if let Ok(decrypted_intro_packet) =
                                self.this_peer_keypair.secret.decrypt(packet.data())
                            {
                                let protoc = &decrypted_intro_packet[..PROTOC_VERSION.len()];
                                if protoc != PROTOC_VERSION {
                                    todo!("return error");
                                }
                                let outbound_key_bytes = &decrypted_intro_packet
                                    [PROTOC_VERSION.len()..PROTOC_VERSION.len() + 16];
                                let outbound_key = Aes128Gcm::new_from_slice(outbound_key_bytes)
                                    .expect("correct length");
                                outbound_sym_key = Some(outbound_key.clone());
                                state = ConnectionState::RemoteInbound {
                                    intro_packet: packet,
                                };
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
                            let packet = PacketData::from(&packet[..size]);
                            if packet.is_intro_packet(intro_packet) {
                                continue;
                            }
                            // if is not an intro packet, the connection is successful and we can proceed
                            let (inbound_sender, inbound_recv) = mpsc::channel(100);
                            return Ok((
                                RemoteConnection {
                                    outbound_packets: self.outbound_packets.clone(),
                                    outbound_symmetric_key: outbound_sym_key
                                        .expect("should be set at this stage"),
                                    remote_is_gateway: false,
                                    remote_addr,
                                    sent_tracker: Arc::new(parking_lot::Mutex::new(
                                        SentPacketTracker::new(),
                                    )),
                                    last_message_id: Arc::new(AtomicU32::new(0)),
                                    inbound_packet_recv: inbound_recv,
                                    inbound_symmetric_key: inbound_sym_key,
                                },
                                InboundRemoteConnection {
                                    inbound_packet_sender: inbound_sender,
                                    inbound_intro_packet: Some(intro_packet.clone()),
                                    inbound_checked_times: 0,
                                },
                            ));
                        }
                        ConnectionState::AckConnectionOutbound => {
                            // we never reach this state cause we break out of this function before checking
                            // for more remote packets
                            unreachable!()
                        }
                    }
                }
                Ok(Err(io_error)) => {
                    failures += 1;
                    tracing::debug!(%io_error, "Failed to receive UDP response");
                }
                Err(_) => {
                    failures += 1;
                    tracing::debug!("Failed to receive UDP response, time out");
                }
            }
            // Update timeout using exponential backoff, capped at MAX_TIMEOUT
            timeout = std::cmp::min(
                Duration::from_secs(timeout.as_secs() * TIMEOUT_MULTIPLIER),
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
    }
}

enum ConnectionEvent {
    ConnectionStart {
        remote_public_key: TransportPublicKey,
        open_connection: oneshot::Sender<Result<RemoteConnection, TransportError>>,
    },
}

struct InboundRemoteConnection {
    inbound_packet_sender: mpsc::Sender<PacketData>,
    inbound_intro_packet: Option<PacketData>,
    inbound_checked_times: usize,
}

impl InboundRemoteConnection {
    fn check_inbound_packet(&mut self, packet: &PacketData) -> bool {
        let mut inbound = false;
        if let Some(inbound_intro_packet) = self.inbound_intro_packet.as_ref() {
            if packet.is_intro_packet(inbound_intro_packet) {
                inbound = true;
            }
        }
        if self.inbound_checked_times >= UdpPacketsListener::<UdpSocket>::NAT_TRAVERSAL_MAX_ATTEMPTS
        {
            // no point in checking more than the max attemps since they won't be sending
            // the intro packet more than this amount of times
            self.inbound_intro_packet = None;
        } else {
            self.inbound_checked_times += 1;
        }
        inbound
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransportError {
    #[error("transport handler channel closed")]
    ChannelClosed,
    #[error("connection to remote closed")]
    ConnectionClosed,
    #[error("failed while establishing connection, reason: {cause}")]
    ConnectionEstablishmentFailure { cause: Cow<'static, str> },
    #[error("incomplete inbound stream: {0}")]
    IncompleteInboundStream(u32),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error("{0}")]
    PrivateKeyDecryptionError(aes_gcm::aead::Error),
    #[error(transparent)]
    PubKeyDecryptionError(#[from] rsa::errors::Error),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
    #[error("received unexpected message from remote: {0}")]
    UnexpectedMessage(Cow<'static, str>),
}

// TODO: add test for establishing a connection between two non-gateways (at localhost)
// it should be already possible to do this with the current code
// (spawn an other thread for the 2nd peer)
