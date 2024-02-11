use aes_gcm::{Aes128Gcm, KeyInit};
use futures::FutureExt;
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::vec::Vec;
use std::{borrow::Cow, time::Duration};
use tokio::net::{ToSocketAddrs, UdpSocket};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task;

use crate::transport::received_packet_tracker::ReportResult;
use crate::util::time_source::CachingSystemTimeSrc;

use super::{
    bw,
    crypto::{TransportKeypair, TransportPublicKey},
    packet_data::MAX_PACKET_SIZE,
    peer_connection::{OutboundRemoteConnection, PeerConnection, SenderStreamError},
    received_packet_tracker::ReceivedPacketTracker,
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

pub(crate) struct ConnectionHandler<S = UdpSocket> {
    max_upstream_rate: Arc<arc_swap::ArcSwap<BytesPerSecond>>,
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent<S>)>,
    new_connection_notifier: mpsc::Receiver<PeerConnection<S>>,
    bw_tracker: Arc<Mutex<bw::PacketBWTracker<CachingSystemTimeSrc>>>,
}

impl<S: Socket> ConnectionHandler<S> {
    pub async fn new(
        keypair: TransportKeypair,
        listen_port: u16,
        is_gateway: bool,
        max_upstream_rate: BytesPerSecond,
    ) -> Result<Self, TransportError> {
        // Bind the UDP socket to the specified port
        let socket = Arc::new(S::bind(("0.0.0.0", listen_port)).await?);

        // Channel buffer is one so senders will await until the receiver is ready, important for bandwidth limiting
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(1);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(100);

        let max_upstream_rate = Arc::new(arc_swap::ArcSwap::from_pointee(max_upstream_rate));
        let transport = UdpPacketsListener {
            is_gateway,
            socket,
            this_peer_keypair: keypair,
            remote_connections: BTreeMap::new(),
            connection_handler: conn_handler_receiver,
            max_upstream_rate: max_upstream_rate.clone(),
            new_connection_notifier: new_connection_sender,
        };
        let bw_tracker = Arc::new(Mutex::new(super::bw::PacketBWTracker::new(
            DEFAULT_BW_TRACKER_WINDOW_SIZE,
        )));
        let connection_handler = ConnectionHandler {
            max_upstream_rate,
            send_queue: conn_handler_sender,
            new_connection_notifier,
            bw_tracker,
        };

        task::spawn(transport.listen());

        Ok(connection_handler)
    }

    pub async fn connect(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
        remote_is_gateway: bool,
    ) -> Result<PeerConnection<S>, TransportError> {
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
            Ok(PeerConnection::new(outbound_conn, self.bw_tracker.clone()))
        } else {
            todo!("establish connection with a gateway")
        }
    }

    pub async fn new_connection(&mut self) -> Option<PeerConnection<S>> {
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
    socket: Arc<S>,
    remote_connections: BTreeMap<SocketAddr, InboundRemoteConnection>,
    connection_handler: mpsc::Receiver<(SocketAddr, ConnectionEvent<S>)>,
    this_peer_keypair: TransportKeypair,
    max_upstream_rate: Arc<arc_swap::ArcSwap<BytesPerSecond>>,
    is_gateway: bool,
    // /// A new inbound connection that we haven't sent an explicit message yet
    // inbound_connections:
    //     BTreeMap<SocketAddr, (PeerChannel, Aes128Gcm, mpsc::Receiver<SerializedMessage>)>,
    new_connection_notifier: mpsc::Sender<PeerConnection<S>>,
}

#[allow(clippy::large_enum_variant)]
enum ConnectionState {
    /// Initial state of the joiner
    StartOutbound {
        remote_public_key: TransportPublicKey,
    },
    /// Initial state of the joinee, at this point NAT has been already traversed
    RemoteInbound {
        outbound_key: Aes128Gcm,
        /// Encrypted intro packet for comparison
        intro_packet: PacketData,
    },
    /// Second state of the joiner, acknowledging their connection
    AckConnectionOutbound,
}

impl<S: Socket> UdpPacketsListener<S> {
    async fn listen(mut self) -> Result<(), TransportError> {
        loop {
            let mut buf = [0u8; MAX_PACKET_SIZE];
            tokio::select! {
                // Handling of inbound packets
                recv_result = self.socket.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, remote_addr)) => {
                            let remote_conn = self.remote_connections.remove(&remote_addr);
                            match remote_conn {
                                Some(mut remote_conn) => {
                                    // todo: in the future optimize this to just take a PacketData only as large as necessary
                                    let packet_data = PacketData::from(&buf[..size]);
                                    if remote_conn.check_inbound_packet(&packet_data) {
                                        // we received a duplicate of the intro packet from the remote
                                        // we can ignore it since we already have the connection established
                                        continue;
                                    }
                                    let decrypted = packet_data.decrypt(&remote_conn.inbound_symmetric_key).map_err(|e| {
                                        tracing::error!(%e, ?remote_addr, "Failed to decrypt packet");
                                    }).unwrap();
                                    let msg = SymmetricMessage::deser(decrypted.data()).unwrap();
                                    if let SymmetricMessagePayload::AckConnection { result } = &msg.payload {
                                        match result {
                                            Ok(_) => {
                                                // if let Some(((outbound_sender, inbound_recv), inbound_sym_key, outbound_receiver)) = self.inbound_connections.remove(&remote_addr) {
                                                //     if self.new_connection_notifier.send(PeerConnection {
                                                //         inbound_recv,
                                                //         outbound_sender,
                                                //         inbound_sym_key,
                                                //         ongoing_stream: None,
                                                //     }).await.is_err() {
                                                //         return Err(TransportError::ChannelClosed);
                                                //     }
                                                //     let socket = self.socket.clone();
                                                //     // fixme
                                                //     // peer_messages.push(tokio::spawn(async move {
                                                //     //     outbound_messages(
                                                //     //         socket,
                                                //     //         outbound_receiver,
                                                //     //         remote_addr
                                                //     //     ).await
                                                //     // }));
                                                // }
                                                todo!()
                                            }
                                            Err(error) => {
                                                tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                                                continue;
                                            }
                                        }
                                    }
                                    match remote_conn.received_tracker.report_received_packet(msg.message_id) {
                                        ReportResult::Ok => {
                                            if remote_conn.inbound_packet_sender.send(msg.payload).await.is_err() {
                                                // dropping this remote connection since we don't care about their messages anymore
                                                tracing::debug!(%remote_addr, "Remote disconnected");
                                            }
                                        },
                                        ReportResult::AlreadyReceived => {
                                            if remote_conn.receipts_sender.send(vec![msg.message_id]).await.is_err() {
                                                tracing::debug!(%remote_addr, "Remote disconnected");
                                            }
                                        }
                                        ReportResult::QueueFull => todo!(),
                                    }
                                }
                                None => {
                                    // if we received a message, it means that a packet reached us and ports were mapped
                                    // so we can successfully receive messages from the remote
                                    let packet_data = PacketData::from(std::mem::replace(&mut buf, [0; MAX_PACKET_SIZE]));
                                    match self.handle_unrecogized_remote(remote_addr, packet_data).await {
                                        Err(error) => {
                                            tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                                        }
                                        Ok((outbound_remote_conn, inbound_remote_conn)) => {
                                            self.remote_connections.insert(remote_addr, inbound_remote_conn);
                                            // self.inbound_connections.insert(
                                            //     remote_addr,
                                            //     ((outbound_sender, inbound_recv), inbound_sym_key, outbound_receiver)
                                            // );
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // todo: this should panic and be propagate to the main task or retry and eventually fail
                            tracing::error!("Failed to receive UDP packet: {:?}", e);
                            return Err(e.into());
                        }
                    }
                },
                // Handling of connection events
                send_message = self.connection_handler.recv() => {
                    let Some((remote_addr, event)) = send_message else { return Ok(()); };
                    let ConnectionEvent::ConnectionStart { remote_public_key, open_connection } = event;
                    match self.traverse_nat(remote_addr, ConnectionState::StartOutbound { remote_public_key }).await {
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

    // fixme: there is a problem when establishing conenction since both peers are trying to connect simultaneously
    // they have an attempt where the connection is outbound and the other is inbound 2 different instances
    // of RemoteConnection will be created and the only the last one may be tracked, this is a race condition
    // that might (not sure) lead to weird behaviour for the packet trackers etc. so need to ensure both connections
    // are consolidated into one properly
    async fn traverse_nat(
        &self,
        remote_addr: SocketAddr,
        mut state: ConnectionState,
    ) -> Result<(OutboundRemoteConnection<S>, InboundRemoteConnection), TransportError> {
        // Initialize timeout and interval
        let mut timeout = INITIAL_TIMEOUT;
        let mut interval_duration = INITIAL_INTERVAL;
        let mut tick = tokio::time::interval(interval_duration);

        let mut failures = 0;
        let mut packet = [0u8; MAX_PACKET_SIZE];

        let inbound_sym_key_bytes = rand::random::<[u8; 16]>();
        let inbound_sym_key = Aes128Gcm::new(&inbound_sym_key_bytes.into());
        let mut outbound_sym_key: Option<Aes128Gcm> = {
            if let ConnectionState::RemoteInbound { outbound_key, .. } = &state {
                Some(outbound_key.clone())
            } else {
                None
            }
        };

        let mut outbound_intro_packet = None;
        let mut update_state: Option<ConnectionState> = None;

        while failures < Self::NAT_TRAVERSAL_MAX_ATTEMPTS {
            if let Some(new_state) = update_state.take() {
                state = new_state;
            }
            match state {
                ConnectionState::StartOutbound {
                    ref remote_public_key,
                } => {
                    // todo: refactor so Start and Remote response use the same code
                    if outbound_intro_packet.is_none() {
                        let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
                        data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
                        data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
                        outbound_intro_packet =
                            Some(PacketData::<MAX_PACKET_SIZE>::encrypted_with_remote(
                                &data,
                                remote_public_key,
                            ));
                    }
                    let data = outbound_intro_packet.as_ref().unwrap();
                    tracing::debug!("Sending protocol version and inbound key to remote");
                    if let Err(error) = self.socket.send_to(data.data(), remote_addr).await {
                        failures += 1;
                        if failures == Self::NAT_TRAVERSAL_MAX_ATTEMPTS {
                            return Err(error.into());
                        }
                        tick.tick().await;
                        continue;
                    }
                }
                ConnectionState::AckConnectionOutbound => {
                    let acknowledgment =
                        SymmetricMessage::ack_ok(outbound_sym_key.as_mut().unwrap())?;
                    let _ = self
                        .socket
                        .send_to(acknowledgment.data(), remote_addr)
                        .await;
                    let mut sent_tracker = SentPacketTracker::new();
                    sent_tracker.report_sent_packet(
                        SymmetricMessage::FIRST_MESSAGE_ID,
                        acknowledgment.data().into(),
                    );
                    // we are connected to the remote and we just send the pub key to them
                    // if they fail to receive it, they will re-request the packet through
                    // the regular error control mechanism
                    let (inbound_sender, inbound_recv) = mpsc::channel(1);
                    let (receipts_sender, receipts_notifier) = mpsc::channel(10);
                    return Ok((
                        OutboundRemoteConnection {
                            socket: self.socket.clone(),
                            outbound_symmetric_key: outbound_sym_key
                                .expect("should be set at this stage"),
                            remote_is_gateway: false,
                            remote_addr,
                            sent_tracker,
                            last_message_id: 0,
                            receipts_notifier,
                            inbound_recv,
                        },
                        InboundRemoteConnection {
                            inbound_symmetric_key: inbound_sym_key,
                            inbound_packet_sender: inbound_sender,
                            inbound_intro_packet: None,
                            inbound_checked_times: 0,
                            received_tracker: ReceivedPacketTracker::new(),
                            receipts_sender,
                        },
                    ));
                }
                ConnectionState::RemoteInbound { .. } => {
                    if outbound_intro_packet.is_none() {
                        // if an intro packet hasn't been created yet, create it
                        let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
                        data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
                        data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
                        outbound_intro_packet =
                            Some(PacketData::<MAX_PACKET_SIZE>::encrypted_with_cipher(
                                &data,
                                outbound_sym_key.as_ref().unwrap(),
                            ));
                    }
                    // the other peer, which is at the Start state, will receive our inbound key (see below)
                    let data = outbound_intro_packet.as_ref().unwrap();
                    tracing::debug!("Sending back protocol version and inbound key to remote");
                    if let Err(error) = self.socket.send_to(data.data(), remote_addr).await {
                        failures += 1;
                        if failures == Self::NAT_TRAVERSAL_MAX_ATTEMPTS {
                            return Err(error.into());
                        }
                        tick.tick().await;
                        continue;
                    }
                }
            }
            let next_inbound = {
                // todo: if a message is received from a different remote, reduce the timeout
                // by the passed time since it doesn't count
                tokio::time::timeout(timeout, self.socket.recv_from(&mut packet)).boxed()
            };
            match next_inbound.await {
                Ok(Ok((size, response_remote))) => {
                    if response_remote != remote_addr {
                        todo!("is a different remote, handle this message");
                    }
                    match state {
                        ConnectionState::StartOutbound { .. } => {
                            let data = PacketData::from(&packet[..size]);
                            // the peer initially received our intro packet and encrypted with our inbound_key
                            // see `handle_unrecogized_remote` for details, so decrypting with our key should work
                            // means that at this point the NAT has been traversed and they are already receiving our messages
                            let Ok(decrypted_packet) = data.decrypt(&inbound_sym_key) else {
                                failures += 1;
                                tracing::debug!("Failed to decrypt packet");
                                continue;
                            };
                            let key = Aes128Gcm::new_from_slice(
                                &decrypted_packet.data()[PROTOC_VERSION.len()..],
                            )
                            .map_err(|_| {
                                TransportError::ConnectionEstablishmentFailure {
                                    cause: "invalid symmetric key".into(),
                                }
                            })?;
                            let protocol_version = &decrypted_packet.data()[..PROTOC_VERSION.len()];
                            if protocol_version != PROTOC_VERSION {
                                let packet = SymmetricMessage::ack_error(&key)?;
                                let _ = self.socket.send_to(packet.data(), remote_addr).await;
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
                        ConnectionState::RemoteInbound {
                            // this is the packet encrypted with out RSA pub key
                            ref intro_packet,
                            ..
                        } => {
                            // update_state = Some(ConnectionState::AckConnection);
                            // next packet should be an acknowledgement packet, but might also be a repeated
                            // intro packet so we need to handle that
                            let packet = PacketData::from(&packet[..size]);
                            if packet.is_intro_packet(intro_packet) {
                                continue;
                            }
                            // if is not an intro packet, the connection is successful and we can proceed
                            let (inbound_sender, inbound_recv) = mpsc::channel(1);
                            let (receipts_sender, receipts_notifier) = mpsc::channel(10);
                            return Ok((
                                OutboundRemoteConnection {
                                    socket: self.socket.clone(),
                                    outbound_symmetric_key: outbound_sym_key
                                        .expect("should be set at this stage"),
                                    remote_is_gateway: false,
                                    remote_addr,
                                    sent_tracker: SentPacketTracker::new(),
                                    last_message_id: 0,
                                    receipts_notifier,
                                    inbound_recv,
                                },
                                InboundRemoteConnection {
                                    inbound_symmetric_key: inbound_sym_key,
                                    inbound_packet_sender: inbound_sender,
                                    inbound_intro_packet: None,
                                    inbound_checked_times: 0,
                                    received_tracker: ReceivedPacketTracker::new(),
                                    receipts_sender,
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

    async fn handle_unrecogized_remote(
        &self,
        remote_addr: SocketAddr,
        inbound_intro_packet: PacketData,
    ) -> Result<(OutboundRemoteConnection<S>, InboundRemoteConnection), TransportError> {
        // logic for gateway should be slightly different cause we don't need to do nat traversal
        let decrypted_intro_packet = match self
            .this_peer_keypair
            .secret
            .decrypt(inbound_intro_packet.data())
        {
            Ok(req) => req,
            Err(error) => {
                tracing::debug!(%error, "Failed to decrypt packet");
                return Err(error.into());
            }
        };
        let protoc = &decrypted_intro_packet[..PROTOC_VERSION.len()];
        if protoc != PROTOC_VERSION {
            todo!("return error");
        }
        let outbound_key_bytes =
            &decrypted_intro_packet[PROTOC_VERSION.len()..PROTOC_VERSION.len() + 16];
        let outbound_key = Aes128Gcm::new_from_slice(outbound_key_bytes).expect("correct length");
        // now we need to attempt punching through the NAT to the remote connection that can reach us
        let (or, mut ir) = self
            .traverse_nat(
                remote_addr,
                ConnectionState::RemoteInbound {
                    outbound_key,
                    intro_packet: inbound_intro_packet.clone(),
                },
            )
            .await?;
        // store the original intro packet encrypted with the pub key
        ir.inbound_intro_packet = Some(inbound_intro_packet);
        Ok((or, ir))
    }
}

enum ConnectionEvent<S = UdpSocket> {
    ConnectionStart {
        remote_public_key: TransportPublicKey,
        open_connection: oneshot::Sender<Result<OutboundRemoteConnection<S>, TransportError>>,
    },
}

struct InboundRemoteConnection {
    inbound_symmetric_key: Aes128Gcm,
    inbound_packet_sender: mpsc::Sender<SymmetricMessagePayload>,
    inbound_intro_packet: Option<PacketData>,
    inbound_checked_times: usize,
    received_tracker: ReceivedPacketTracker<CachingSystemTimeSrc>,
    receipts_sender: mpsc::Sender<Vec<u32>>,
}

impl InboundRemoteConnection {
    fn check_inbound_packet(&mut self, packet: &PacketData) -> bool {
        let mut inbound = false;
        if let Some(inbound_intro_packet) = self.inbound_intro_packet.as_ref() {
            if packet.is_intro_packet(inbound_intro_packet) {
                inbound = true;
            }
        }
        self.inbound_checked_times += 1;
        if self.inbound_checked_times >= UdpPacketsListener::<UdpSocket>::NAT_TRAVERSAL_MAX_ATTEMPTS
        {
            // no point in checking more than the max attemps since they won't be sending
            // the intro packet more than this amount of times
            self.inbound_intro_packet = None;
        }
        inbound
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransportError {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("transport handler channel closed")]
    ChannelClosed,
    #[error("connection to remote closed")]
    ConnectionClosed,
    #[error("failed while establishing connection, reason: {cause}")]
    ConnectionEstablishmentFailure { cause: Cow<'static, str> },
    #[error(transparent)]
    PubKeyDecryptionError(#[from] rsa::errors::Error),
    #[error("{0}")]
    PrivateKeyDecryptionError(aes_gcm::aead::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
    #[error(transparent)]
    StreamingError(#[from] SenderStreamError),
    #[error("received unexpected message from remote: {0}")]
    UnexpectedMessage(Cow<'static, str>),
}

// TODO: add test for establishing a connection between two non-gateways (at localhost)
// it should be already possible to do this with the current code
// (spawn an other thread for the 2nd peer)
