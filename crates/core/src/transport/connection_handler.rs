use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use std::vec::Vec;

use aes_gcm::{Aes128Gcm, KeyInit};
use futures::FutureExt;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task;

use super::{
    crypto::{TransportKeypair, TransportPublicKey},
    packet_data::MAX_PACKET_SIZE,
    peer_connection::{PeerConnection, RemoteConnection},
    sent_packet_tracker::SentPacketTracker,
    symmetric_message::{SymmetricMessage, SymmetricMessagePayload},
    PacketData, Socket, TransportError,
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
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent)>,
    new_connection_notifier: mpsc::Receiver<PeerConnection>,
}

impl ConnectionHandler {
    pub async fn new<S: Socket>(
        keypair: TransportKeypair,
        listen_port: u16,
        is_gateway: bool,
    ) -> Result<Self, TransportError> {
        // Bind the UDP socket to the specified port
        let socket = Arc::new(S::bind((Ipv4Addr::UNSPECIFIED, listen_port).into()).await?);

        // Channel buffer is one so senders will await until the receiver is ready, important for bandwidth limiting
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(100);
        let (new_connection_sender, new_connection_notifier) = mpsc::channel(100);

        let (outbound_sender, outbound_recv) = mpsc::channel(1);
        let transport = UdpPacketsListener {
            is_gateway,
            socket_listener: socket.clone(),
            this_peer_keypair: keypair,
            remote_connections: BTreeMap::new(),
            connection_handler: conn_handler_receiver,
            new_connection_notifier: new_connection_sender,
            outbound_packets: outbound_sender,
        };
        let bw_tracker = super::rate_limiter::PacketRateLimiter::new(
            DEFAULT_BW_TRACKER_WINDOW_SIZE,
            outbound_recv,
        );
        let connection_handler = ConnectionHandler {
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
        let (open_connection, recv_connection) = oneshot::channel();
        self.send_queue
            .send((
                remote_addr,
                ConnectionEvent::ConnectionStart {
                    remote_public_key,
                    remote_is_gateway,
                    open_connection,
                },
            ))
            .await
            .map_err(|_| TransportError::ChannelClosed)?;
        let outbound_conn = recv_connection.await.map_err(|e| anyhow::anyhow!(e))??;
        Ok(PeerConnection::new(outbound_conn))
    }

    pub async fn next_connection(&mut self) -> Option<PeerConnection> {
        self.new_connection_notifier.recv().await
    }
}

pub enum Message {
    Short(Vec<u8>),
    Streamed(Vec<u8>, mpsc::Receiver<StreamFragment>),
}

pub struct StreamFragment {
    pub fragment_number: u32,
    pub fragment: Vec<u8>,
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
}

impl<S: Socket> UdpPacketsListener<S> {
    async fn listen(mut self) -> Result<(), TransportError> {
        let mut buf = [0u8; MAX_PACKET_SIZE];
        loop {
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
                                    if self.is_gateway {
                                        tracing::debug!(%remote_addr, "unexpected packet from remote");
                                    }
                                    let packet_data = PacketData::from(std::mem::replace(&mut buf, [0; MAX_PACKET_SIZE]));
                                    if let Err(error) = self.gateway_connection(packet_data, remote_addr).await {
                                        tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                                    }
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
                    let ConnectionEvent::ConnectionStart { remote_public_key, remote_is_gateway, open_connection } = event;

                    match self.traverse_nat(
                        remote_addr,  remote_public_key, remote_is_gateway
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

    async fn gateway_connection(
        &mut self,
        remote_intro_packet: PacketData,
        remote_addr: SocketAddr,
    ) -> Result<(), TransportError> {
        let Ok(decrypted_intro_packet) = self
            .this_peer_keypair
            .secret
            .decrypt(remote_intro_packet.data())
        else {
            tracing::debug!(%remote_addr, "Failed to decrypt packet with private key");
            return Ok(());
        };
        let protoc = &decrypted_intro_packet[..PROTOC_VERSION.len()];
        let outbound_key_bytes =
            &decrypted_intro_packet[PROTOC_VERSION.len()..PROTOC_VERSION.len() + 16];
        let outbound_key = Aes128Gcm::new_from_slice(outbound_key_bytes).map_err(|_| {
            TransportError::ConnectionEstablishmentFailure {
                cause: "invalid symmetric key".into(),
            }
        })?;
        if protoc != PROTOC_VERSION {
            let packet = SymmetricMessage::ack_error(&outbound_key)?;
            self.outbound_packets
                .send((remote_addr, packet.into()))
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

        let inbound_key_bytes = rand::random::<[u8; 16]>();
        let inbound_key = Aes128Gcm::new(&inbound_key_bytes.into());
        let outbound_ack_packet =
            SymmetricMessage::ack_gateway_connection(&outbound_key, inbound_key_bytes)?;

        let mut buf = [0u8; MAX_PACKET_SIZE];
        let mut waiting_time = INITIAL_INTERVAL;
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 20;
        while attempts < MAX_ATTEMPTS {
            self.outbound_packets
                .send((remote_addr, outbound_ack_packet.clone().into()))
                .await
                .map_err(|_| TransportError::ChannelClosed)?;

            // wait until the remote sends the ack packet
            let timeout =
                tokio::time::timeout(waiting_time, self.socket_listener.recv_from(&mut buf));
            let packet = match timeout.await {
                Ok(Ok((size, remote))) => {
                    let packet = PacketData::from(&buf[..size]);
                    if remote != remote_addr {
                        if let Some(remote) = self.remote_connections.remove(&remote_addr) {
                            let _ = remote.inbound_packet_sender.send(packet).await;
                            self.remote_connections.insert(remote_addr, remote);
                            continue;
                        }
                    }
                    packet
                }
                Ok(Err(_)) => {
                    return Err(TransportError::ChannelClosed);
                }
                Err(_) => {
                    attempts += 1;
                    waiting_time = std::cmp::min(
                        Duration::from_millis(
                            waiting_time.as_millis() as u64 * INTERVAL_INCREASE_FACTOR,
                        ),
                        MAX_INTERVAL,
                    );
                    continue;
                }
            };
            let Ok(_decrypted_packet) = packet.decrypt(&inbound_key) else {
                tracing::debug!(%remote_addr, "Failed to decrypt packet with inbound key");
                return Err(TransportError::ConnectionEstablishmentFailure {
                    cause: "invalid symmetric key".into(),
                });
            };
            // we know the inbound is successfully connected now and can proceed
            // ignoring this will force them to resend the packet but that is fine and simpler
            break;
        }

        let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
        let peer_connection = PeerConnection::new(RemoteConnection {
            outbound_packets: self.outbound_packets.clone(),
            outbound_symmetric_key: outbound_key,
            remote_addr,
            sent_tracker: sent_tracker.clone(),
            last_packet_id: Arc::new(AtomicU32::new(0)),
            inbound_packet_recv: mpsc::channel(100).1,
            inbound_symmetric_key: inbound_key,
        });

        self.new_connection_notifier
            .send(peer_connection)
            .await
            .map_err(|_| TransportError::ChannelClosed)?;

        sent_tracker.lock().report_sent_packet(
            SymmetricMessage::FIRST_PACKET_ID,
            outbound_ack_packet.into(),
        );

        Ok(())
    }

    const NAT_TRAVERSAL_MAX_ATTEMPTS: usize = 20;

    async fn traverse_nat(
        &mut self,
        remote_addr: SocketAddr,
        remote_public_key: TransportPublicKey,
        remote_is_gateway: bool,
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
                    self.outbound_packets
                        .send((remote_addr, outbound_intro_packet.data().into()))
                        .await
                        .map_err(|_| TransportError::ChannelClosed)?;
                }
                ConnectionState::AckConnectionOutbound => {
                    let acknowledgment =
                        SymmetricMessage::ack_ok(outbound_sym_key.as_mut().unwrap())?;
                    self.outbound_packets
                        .send((remote_addr, acknowledgment.data().into()))
                        .await
                        .map_err(|_| TransportError::ChannelClosed)?;
                    let sent_tracker = Arc::new(parking_lot::Mutex::new(SentPacketTracker::new()));
                    sent_tracker.lock().report_sent_packet(
                        SymmetricMessage::FIRST_PACKET_ID,
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
                            remote_addr,
                            sent_tracker,
                            last_packet_id: Arc::new(AtomicU32::new(0)),
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
                    self.outbound_packets
                        .send((remote_addr, outbound_intro_packet.data().into()))
                        .await
                        .map_err(|_| TransportError::ChannelClosed)?;
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
                            self.remote_connections.insert(remote_addr, remote);
                        }
                        // if is an other peer we don't know yet, is an inbound intro packet
                        // we will deal with it later as he keeps sending packets trying to connect
                        continue;
                    }
                    match state {
                        ConnectionState::StartOutbound { .. } => {
                            // at this point it's either the remote sending us an intro packet or a symmetric packet
                            // cause is the first packet that passes through the NAT

                            let packet = PacketData::from(&packet[..size]);
                            if let Ok(decrypted_packet) = packet.decrypt(&inbound_sym_key) {
                                let symmetric_message =
                                    SymmetricMessage::deser(decrypted_packet.data())?;
                                if remote_is_gateway {
                                    match symmetric_message.payload {
                                        SymmetricMessagePayload::GatewayConnection { key } => {
                                            let outbound_sym_key = Aes128Gcm::new_from_slice(&key)
                                                .map_err(|_| {
                                                    TransportError::ConnectionEstablishmentFailure {
                                                        cause: "invalid symmetric key".into(),
                                                    }
                                                })?;
                                            let packet =
                                                SymmetricMessage::ack_ok(&outbound_sym_key)?;
                                            // burst the gateway with oks so it does not keep waiting for inbound packets
                                            // one of them hopefully will arrive fine
                                            for _ in 0..5 {
                                                self.outbound_packets
                                                    .send((remote_addr, packet.data().into()))
                                                    .await
                                                    .map_err(|_| TransportError::ChannelClosed)?;
                                            }
                                            self.outbound_packets
                                                .send((
                                                    remote_addr,
                                                    SymmetricMessage::ack_ok(&outbound_sym_key)?
                                                        .data()
                                                        .into(),
                                                ))
                                                .await
                                                .map_err(|_| TransportError::ChannelClosed)?;
                                            let (inbound_sender, inbound_recv) = mpsc::channel(100);
                                            return Ok((
                                                RemoteConnection {
                                                    outbound_packets: self.outbound_packets.clone(),
                                                    outbound_symmetric_key: outbound_sym_key,
                                                    remote_addr,
                                                    sent_tracker: Arc::new(
                                                        parking_lot::Mutex::new(
                                                            SentPacketTracker::new(),
                                                        ),
                                                    ),
                                                    last_packet_id: Arc::new(AtomicU32::new(0)),
                                                    inbound_packet_recv: inbound_recv,
                                                    inbound_symmetric_key: inbound_sym_key,
                                                },
                                                InboundRemoteConnection {
                                                    inbound_packet_sender: inbound_sender,
                                                    inbound_intro_packet: None,
                                                    inbound_checked_times: 0,
                                                },
                                            ));
                                        }
                                        SymmetricMessagePayload::AckConnection { result } => {
                                            let Err(cause) = result else {
                                                return Err(TransportError::ConnectionEstablishmentFailure { cause: "Unreachable".into() });
                                            };
                                            return Err(
                                                TransportError::ConnectionEstablishmentFailure {
                                                    cause,
                                                },
                                            );
                                        }
                                        _ => {
                                            return Err(
                                                TransportError::ConnectionEstablishmentFailure {
                                                    cause: "Unexpected message".into(),
                                                },
                                            );
                                        }
                                    }
                                }

                                // the other peer initially received our intro packet and encrypted with our inbound_key
                                // so decrypting with our key should work
                                // means that at this point their NAT has been traversed and they are already receiving our messages
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
                                    self.outbound_packets
                                        .send((remote_addr, packet.into()))
                                        .await
                                        .map_err(|_| TransportError::ChannelClosed)?;
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
                                    remote_addr,
                                    sent_tracker: Arc::new(parking_lot::Mutex::new(
                                        SentPacketTracker::new(),
                                    )),
                                    last_packet_id: Arc::new(AtomicU32::new(0)),
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
        remote_is_gateway: bool,
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

// TODO: add test for establishing a connection between two non-gateways (at localhost)
// it should be already possible to do this with the current code
// (spawn an other thread for the 2nd peer)

#[cfg(test)]
mod test {
    use std::{collections::HashMap, net::Ipv4Addr, sync::OnceLock};

    use tokio::sync::Mutex;

    use super::*;

    #[allow(clippy::type_complexity)]
    static CHANNELS: OnceLock<Mutex<HashMap<SocketAddr, mpsc::Sender<(SocketAddr, Vec<u8>)>>>> =
        OnceLock::new();

    struct MockSocket {
        inbound: Mutex<mpsc::Receiver<(SocketAddr, Vec<u8>)>>,
    }

    impl Socket for MockSocket {
        async fn bind(addr: SocketAddr) -> Result<Self, std::io::Error> {
            let channels = CHANNELS.get_or_init(|| Mutex::new(HashMap::new()));
            let (outbound, inbound) = mpsc::channel(1);
            channels.lock().await.insert(addr, outbound);
            Ok(MockSocket {
                inbound: Mutex::new(inbound),
            })
        }

        async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            let Some((remote, packet)) = self.inbound.lock().await.recv().await else {
                return Err(std::io::ErrorKind::ConnectionAborted.into());
            };
            buf.copy_from_slice(&packet[..]);
            Ok((packet.len(), remote))
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
            let channels = CHANNELS.get_or_init(|| Mutex::new(HashMap::new()));
            let channels = channels.lock().await;
            let Some(sender) = channels.get(&target) else {
                return Ok(0);
            };
            sender
                .send((target, buf.to_vec()))
                .await
                .map_err(|_| std::io::ErrorKind::ConnectionAborted)?;
            Ok(buf.len())
        }
    }

    #[tokio::test]
    async fn simulate_nat_traversal() -> Result<(), Box<dyn std::error::Error>> {
        let peer_a_keypair = TransportKeypair::new();
        let peer_a_pub = peer_a_keypair.public.clone();
        let mut peer_a = ConnectionHandler::new::<MockSocket>(peer_a_keypair, 8080, false)
            .await
            .unwrap();

        let peer_b_keypair = TransportKeypair::new();
        let peer_b_pub = peer_b_keypair.public.clone();
        let mut peer_b = ConnectionHandler::new::<MockSocket>(peer_b_keypair, 8081, false)
            .await
            .unwrap();

        let peer_b = tokio::spawn(async move {
            let _peer_a_conn = peer_b
                .connect(peer_a_pub, (Ipv4Addr::LOCALHOST, 8080).into(), false)
                .await?;
            Ok::<_, TransportError>(())
        });

        let peer_a = tokio::spawn(async move {
            let _peer_b_conn = peer_a
                .connect(peer_b_pub, (Ipv4Addr::LOCALHOST, 8081).into(), false)
                .await?;
            Ok::<_, TransportError>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
        Ok(())
    }
}
