use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use std::vec::Vec;

use crate::transport::crypto::TransportSecretKey;
use crate::transport::packet_data::{AssymetricRSA, UnknownEncryption};
use aes_gcm::{Aes128Gcm, KeyInit};
use futures::{
    stream::{FuturesUnordered, StreamExt},
    Future,
};
use futures::{FutureExt, TryFutureExt};
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
        Self::config_listener(
            socket,
            keypair,
            is_gateway,
            #[cfg(test)]
            (Ipv4Addr::UNSPECIFIED, listen_port).into(),
        )
    }

    fn config_listener(
        socket: Arc<impl Socket>,
        keypair: TransportKeypair,
        is_gateway: bool,
        #[cfg(test)] socket_addr: SocketAddr,
    ) -> Result<Self, TransportError> {
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
            #[cfg(test)]
            this_addr: socket_addr,
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

    #[cfg(test)]
    fn test_set_up(
        socket_addr: SocketAddr,
        socket: Arc<impl Socket>,
        keypair: TransportKeypair,
        is_gateway: bool,
    ) -> Result<Self, TransportError> {
        Self::config_listener(socket, keypair, is_gateway, socket_addr)
    }

    pub async fn connect(
        &mut self,
        remote_public_key: TransportPublicKey,
        remote_addr: SocketAddr,
        remote_is_gateway: bool,
    ) -> Pin<Box<dyn Future<Output = Result<PeerConnection, TransportError>> + Send>> {
        let (open_connection, recv_connection) = oneshot::channel();
        if self
            .send_queue
            .send((
                remote_addr,
                ConnectionEvent::ConnectionStart {
                    remote_public_key,
                    remote_is_gateway,
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
    #[cfg(test)]
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

impl<T> Drop for UdpPacketsListener<T> {
    fn drop(&mut self) {
        #[cfg(test)]
        tracing::info!(%self.this_addr, "Dropping UdpPacketsListener");
    }
}

impl<S: Socket> UdpPacketsListener<S> {
    async fn listen(mut self) -> Result<(), TransportError> {
        let mut buf = [0u8; MAX_PACKET_SIZE];
        let mut ongoing_connections: BTreeMap<SocketAddr, OngoingConnection> = BTreeMap::new();
        let mut connection_tasks = FuturesUnordered::new();
        loop {
            tokio::select! {
                // Handling of inbound packets
                recv_result = self.socket_listener.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, remote_addr)) => {
                            let packet_data = PacketData::from_buf(&buf[..size]);
                            if let Some(remote_conn) = self.remote_connections.remove(&remote_addr){
                                let _ = remote_conn.inbound_packet_sender.send(packet_data).await;
                                self.remote_connections.insert(remote_addr, remote_conn);
                                continue;
                            }

                            if let Some((packets_sender, open_connection)) = ongoing_connections.remove(&remote_addr) {
                                if packets_sender.send(packet_data).await.is_err() {
                                    // it can happen that the connection is established but the channel is closed because the task completed
                                    // but we still ahven't polled the result future
                                    tracing::debug!(%remote_addr, "failed to send packet to remote");
                                }
                                ongoing_connections.insert(remote_addr, (packets_sender, open_connection));
                                continue;
                            }

                            if !self.is_gateway {
                                tracing::debug!(%remote_addr, "unexpected packet from remote");
                                continue;
                            }
                            let packet_data = PacketData::from_buf(&buf[..size]);
                            // FIXME: also parallelize this like we do with nat_traversal future
                            if let Err(error) = self.gateway_connection(packet_data, remote_addr).await {
                                tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                            }
                        }
                        Err(e) => {
                            // TODO: this should panic and be propagate to the main task or retry and eventually fail
                            tracing::error!("Failed to receive UDP packet: {:?}", e);
                            return Err(e.into());
                        }
                    }
                },
                connection_handshake = connection_tasks.next(), if !connection_tasks.is_empty() => {
                    let Some(res): OngoingConnectionResult = connection_handshake else {
                        unreachable!();
                    };
                    match res.expect("task shouldn't panic") {
                        Ok((outbound_remote_conn, inbound_remote_connection)) => {
                            if let Some((_, result_sender)) = ongoing_connections.remove(&outbound_remote_conn.remote_addr) {
                                tracing::debug!(%outbound_remote_conn.remote_addr, "connection established");
                                self.remote_connections.insert(outbound_remote_conn.remote_addr, inbound_remote_connection);
                                let _ = result_sender.send(Ok(outbound_remote_conn));
                            } else {
                                tracing::error!(%outbound_remote_conn.remote_addr, "connection established but no ongoing connection found");
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
                    let Some((remote_addr, event)) = connection_event else { return Ok(()); };
                    let ConnectionEvent::ConnectionStart { remote_public_key, remote_is_gateway, open_connection } = event;
                    tracing::debug!(%remote_addr, "attempting to establish connection");
                    let (ongoing_connection, packets_sender) = self.traverse_nat(
                        remote_addr,  remote_public_key, remote_is_gateway
                    );
                    let task = tokio::spawn(ongoing_connection.map_err(move |error| {
                        (error, remote_addr)
                    }));
                    connection_tasks.push(task);
                    ongoing_connections.insert(remote_addr, (packets_sender, open_connection));
                },
            }
        }
    }

    async fn gateway_connection(
        &mut self,
        remote_intro_packet: PacketData<UnknownEncryption>,
        remote_addr: SocketAddr,
    ) -> Result<(), TransportError> {
        tracing::debug!(%remote_addr, "new connection to gateway");
        let Ok(decrypted_intro_packet) = self
            .this_peer_keypair
            .secret
            .decrypt(remote_intro_packet.data())
        else {
            tracing::debug!(%remote_addr, "failed to decrypt packet with private key");
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

        let inbound_key_bytes = rand::random::<[u8; 16]>();
        let inbound_key = Aes128Gcm::new(&inbound_key_bytes.into());
        let outbound_ack_packet = SymmetricMessage::ack_gateway_connection(
            &outbound_key,
            inbound_key_bytes,
            remote_addr,
        )?;

        let mut buf = [0u8; MAX_PACKET_SIZE];
        let mut waiting_time = INITIAL_INTERVAL;
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 20;
        while attempts < MAX_ATTEMPTS {
            self.outbound_packets
                .send((remote_addr, outbound_ack_packet.clone().prepared_send()))
                .await
                .map_err(|_| TransportError::ChannelClosed)?;

            // wait until the remote sends the ack packet
            let timeout =
                tokio::time::timeout(waiting_time, self.socket_listener.recv_from(&mut buf));
            match timeout.await {
                Ok(Ok((size, remote))) => {
                    let packet = PacketData::from_buf(&buf[..size]);
                    if remote != remote_addr {
                        if let Some(remote) = self.remote_connections.remove(&remote_addr) {
                            let _ = remote.inbound_packet_sender.send(packet).await;
                            self.remote_connections.insert(remote_addr, remote);
                            continue;
                        }
                    }
                    let _ = packet.try_decrypt_sym(&inbound_key).map_err(|_| {
                        tracing::debug!(%remote_addr, "Failed to decrypt packet with inbound key");
                        TransportError::ConnectionEstablishmentFailure {
                            cause: "invalid symmetric key".into(),
                        }
                    })?;
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
            }
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
            my_address: None,
        });

        self.new_connection_notifier
            .send(peer_connection)
            .await
            .map_err(|_| TransportError::ChannelClosed)?;

        sent_tracker.lock().report_sent_packet(
            SymmetricMessage::FIRST_PACKET_ID,
            outbound_ack_packet.prepared_send(),
        );

        Ok(())
    }

    const NAT_TRAVERSAL_MAX_ATTEMPTS: usize = 20;

    fn traverse_nat(
        &mut self,
        remote_addr: SocketAddr,
        remote_public_key: TransportPublicKey,
        remote_is_gateway: bool,
    ) -> (
        impl Future<Output = Result<(RemoteConnection, InboundRemoteConnection), TransportError>>
            + Send
            + 'static,
        mpsc::Sender<PacketData<UnknownEncryption>>,
    ) {
        #[allow(clippy::large_enum_variant)]
        enum ConnectionState {
            /// Initial state of the joiner
            StartOutbound,
            /// Initial state of the joinee, at this point NAT has been already traversed
            RemoteInbound {
                /// Encrypted intro packet for comparison
                intro_packet: PacketData<AssymetricRSA>,
            },
            /// Second state of the joiner, acknowledging their connection
            AckConnectionOutbound,
        }

        fn decrypt_asym(
            packet: &PacketData<UnknownEncryption>,
            transport_secret_key: &TransportSecretKey,
            outbound_sym_key: &mut Option<Aes128Gcm>,
            state: &mut ConnectionState,
        ) -> Result<(), ()> {
            // probably the first packet to punch through the NAT
            if let Ok(decrypted_intro_packet) = packet.try_decrypt_asym(transport_secret_key) {
                let protoc = &decrypted_intro_packet.data()[..PROTOC_VERSION.len()];
                if protoc != PROTOC_VERSION {
                    todo!("return error");
                }
                let outbound_key_bytes =
                    &decrypted_intro_packet.data()[PROTOC_VERSION.len()..PROTOC_VERSION.len() + 16];
                let outbound_key =
                    Aes128Gcm::new_from_slice(outbound_key_bytes).expect("correct length");
                *outbound_sym_key = Some(outbound_key.clone());
                *state = ConnectionState::RemoteInbound {
                    intro_packet: packet.assert_assymetric(),
                };
                Ok(())
            } else {
                Err(())
            }
        }

        let outbound_packets = self.outbound_packets.clone();
        let transport_secret_key = self.this_peer_keypair.secret.clone();
        let (inbound_from_remote, mut next_inbound) =
            mpsc::channel::<PacketData<UnknownEncryption>>(1);
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

            let mut resend_intro = false;
            let mut sent_tracker = SentPacketTracker::new();

            while failures < Self::NAT_TRAVERSAL_MAX_ATTEMPTS {
                match state {
                    ConnectionState::StartOutbound { .. } => {
                        tracing::debug!(%remote_addr, "sending protocol version and inbound key");
                        outbound_packets
                            .send((remote_addr, outbound_intro_packet.data().into()))
                            .await
                            .map_err(|_| TransportError::ChannelClosed)?;
                    }
                    ConnectionState::AckConnectionOutbound => {
                        let acknowledgment =
                            SymmetricMessage::ack_ok(outbound_sym_key.as_mut().unwrap())?;
                        outbound_packets
                            .send((remote_addr, acknowledgment.data().into()))
                            .await
                            .map_err(|_| TransportError::ChannelClosed)?;
                        sent_tracker.report_sent_packet(
                            SymmetricMessage::FIRST_PACKET_ID,
                            acknowledgment.data().into(),
                        );
                        // we are connected to the remote and we just send the pub key to them
                        // if they fail to receive it, they will re-request the packet through
                        // the regular error control mechanism
                        let (inbound_sender, inbound_recv) = mpsc::channel(1);
                        return Ok((
                            RemoteConnection {
                                outbound_packets: outbound_packets.clone(),
                                outbound_symmetric_key: outbound_sym_key
                                    .expect("should be set at this stage"),
                                remote_addr,
                                sent_tracker: Arc::new(parking_lot::Mutex::new(sent_tracker)),
                                last_packet_id: Arc::new(AtomicU32::new(0)),
                                inbound_packet_recv: inbound_recv,
                                inbound_symmetric_key: inbound_sym_key,
                                my_address: None,
                            },
                            InboundRemoteConnection {
                                inbound_packet_sender: inbound_sender,
                                inbound_intro_packet: None,
                                inbound_checked_times: 0,
                            },
                        ));
                    }
                    ConnectionState::RemoteInbound { .. } => {
                        if resend_intro {
                            resend_intro = false;
                            tracing::debug!(%remote_addr, "resending intro packet due to possible packet loss");
                            outbound_packets
                                .send((remote_addr, outbound_intro_packet.data().into()))
                                .await
                                .map_err(|_| TransportError::ChannelClosed)?;
                        } else {
                            tracing::debug!(%remote_addr, "sending back protocol version and inbound key to remote");
                            let acknowledgment =
                                SymmetricMessage::ack_ok(outbound_sym_key.as_mut().unwrap())?;
                            outbound_packets
                                .send((remote_addr, acknowledgment.data().into()))
                                .await
                                .map_err(|_| TransportError::ChannelClosed)?;
                            sent_tracker.report_sent_packet(
                                SymmetricMessage::FIRST_PACKET_ID,
                                acknowledgment.data().into(),
                            );
                        }
                    }
                }
                let next_inbound = tokio::time::timeout(timeout, next_inbound.recv());
                match next_inbound.await {
                    Ok(Some(packet)) => {
                        match state {
                            ConnectionState::StartOutbound { .. } => {
                                // at this point it's either the remote sending us an intro packet or a symmetric packet
                                // cause is the first packet that passes through the NAT
                                // let sym_packet = packet.with_sym_encryption();
                                if let Ok(decrypted_packet) =
                                    packet.try_decrypt_sym(&inbound_sym_key)
                                {
                                    let symmetric_message =
                                        SymmetricMessage::deser(decrypted_packet.data())?;
                                    if remote_is_gateway {
                                        match symmetric_message.payload {
                                            SymmetricMessagePayload::GatewayConnection {
                                                key,
                                                remote_addr: my_address,
                                            } => {
                                                let outbound_sym_key = Aes128Gcm::new_from_slice(
                                                    &key,
                                                )
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
                                                    outbound_packets
                                                        .send((remote_addr, packet.data().into()))
                                                        .await
                                                        .map_err(|_| {
                                                            TransportError::ChannelClosed
                                                        })?;
                                                }
                                                outbound_packets
                                                    .send((
                                                        remote_addr,
                                                        SymmetricMessage::ack_ok(
                                                            &outbound_sym_key,
                                                        )?
                                                        .data()
                                                        .into(),
                                                    ))
                                                    .await
                                                    .map_err(|_| TransportError::ChannelClosed)?;
                                                let (inbound_sender, inbound_recv) =
                                                    mpsc::channel(100);
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
                                                        my_address: Some(my_address),
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
                                    let Ok(key) = Aes128Gcm::new_from_slice(
                                        &decrypted_packet.data()[PROTOC_VERSION.len()..],
                                    ).map_err(|error| {
                                        tracing::error!(%remote_addr, ?error, "Failed to create outbound symmetric key");
                                        error
                                    }) else {
                                        // it may be the case the remote still hasn't received the intro packet due to packet loss
                                        // and is sending intro packets still

                                        if decrypt_asym(&packet, &transport_secret_key, &mut outbound_sym_key, &mut state).is_err() {
                                            tracing::error!(%remote_addr, "Also failed to decrypt packet with private key");
                                            failures += 1;
                                        }
                                        continue;
                                    };
                                    let protocol_version =
                                        &decrypted_packet.data()[..PROTOC_VERSION.len()];
                                    if protocol_version != PROTOC_VERSION {
                                        let packet = SymmetricMessage::ack_error(&key)?;
                                        outbound_packets
                                            .send((remote_addr, packet.prepared_send()))
                                            .await
                                            .map_err(|_| TransportError::ChannelClosed)?;
                                        return Err(
                                            TransportError::ConnectionEstablishmentFailure {
                                                cause: format!(
                                            "remote is using a different protocol version: {:?}",
                                            String::from_utf8_lossy(protocol_version)
                                        )
                                                .into(),
                                            },
                                        );
                                    }
                                }

                                // probably the first packet to punch through the NAT
                                if decrypt_asym(
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
                                    resend_intro = true;
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
                                        my_address: None,
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
                    Ok(None) => {
                        return Err(TransportError::ConnectionClosed);
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
        };
        (f, inbound_from_remote)
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
    inbound_packet_sender: mpsc::Sender<PacketData<UnknownEncryption>>,
    inbound_intro_packet: Option<PacketData<AssymetricRSA>>,
    inbound_checked_times: usize,
}

impl InboundRemoteConnection {
    fn check_inbound_packet(&mut self, packet: &PacketData<UnknownEncryption>) -> bool {
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

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        net::Ipv4Addr,
        ops::Range,
        sync::{
            atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering},
            OnceLock,
        },
    };

    use futures::{stream::FuturesOrdered, TryStreamExt};
    use rand::{Rng, SeedableRng};
    use serde::{de::DeserializeOwned, Serialize};
    use tokio::sync::Mutex;
    use tracing::info;

    use super::*;
    use crate::DynError;

    #[allow(clippy::type_complexity)]
    static CHANNELS: OnceLock<
        Arc<Mutex<HashMap<SocketAddr, mpsc::UnboundedSender<(SocketAddr, Vec<u8>)>>>>,
    > = OnceLock::new();

    #[derive(Default, Clone)]
    enum PacketDropPolicy {
        /// Receive all packets without dropping
        #[default]
        ReceiveAll,
        /// Drop the packets randomly based on the factor
        Factor(f64),
        /// Drop packets fall in the given range
        Range(Range<usize>),
    }

    struct MockSocket {
        inbound: Mutex<mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>>,
        this: SocketAddr,
        packet_drop_policy: PacketDropPolicy,
        num_packets_sent: AtomicUsize,
        rng: Mutex<rand::rngs::SmallRng>,
    }

    impl MockSocket {
        async fn test_config(packet_drop_policy: PacketDropPolicy, addr: SocketAddr) -> Self {
            let channels = CHANNELS
                .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
                .clone();
            let (outbound, inbound) = mpsc::unbounded_channel();
            channels.lock().await.insert(addr, outbound);
            static SEED: AtomicU64 = AtomicU64::new(0xfeedbeef);
            MockSocket {
                inbound: Mutex::new(inbound),
                this: addr,
                packet_drop_policy,
                num_packets_sent: AtomicUsize::new(0),
                rng: Mutex::new(rand::rngs::SmallRng::seed_from_u64(
                    SEED.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                )),
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
                        tracing::trace!(id=%packet_idx, data=?buf, "drop packet");
                        return Ok(buf.len());
                    }
                }
                PacketDropPolicy::Range(r) => {
                    if r.contains(&packet_idx) {
                        tracing::trace!(id=%packet_idx, data=?buf, "drop packet");
                        return Ok(buf.len());
                    }
                }
            }

            assert!(self.this != target, "cannot send to self");
            let channels = CHANNELS
                .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
                .clone();
            let channels = channels.lock().await;
            let Some(sender) = channels.get(&target).cloned() else {
                return Ok(0);
            };
            drop(channels);
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
            let channels = CHANNELS
                .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
                .clone();
            loop {
                if let Ok(mut channels) = channels.try_lock() {
                    channels.remove(&self.this);
                    break;
                }
                // unorthodox blocking here but shouldn't be a problem for testing
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    async fn set_peer_connection(
        packet_drop_policy: PacketDropPolicy,
    ) -> Result<(TransportPublicKey, ConnectionHandler, SocketAddr), DynError> {
        set_peer_connection_in(packet_drop_policy, false).await
    }

    async fn set_gateway_connection(
        packet_drop_policy: PacketDropPolicy,
    ) -> Result<(TransportPublicKey, ConnectionHandler, SocketAddr), DynError> {
        set_peer_connection_in(packet_drop_policy, true).await
    }

    async fn set_peer_connection_in(
        packet_drop_policy: PacketDropPolicy,
        gateway: bool,
    ) -> Result<(TransportPublicKey, ConnectionHandler, SocketAddr), DynError> {
        static PORT: AtomicU16 = AtomicU16::new(25000);

        let peer_keypair = TransportKeypair::new();
        let peer_pub = peer_keypair.public.clone();
        let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let socket = Arc::new(
            MockSocket::test_config(packet_drop_policy, (Ipv4Addr::LOCALHOST, port).into()).await,
        );
        let peer_conn = ConnectionHandler::test_set_up(
            (Ipv4Addr::LOCALHOST, port).into(),
            socket,
            peer_keypair,
            gateway,
        )
        .expect("failed to create peer");
        Ok((peer_pub, peer_conn, (Ipv4Addr::LOCALHOST, port).into()))
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
    ) -> Result<(), DynError> {
        assert_eq!(generators.len(), config.peers);
        let mut peer_keys_and_addr = vec![];
        let mut peer_conns = vec![];
        for _ in 0..config.peers {
            let (peer_pub, peer, peer_addr) =
                set_peer_connection(config.packet_drop_policy.clone()).await?;
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
                        Duration::from_secs(10),
                        peer.connect(peer_pub.clone(), *peer_addr, false).await,
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
                    Duration::from_secs(1)
                };
                for ((_, peer_addr), mut peer_conn) in
                    peer_keys_and_addr.into_iter().zip(connections)
                {
                    let mut test_gen_cp = test_generator.clone();
                    conns.push_back(async move {
                        let mut messages = vec![];
                        while messages.len() < test_gen_cp.expected_iterations() {
                            peer_conn.send(test_gen_cp.gen_msg()).await?;
                            match peer_conn.recv().await {
                                Ok(msg) => {
                                    let output_as_str: G::Message = bincode::deserialize(&msg)?;
                                    messages.push(output_as_str);
                                    info!("{peer_addr:?} received {} messages", messages.len());
                                }
                                Err(error) => {
                                    tracing::error!(%error, "error receiving message");
                                    return Err(error);
                                }
                            }
                        }

                        let _ = tokio::time::timeout(extra_wait, peer_conn.recv()).await;
                        Ok(messages)
                    });
                }
                let results = tokio::time::timeout(
                    config.wait_time + extra_wait,
                    conns.try_collect::<Vec<_>>(),
                )
                .await??;
                Ok::<_, DynError>((results, test_generator))
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
    async fn simulate_nat_traversal() -> Result<(), DynError> {
        // crate::config::set_logger();
        let (peer_a_pub, mut peer_a, peer_a_addr) = set_peer_connection(Default::default()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) = set_peer_connection(Default::default()).await?;

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr, false).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, DynError>(())
        });

        let peer_a = tokio::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr, false).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            Ok::<_, DynError>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test]
    async fn simulate_gateway_traversal() -> Result<(), DynError> {
        // crate::config::set_logger();
        let (peer_a_pub, mut peer_a, peer_a_addr) = set_peer_connection(Default::default()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_gateway_connection(Default::default()).await?;

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr, false).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, DynError>(())
        });

        let peer_a = tokio::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr, false).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            Ok::<_, DynError>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn simulate_gateway_traversal_drop_first_3_packets_of_gateway() -> Result<(), DynError> {
        crate::config::set_logger(Some(tracing::level_filters::LevelFilter::TRACE));
        let (peer_a_pub, mut peer_a, peer_a_addr) = set_peer_connection(Default::default()).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_gateway_connection(PacketDropPolicy::Range(0..3)).await?;

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr, false).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, DynError>(())
        });

        let peer_a = tokio::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr, false).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            Ok::<_, DynError>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn simulate_gateway_traversal_drop_first_3_packets_of_peer() -> Result<(), DynError> {
        crate::config::set_logger(Some(tracing::level_filters::LevelFilter::TRACE));
        let (peer_a_pub, mut peer_a, peer_a_addr) =
            set_peer_connection(PacketDropPolicy::Range(0..3)).await?;
        let (peer_b_pub, mut peer_b, peer_b_addr) =
            set_gateway_connection(Default::default()).await?;

        let peer_b = tokio::spawn(async move {
            let peer_a_conn = peer_b.connect(peer_a_pub, peer_a_addr, false).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_a_conn).await??;
            Ok::<_, DynError>(())
        });

        let peer_a = tokio::spawn(async move {
            let peer_b_conn = peer_a.connect(peer_b_pub, peer_b_addr, false).await;
            let _ = tokio::time::timeout(Duration::from_secs(500), peer_b_conn).await??;
            Ok::<_, DynError>(())
        });

        let (a, b) = tokio::try_join!(peer_a, peer_b)?;
        a?;
        b?;
        Ok(())
    }

    #[tokio::test]
    async fn simulate_send_short_message() -> Result<(), DynError> {
        // crate::config::set_logger(Some(tracing::level_filters::LevelFilter::TRACE));
        #[derive(Clone, Copy)]
        struct TestData(&'static str, usize);

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
            Vec::from_iter((0..10).map(|i| TestData("foo", i))),
        )
        .await
    }

    #[tokio::test]
    async fn simulate_send_streamed_message() -> Result<(), DynError> {
        // crate::config::set_logger(Some(tracing::level_filters::LevelFilter::TRACE));
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

    #[tokio::test]
    async fn packet_dropping() -> Result<(), DynError> {
        crate::config::set_logger(Some(tracing::level_filters::LevelFilter::TRACE));
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

        let mut rng = rand::rngs::StdRng::seed_from_u64(3);
        for factor in std::iter::repeat(())
            .map(|_| rng.gen::<f64>())
            .filter(|x| *x > 0.05 && *x < 0.25)
            .take(3)
        {
            let wait_time = Duration::from_secs((15.0 * ((factor + 1.0) * 1.25)) as u64);
            tracing::info!(
                "packet loss factor: {factor} (wait time: {wait_time})",
                wait_time = wait_time.as_secs()
            );
            run_test(
                TestConfig {
                    packet_drop_policy: PacketDropPolicy::Factor(factor),
                    wait_time,
                    ..Default::default()
                },
                vec![TestData("foo"), TestData("bar")],
            )
            .await?
        }
        Ok(())
    }
}
