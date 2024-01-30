use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::vec::Vec;
use std::{borrow::Cow, time::Duration};

use aes_gcm::{Aes128Gcm, KeyInit};
use futures::{channel::oneshot, stream::FuturesUnordered, FutureExt, SinkExt, StreamExt};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task;

use super::peer_connection::SenderStreamError;
use super::{
    crypto::{TransportKeypair, TransportPublicKey},
    packet_data::{MAX_DATA_SIZE, MAX_PACKET_SIZE},
    peer_connection::{PeerConnection, SenderStream},
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

type ConnectionHandlerMessage = (SocketAddr, Vec<u8>);
pub type SerializedMessage = Vec<u8>;
type PeerChannel = (mpsc::Sender<SerializedMessage>, mpsc::Receiver<PacketData>);

struct OutboundMessage {
    remote_addr: SocketAddr,
    msg: SerializedMessage,
    recv: mpsc::Receiver<SerializedMessage>,
}

pub(crate) struct ConnectionHandler {
    max_upstream_rate: Arc<arc_swap::ArcSwap<BytesPerSecond>>,
    send_queue: mpsc::Sender<(SocketAddr, ConnectionEvent)>,
}

impl ConnectionHandler {
    pub async fn new(
        keypair: TransportKeypair,
        listen_port: u16,
        is_gateway: bool,
        max_upstream_rate: BytesPerSecond,
    ) -> Result<Self, TransportError> {
        // Bind the UDP socket to the specified port
        let socket = UdpSocket::bind(("0.0.0.0", listen_port)).await?;

        // Channel buffer is one so senders will await until the receiver is ready, important for bandwidth limiting
        let (conn_handler_sender, conn_handler_receiver) = mpsc::channel(1);

        let max_upstream_rate = Arc::new(arc_swap::ArcSwap::from_pointee(max_upstream_rate));
        let transport = UdpPacketsListener {
            remote_connections: HashMap::new(),
            socket,
            connection_handler: conn_handler_receiver,
            this_peer_keypair: keypair,
            max_upstream_rate: max_upstream_rate.clone(),
            is_gateway,
        };
        let connection_handler = ConnectionHandler {
            send_queue: conn_handler_sender,
            max_upstream_rate,
        };

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
            let ((outbound_sender, inbound_recv), inbound_sym_key) =
                recv_connection.await.map_err(|e| anyhow::anyhow!(e))??;
            Ok(PeerConnection {
                inbound_recv,
                outbound_sender,
                inbound_sym_key,
                ongoing_stream: None,
            })
        } else {
            todo!("establish connection with a gateway")
        }
    }

    fn update_max_upstream_rate(&mut self, max_upstream_rate: BytesPerSecond) {
        self.max_upstream_rate.store(Arc::new(max_upstream_rate));
    }
}

/// Handles UDP transport internally.
struct UdpPacketsListener {
    socket: UdpSocket,
    remote_connections: HashMap<SocketAddr, RemoteConnection>,
    connection_handler: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
    this_peer_keypair: TransportKeypair,
    max_upstream_rate: Arc<arc_swap::ArcSwap<BytesPerSecond>>,
    is_gateway: bool,
}
enum ConnectionState {
    Start {
        remote_public_key: TransportPublicKey,
    },
    RemoteResponse {
        outbound_key: Aes128Gcm,
    },
    AckConnection,
}

impl UdpPacketsListener {
    async fn listen(mut self) {
        let mut peer_messages = FuturesUnordered::new();
        loop {
            let mut buf = [0u8; MAX_PACKET_SIZE];
            tokio::select! {
                // Handling of inbound packets
                recv_result = self.socket.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, remote_addr)) => {
                            let remote_conn = self.remote_connections.remove(&remote_addr);
                            match remote_conn {
                                Some(remote_conn) => {
                                    let packet_data = PacketData::from(std::mem::replace(&mut buf, [0; MAX_PACKET_SIZE]));
                                    if remote_conn.inbound_packet_sender.send(packet_data).await.is_err() {
                                        // dropping this remote connection since we don't care about their messages anymore
                                        tracing::debug!(%remote_addr, "Remote disconnected");
                                    }
                                }
                                None => {
                                    self.handle_unrecogized_remote(remote_addr, &buf[..size]).await;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to receive UDP packet: {:?}", e);
                        }
                    }
                },
                // Handling of connection events
                send_message = self.connection_handler.recv() => {
                    if let Some((remote_addr, event)) = send_message {
                        match event {
                            ConnectionEvent::SendRawPacket(data) => {
                                if let Err(e) = self.socket.send_to(data.data(), remote_addr).await {
                                    tracing::warn!("Failed to send UDP packet: {:?}", e);
                                }
                            }
                            ConnectionEvent::ConnectionStart { remote_public_key, open_connection  }  => {
                                match self.traverse_nat(remote_addr, ConnectionState::Start { remote_public_key }).await {
                                    Err(error) => {
                                        tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                                    }
                                    Ok((remote_connection, inbound_recv, inbound_sym_key)) => {
                                        let (outbound_sender, outbound_receiver) = mpsc::channel(1);
                                        self.remote_connections.insert(remote_addr, remote_connection);
                                        let _ = open_connection.send(Ok(((outbound_sender, inbound_recv), inbound_sym_key)));
                                        peer_messages.push(peer_message(outbound_receiver, remote_addr));
                                    }
                                }
                            }
                        }
                    }
                },
                // Handling of outbound packets
                outbound_message = peer_messages.next(), if !peer_messages.is_empty() => {
                    let Some(outbound_msg) = outbound_message else {
                        // this should be unreachable, but it wouldn't matter either way
                        // the remote conn has been dropped
                        tracing::error!("peer_messages.next() returned None");
                        continue;
                    };
                    let OutboundMessage { remote_addr, msg, recv } = {
                        match outbound_msg {
                            Ok(outbound) => outbound,
                            Err(remote_addr) => {
                                // dropping this remote connection since we don't care about their messages anymore
                                tracing::debug!(%remote_addr, "Remote disconnected");
                                self.remote_connections.remove(&remote_addr);
                                continue;
                            }
                        }
                    };
                    let Some(remote_conn) = self.remote_connections.remove(&remote_addr) else {
                        // the connection was dropped by the other side
                        continue;
                    };
                    let Ok(remote_conn) = self.send_outbound_msg(remote_conn, msg).await else {
                        tracing::debug!(%remote_addr, "Remote disconnected");
                        continue;
                    };
                    self.remote_connections.insert(remote_addr, remote_conn);
                    peer_messages.push(peer_message(recv, remote_addr));
                }
            }
        }
    }

    #[inline]
    async fn send_outbound_msg(
        &self,
        mut remote_conn: RemoteConnection,
        serialized_data: SerializedMessage,
    ) -> Result<RemoteConnection, TransportError> {
        if serialized_data.len() > MAX_DATA_SIZE {
            let mut sender = SenderStream::new(&self.socket, &mut remote_conn);
            sender.send(serialized_data).await?;
        } else {
            let msg_id = remote_conn.last_message_id.wrapping_add(1);
            let packet = SymmetricMessage::short_message(
                msg_id,
                serialized_data,
                &remote_conn.outbound_symmetric_key,
            )?;
            self.socket
                .send_to(packet.data(), remote_conn.remote_addr)
                .await?;
        }
        Ok(remote_conn)
    }

    async fn traverse_nat(
        &self,
        remote_addr: SocketAddr,
        mut state: ConnectionState,
    ) -> Result<(RemoteConnection, mpsc::Receiver<PacketData>, Aes128Gcm), TransportError> {
        // Initialize timeout and interval
        let mut timeout = INITIAL_TIMEOUT;
        let mut interval_duration = INITIAL_INTERVAL;
        let mut tick = tokio::time::interval(interval_duration);

        const MAX_FAILURES: usize = 20;
        let mut failures = 0;
        let mut packet = [0u8; MAX_PACKET_SIZE];

        let inbound_sym_key_bytes = rand::random::<[u8; 16]>();
        let inbound_sym_key = Aes128Gcm::new(&inbound_sym_key_bytes.into());
        let mut outbound_sym_key: Option<Aes128Gcm> = None;

        let mut outbound_intro_packet = None;

        while failures < MAX_FAILURES {
            match state {
                ConnectionState::Start {
                    ref remote_public_key,
                } => {
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
                    tracing::debug!("Sending protocol version to remote");
                    if let Err(error) = self.socket.send_to(data.data(), remote_addr).await {
                        failures += 1;
                        if failures == MAX_FAILURES {
                            return Err(error.into());
                        }
                        tick.tick().await;
                        continue;
                    }
                }
                ConnectionState::AckConnection => {
                    self.socket
                        .send_to(
                            SymmetricMessage::ack_ok(outbound_sym_key.as_mut().unwrap())?.data(),
                            remote_addr,
                        )
                        .await?;
                }
                ConnectionState::RemoteResponse { outbound_key } => todo!(),
            }
            let next_inbound = if matches!(state, ConnectionState::Start { .. }) {
                async { Ok(self.socket.recv_from(&mut packet).await) }.boxed()
            } else {
                tokio::time::timeout(timeout, self.socket.recv_from(&mut packet)).boxed()
            };
            match next_inbound.await {
                Ok(Ok((_size, response_remote))) => {
                    if response_remote != remote_addr {
                        todo!("is a different remote, handle this message");
                    }
                    match state {
                        ConnectionState::Start { .. } => {
                            let data = PacketData::from(std::mem::replace(
                                &mut packet,
                                [0; MAX_PACKET_SIZE],
                            ));
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
                            // at this point the remote should have checked if the remote
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
                            state = ConnectionState::AckConnection;
                            continue;
                        }
                        ConnectionState::AckConnection => {
                            let packet: PacketData<MAX_PACKET_SIZE> =
                                std::mem::replace(&mut packet, [0; MAX_PACKET_SIZE]).into();
                            let decrypted = packet.decrypt(&inbound_sym_key).unwrap();
                            let packet =
                                bincode::deserialize::<SymmetricMessage>(decrypted.data())?;
                            if let SymmetricMessagePayload::AckConnection { result: Ok(_) } =
                                packet.payload
                            {
                                let (inbound_sender, inbound_recv) = mpsc::channel(1);
                                return Ok((
                                    RemoteConnection {
                                        outbound_symmetric_key: outbound_sym_key
                                            .expect("should be set at this stage"),
                                        inbound_packet_sender: inbound_sender,
                                        remote_is_gateway: false,
                                        remote_addr,
                                        last_message_id: 0,
                                    },
                                    inbound_recv,
                                    inbound_sym_key,
                                ));
                            }
                            tracing::debug!("Received unrecognized message from remote");
                            return Err(TransportError::ConnectionEstablishmentFailure {
                                cause: "received unrecognized message from remote".into(),
                            });
                        }
                        ConnectionState::RemoteResponse { outbound_key } => todo!(),
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
        packet: &[u8],
    ) -> Result<(RemoteConnection, mpsc::Receiver<PacketData>, Aes128Gcm), TransportError> {
        // logic for gateway should be slightly different cause we don't need to do nat traversal
        let decrypted_packet = match self.this_peer_keypair.secret.decrypt(packet) {
            Ok(req) => req,
            Err(error) => {
                tracing::debug!(%error, "Failed to decrypt packet");
                return Err(error.into());
            }
        };
        let protoc = &decrypted_packet[..PROTOC_VERSION.len()];
        if protoc != PROTOC_VERSION {
            todo!("return error");
        }
        let outbound_key_bytes = &decrypted_packet[PROTOC_VERSION.len()..PROTOC_VERSION.len() + 16];
        let outbound_key = Aes128Gcm::new_from_slice(outbound_key_bytes).expect("correct length");
        self.traverse_nat(
            remote_addr,
            ConnectionState::RemoteResponse { outbound_key },
        )
        .await
    }
}

#[inline]
async fn peer_message(
    mut outbound_receiver: mpsc::Receiver<SerializedMessage>,
    remote_addr: SocketAddr,
) -> Result<OutboundMessage, SocketAddr> {
    let msg = outbound_receiver.recv().await.ok_or({
        // dropping this remote connection since we don't care about their messages anymore
        // since the other side dropped the PeerConnectin
        remote_addr
    })?;
    Ok(OutboundMessage {
        remote_addr,
        msg,
        recv: outbound_receiver,
    })
}

#[allow(clippy::large_enum_variant)]
// we don't care about ConnectionStart being smaller since it's only used to establish the connection
enum ConnectionEvent {
    ConnectionStart {
        remote_public_key: TransportPublicKey,
        open_connection: oneshot::Sender<Result<(PeerChannel, Aes128Gcm), TransportError>>,
    },
    SendRawPacket(PacketData),
}

#[must_use]
pub(super) struct RemoteConnection {
    outbound_symmetric_key: Aes128Gcm,
    remote_is_gateway: bool,
    remote_addr: SocketAddr,
    inbound_packet_sender: mpsc::Sender<PacketData>,
    last_message_id: u32,
}

// Define a custom error type for the transport layer
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
