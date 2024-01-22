use std::collections::HashMap;
use std::net::SocketAddr;
use std::vec::Vec;
use std::{borrow::Cow, time::Duration};

use aes_gcm::{Aes128Gcm, KeyInit};
use futures::channel::oneshot;
use serde::Serialize;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task;

use crate::transport::packet_data::MAX_PACKET_SIZE;
use crate::transport::symmetric_message::{SymmetricMessage, SymmetricMessagePayload};

use super::{
    connection_info::{ConnectionError, ConnectionInfo},
    crypto::{TransportKeypair, TransportPublicKey},
    BytesPerSecond, PacketData,
};

pub(super) type ConnectionHandlerMessage = (SocketAddr, Vec<u8>);

const PROTOC_VERSION: [u8; 2] = 1u16.to_le_bytes();

type SerializedMessage = Vec<u8>;

pub struct PeerConnection {
    inbound_recv: mpsc::Receiver<PacketData>,
    outbound_sender: mpsc::Sender<SerializedMessage>,
}

impl PeerConnection {
    pub async fn recv(&mut self) -> Result<Vec<u8>, ConnectionError> {
        let packet_data = self
            .inbound_recv
            .recv()
            .await
            .ok_or(ConnectionError::ChannelClosed);
        todo!()
    }

    pub async fn send<T: Serialize>(&mut self, data: &T) -> Result<(), ConnectionError> {
        let _serialized_data = bincode::serialize(data).unwrap();
        // todo: send this to the udp listener and send in one packet or stream the message
        todo!()
    }
}

pub(crate) struct ConnectionHandler {
    listen_port: u16,
    is_gateway: bool,
    max_upstream_rate: BytesPerSecond,
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
        let (send_queue, send_queue_receiver) = mpsc::channel(1);

        let transport = UdpPacketsListener {
            connection_raw_packet_senders: HashMap::new(),
            socket,
            send_queue: send_queue_receiver,
            this_peer_keypair: keypair,
        };
        let connection_handler = ConnectionHandler {
            listen_port,
            is_gateway,
            max_upstream_rate,
            send_queue,
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
                .await?;
            let (outbound_sender, inbound_recv) =
                recv_connection.await.map_err(|e| anyhow::anyhow!(e))??;
            Ok(PeerConnection {
                inbound_recv,
                outbound_sender,
            })
        } else {
            todo!("establish connection with a gateway")
        }
    }

    fn update_max_upstream_rate(&mut self, max_upstream_rate: BytesPerSecond) {
        self.max_upstream_rate = max_upstream_rate;
    }

    fn handle_unrecognized_message(&self, (_socket, packet): (SocketAddr, PacketData)) {
        if !self.is_gateway {
            tracing::warn!(
                packet = ?packet.send_data(),
                "Received unrecognized message, ignoring because not a gateway",
            );
            return;
        }
        // use self.keypair to decrypt the message, which should contain a symmetric key
        todo!()
    }
}

/// Handles UDP transport internally.
struct UdpPacketsListener {
    socket: UdpSocket,
    connection_raw_packet_senders: HashMap<SocketAddr, (ConnectionInfo, mpsc::Sender<PacketData>)>,
    send_queue: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
    this_peer_keypair: TransportKeypair,
}

impl UdpPacketsListener {
    async fn listen(mut self) {
        loop {
            let mut buf = [0u8; MAX_PACKET_SIZE];
            tokio::select! {
                // Handling of inbound packets
                recv_result = self.socket.recv_from(&mut buf) => {
                    match recv_result {
                        Ok((size, addr)) => {
                            match self.connection_raw_packet_senders.get_mut(&addr) {
                                Some((conn_info, sender)) => {
                                    // let packet_data = PacketData::decrypt(std::mem::replace(&mut buf, [0; MAX_PACKET_SIZE]),  &mut conn_info.outbound_symmetric_key).unwrap();
                                    // if let Err(e) = sender.send(packet_data).await {
                                    //     tracing::warn!("Failed to send raw packet to connection sender: {:?}", e);
                                    // }
                                }
                                None => {
                                    self
                                        .handle_unrecognized_remote(addr);
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to receive UDP packet: {:?}", e);
                        }
                    }
                },
                // Handling of outbound packets
                send_message = self.send_queue.recv() => {
                    if let Some((remote_addr, event)) = send_message {
                        match event {
                            ConnectionEvent::SendRawPacket(data) => {
                                if let Err(e) = self.socket.send_to(data.send_data(), remote_addr).await {
                                    tracing::warn!("Failed to send UDP packet: {:?}", e);
                                }
                            }
                            ConnectionEvent::ConnectionStart { remote_public_key, open_connection  }  => {
                                match self.traverse_nat(remote_addr, remote_public_key).await {
                                    Err(error) => {
                                        tracing::error!(%error, ?remote_addr, "Failed to establish connection");
                                    }
                                    Ok(connection_info) => {
                                        let (outbound_sender, outbound_receiver) = mpsc::channel(1);
                                        let (inbound_sender, inbound_recv) = mpsc::channel(1);
                                        self.connection_raw_packet_senders.insert(remote_addr, (connection_info, inbound_sender));
                                        let _ = open_connection.send(Ok((outbound_sender, inbound_recv)));
                                    }
                                }
                            }
                        }
                    }
                },
            }
        }
    }

    async fn traverse_nat(
        &mut self,
        remote_addr: SocketAddr,
        remote_public_key: TransportPublicKey,
    ) -> Result<ConnectionInfo, TransportError> {
        enum ConnectionState {
            Start,
            AckConnection,
        }
        // todo: probably should use exponential backoff with an upper limit: `timeout`
        let timeout = Duration::from_secs(5);

        // todo: probably instead of a fixed interval we should monotonically increase the interval
        // until we reach a maximum, and then just keep trying at that maximum interval
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(200));
        const MAX_FAILURES: usize = 20;
        let mut failures = 0;
        let mut packet = [0u8; MAX_PACKET_SIZE];
        let mut state = ConnectionState::Start;

        let inbound_sym_key_bytes = rand::random::<[u8; 16]>();
        let mut inbound_sym_key = Aes128Gcm::new(&inbound_sym_key_bytes.into());
        let mut outbound_sym_key: Option<Aes128Gcm> = None;

        let outbound_intro_packet = {
            let mut data = [0u8; { 16 + PROTOC_VERSION.len() }];
            data[..PROTOC_VERSION.len()].copy_from_slice(&PROTOC_VERSION);
            data[PROTOC_VERSION.len()..].copy_from_slice(&inbound_sym_key_bytes);
            PacketData::<MAX_PACKET_SIZE>::encrypted_with_remote(&data, &remote_public_key)
        };

        // fixme: use typed messages instead of raw bytes
        let hello_packet = {
            const HELLO: &[u8; 5] = b"hello";
            PacketData::<MAX_PACKET_SIZE>::encrypted_with_cipher(HELLO, &mut inbound_sym_key)
        };

        while failures < MAX_FAILURES {
            match state {
                ConnectionState::Start => {
                    tracing::debug!("Sending protocol version to remote");
                    if let Err(error) = self
                        .socket
                        .send_to(outbound_intro_packet.send_data(), remote_addr)
                        .await
                    {
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
                        .send_to(hello_packet.send_data(), remote_addr)
                        .await?;
                }
            }
            match tokio::time::timeout(timeout, self.socket.recv_from(&mut packet)).await {
                Ok(Ok((size, response_remote))) => {
                    if response_remote != remote_addr {
                        todo!("is a different remote, handle this message");
                    }
                    match state {
                        ConnectionState::Start
                        // this is the other peer intro packer theoretically
                            if size == outbound_intro_packet.send_data().len() =>
                        {
                            // try to decrypt the message with the symmetric key
                            let Ok(data) = self.this_peer_keypair.secret.decrypt(&packet[..size]) else {
                                failures += 1;
                                tracing::debug!("Received unexpect packet from remote");
                                continue;
                            };
                            let mut key = Aes128Gcm::new_from_slice(&data[PROTOC_VERSION.len()..])
                                .map_err(|_| TransportError::ConnectionEstablishmentFailure {
                                    cause: "invalid symmetric key".into(),
                                })?;
                            let protocol_version = &data[..PROTOC_VERSION.len()];
                            if protocol_version != PROTOC_VERSION {
                                let packet = {
                                    let msg = SymmetricMessage {
                                        message_id: 0,
                                        confirm_receipt: None,
                                        payload: SymmetricMessagePayload::AckConnection {
                                            result: Err("remote is using a different protocol version".into()),
                                        },
                                    };
                                    let mut packet = [0u8; MAX_PACKET_SIZE];
                                    bincode::serialize_into(packet.as_mut_slice(), &msg)?;
                                    PacketData::<MAX_PACKET_SIZE>::encrypted_with_cipher(&packet[..], &mut key)
                                };
                                let _ = self.socket.send_to(packet.send_data(), remote_addr).await;
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
                        ConnectionState::Start => {
                            failures += 1;
                            tracing::debug!("Received unexpect response from remote");
                        }
                        ConnectionState::AckConnection => {
                            let packet: PacketData<MAX_PACKET_SIZE> = (std::mem::replace(&mut packet, [0; MAX_PACKET_SIZE]), size).into();
                            let decrypted = packet.decrypt(
                                outbound_sym_key
                                    .as_mut()
                                    .expect("should be set at this stage"),
                            ).unwrap();
                            let packet = bincode::deserialize::<SymmetricMessage>(decrypted.send_data())?;
                            if let SymmetricMessagePayload::AckConnection { result: Ok(key_bytes) } = packet.payload {
                                let  inbound_symmetric_key = Aes128Gcm::new(&key_bytes.into());
                                return Ok(ConnectionInfo {
                                    outbound_symmetric_key: outbound_sym_key
                                        .expect("should be set at this stage"),
                                    inbound_symmetric_key,
                                    remote_public_key,
                                    remote_is_gateway: false,
                                    remote_addr,
                                });
                            }
                                tracing::debug!("Received unrecognized message from remote");
                                return Err(TransportError::ConnectionEstablishmentFailure {
                                    cause: "received unrecognized message from remote".into(),
                                });
                        }
                        _ => {
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
            tick.tick().await;
        }
        Err(TransportError::ConnectionEstablishmentFailure {
            cause: "max connection attemps reached".into(),
        })
    }

    fn handle_unrecognized_remote(&mut self, _remote: SocketAddr) {
        tracing::warn!("Received unrecognized remote, ignoring");
    }
}

type PeerChannel = (mpsc::Sender<SerializedMessage>, mpsc::Receiver<PacketData>);

pub(super) enum ConnectionEvent {
    ConnectionStart {
        remote_public_key: TransportPublicKey,
        open_connection: oneshot::Sender<Result<PeerChannel, TransportError>>,
    },
    SendRawPacket(PacketData),
}

// Define a custom error type for the transport layer
#[derive(Debug, thiserror::Error)]
pub(super) enum TransportError {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("transport handler channel closed")]
    ChannelClosed(#[from] mpsc::error::SendError<(SocketAddr, ConnectionEvent)>),
    #[error("failed while establishing connection, reason: {cause}")]
    ConnectionEstablishmentFailure { cause: Cow<'static, str> },
    #[error(transparent)]
    DescryptionError(#[from] rsa::errors::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
}
