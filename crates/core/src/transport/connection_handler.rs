use super::*;
use crate::node::PeerId;
use aes_gcm::{aes::Aes128, KeyInit};
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::vec::Vec;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task;

use super::{
    connection_info::ConnectionInfo,
    crypto::{TransportKeypair, TransportPublicKey},
};

/// The maximum size of a received UDP packet, MTU tipically is 1500
/// so this should be more than enough.
// todo: probably reduce this to 1500? since we are using this for breaking up messages etc.
pub(super) const MAX_PACKET_SIZE: usize = 2048;

pub(super) type ConnectionHandlerMessage = (SocketAddr, Vec<u8>);

pub(crate) struct ConnectionHandler {
    connection_info: HashMap<PeerId, ConnectionInfo>,
    keypair: TransportKeypair,
    listen_port: u16,
    is_gateway: bool,
    max_upstream_rate: BytesPerSecond,
    // todo: don't think we need to set in a second task and we can manage all this
    // with FuturesUnordered and concurrently handling all the connections,
    // but revisit this when the code is a bit more mature and see if it's the case
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
        };
        let connection_handler = ConnectionHandler {
            connection_info: HashMap::new(),
            keypair,
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
        peer_id: PeerId,
        remote_public_key: TransportPublicKey,
        remote_socket_addr: SocketAddr,
        remote_is_gateway: bool,
        timeout: std::time::Duration,
    ) -> Result<(), TransportError> {
        /*
        Transport message types when establishing connection
        * 0: Symmetric key encrypted with our public key
        * 1: Acknowledgement of symmetric key - encrypted with symmetric key
        * 2: Message - encrypted with symmetric key
        * 3: Disconnect message - encrypted with symmetric key
        */
        let key = rand::random::<[u8; 16]>();
        let outbound_sym_key: Aes128 = Aes128::new_from_slice(&key).expect("valid length");
        // todo: how large is this `intro_packet`, 16 bytes too? if it fits in a single UDP packet
        let encrypted_key: Vec<u8> = remote_public_key.encrypt(&key);
        let intro_packet = {
            // fixme, this assetion would fail now
            // ideally we want protoc version + encrypted key to fit into a packet
            debug_assert!(encrypted_key.len() <= MAX_PACKET_SIZE - std::mem::size_of::<u16>());
            let mut data = [0; MAX_PACKET_SIZE];
            data.copy_from_slice(&encrypted_key[..]);
            PacketData::from_bytes(data, encrypted_key.len())
        };

        if !remote_is_gateway {
            self.nat_traversal(remote_socket_addr, intro_packet).await?;
        } else {
            todo!()
        }

        todo!("attempt establishing connection; build a `ConnectionInfo` instance and save it")
    }

    pub(super) const PROTOC_VERSION: u16 = 0;

    async fn nat_traversal(
        &self,
        remote_socket: SocketAddr,
        intro_packet: PacketData,
    ) -> Result<(), TransportError> {
        self.send_queue
            .send((
                remote_socket,
                ConnectionEvent::ConnectionStart { intro_packet },
            ))
            .await?;
        Ok(())
    }

    /// Method used by users of connection handler to send a message.
    /// Message partitioning, encryption, etc. is handled internally.
    pub async fn send_message(
        &self,
        peer_id: &PeerId,
        message: Vec<u8>,
    ) -> Result<(), TransportError> {
        todo!()
    }

    /// Method used by users of connection handler to receive any incoming messages.
    pub async fn receive_message(
        &mut self,
        peer_id: &PeerId,
    ) -> Result<(PeerId, Vec<u8>), TransportError> {
        todo!()
    }

    async fn send_raw_packet(
        &self,
        peer_id: &PeerId,
        data: PacketData,
    ) -> Result<(), TransportError> {
        let connection = self
            .connection_info
            .get(peer_id)
            .ok_or_else(|| TransportError::MissingPeer(*peer_id))?;
        self.send_queue
            .send((connection.remote_addr, ConnectionEvent::SendRawPacket(data)))
            .await?;
        Ok(())
    }

    fn update_max_upstream_rate(&mut self, max_upstream_rate: BytesPerSecond) {
        self.max_upstream_rate = max_upstream_rate;
    }

    fn handle_unrecognized_message(&self, (_socket, data): (SocketAddr, PacketData)) {
        if !self.is_gateway {
            tracing::warn!(
                packet = ?&*data,
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
    connection_raw_packet_senders: HashMap<SocketAddr, mpsc::Sender<(SocketAddr, PacketData)>>,
    send_queue: mpsc::Receiver<(SocketAddr, ConnectionEvent)>,
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
                            let packet_data = PacketData::from_bytes(std::mem::replace(&mut buf, [0; MAX_PACKET_SIZE]), size);
                            let message: (SocketAddr, PacketData) = (addr, packet_data);
                            match self.connection_raw_packet_senders.get(&addr) {
                                Some(sender) => {
                                    if let Err(e) = sender.send(message).await {
                                        tracing::warn!("Failed to send raw packet to connection sender: {:?}", e);
                                    }
                                }
                                None => {
                                    self
                                        .handle_unrecognized_message(message);
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
                    if let Some((socket, event)) = send_message {
                        match event {
                            ConnectionEvent::SendRawPacket(data) => {
                                if let Err(e) = self.socket.send_to(&data, socket).await {
                                    tracing::warn!("Failed to send UDP packet: {:?}", e);
                                }
                            }
                            ConnectionEvent::ConnectionStart { intro_packet } => {
                                // todo: repeat each 200ms and control for an Ack packet comming back
                                if let Err(e) = self.socket.send_to(&intro_packet, socket).await {
                                    tracing::warn!("Failed to send UDP packet: {:?}", e);
                                }
                            }
                        }
                    }
                },
            }
        }
    }

    fn handle_unrecognized_message(&mut self, message: (SocketAddr, PacketData)) {
        tracing::warn!("Received unrecognized message, ignoring");
    }
}

enum ConnectionEvent {
    SendRawPacket(PacketData),
    ConnectionStart { intro_packet: PacketData },
    // UdpPacketReceived { source: SocketAddr, data: Vec<u8> },
}

// Define a custom error type for the transport layer
#[derive(Debug, thiserror::Error)]
pub(super) enum TransportError {
    #[error("missing peer: {0}")]
    MissingPeer(PeerId),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("transport handler channel closed")]
    ChannelClosed(#[from] mpsc::error::SendError<(SocketAddr, ConnectionEvent)>),
}

#[test]
fn check_size() {
    let pair = super::crypto::TransportKeypair::new();
    let encrypted = pair.public.encrypt(&[0; 16]);
    eprintln!("encrypted: {:?}", encrypted.len());
}
