use super::*;
use crate::node::PeerId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::vec::Vec;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task;

use super::{
    connection::Connection,
    crypto::{TransportKeypair, TransportPublicKey},
};

/// The maximum size of a received UDP packet, MTU tipically is 1500
/// so this should be more than enough.
pub(super) const MAX_PACKET_SIZE: usize = 2048;

pub(super) type ConnectionHandlerMessage = (SocketAddr, Vec<u8>);

pub(crate) struct ConnectionHandler {
    connection_info: HashMap<PeerId, Connection>,
    keypair: TransportKeypair,
    listen_port: u16,
    is_gateway: bool,
    max_upstream_rate: BytesPerSecond,
    // todo: don't think we need to set in a second task and we can manage all this
    // with FuturesUnordered and concurrently handling all the connections,
    // but revisit this when the code is a bit more mature and see if it's the case
    send_queue: mpsc::Sender<(SocketAddr, PacketData)>,
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
        socket_addr: SocketAddr,
        remote_is_gateway: bool,
        timeout: std::time::Duration,
    ) -> Result<(), TransportError> {
        // let key = random::<[u8; 16]>();
        // let outbound_sym_key: Aes128 =
        //     Aes128::new_from_slice(&key).map_err(|e| TransportError(e.to_string()))?;

        // let intro_packet = remote_public_key
        //     .encrypt(&key)
        //     .map_err(|e| TransportError(e.to_string()))?;
        todo!("attempt establishing connection; build a `Connection` instance and save it")
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
        self.send_queue.send((connection.remote_addr, data)).await?;
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
    send_queue: mpsc::Receiver<(SocketAddr, PacketData)>,
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
                    if let Some((ip_addr, data)) = &send_message {
                        // if let Err(e) = self.socket.send_to(&data, ip_addr).await {
                        //     tracing::warn!("Failed to send UDP packet: {:?}", e);
                        // }
                    }
                },
            }
        }
    }

    fn handle_unrecognized_message(&mut self, message: (SocketAddr, PacketData)) {
        tracing::warn!("Received unrecognized message, ignoring");
    }
}

#[derive(Debug)]
enum InternalMessage {
    UdpPacketReceived { source: SocketAddr, data: Vec<u8> },
}

// Define a custom error type for the transport layer
#[derive(Debug, thiserror::Error)]
pub(super) enum TransportError {
    #[error("missing peer: {0}")]
    MissingPeer(PeerId),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("transport handler channel closed")]
    ChannelClosed(#[from] mpsc::error::SendError<(SocketAddr, PacketData)>),
}
