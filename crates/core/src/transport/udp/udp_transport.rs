//! UDP transport implementation
//!
//! # Protocol
//!
//! ## Connection Establishment
//!
//! ### Neither peer is a gateway
//!
//!
//!
//! # Transport message types (u8)
//!
//! * 0: Symmetric key encrypted with our public key
//! * 1: Acknowledgement of symmetric key - encrypted with symmetric key
//! * 2: Message - encrypted with symmetric key
//! * 3: Disconnect message - encrypted with symmetric key

use super::*;
use crate::transport::crypto::{TransportKeypair, TransportPublicKey};
use crate::transport::udp::udp_connection::UdpConnection;
use crate::transport::udp::udp_transport::InternalMessage::UdpPacketReceived;
use aes::cipher::KeyInit;
use aes::Aes128;
use bytes::{Buf, Bytes, BytesMut};
use rand::random;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::task;
use tracing::Value;

/// The maximum size of a received UDP packet, MTU is 1500
/// so this should be more than enough.
const MAX_PACKET_SIZE: usize = 2048;

const RECEIVE_QUEUE_SIZE: usize = 100;
const SEND_QUEUE_SIZE: usize = 100;

pub(crate) struct UdpTransport {
    connections: DashMap<SocketAddr, UdpConnection>,
    keypair: TransportKeypair,
    listen_port: u16,
    is_gateway: bool,
    max_upstream_rate: BytesPerSecond,
    send_queue: (
        mpsc::Sender<(IpAddr, Vec<u8>)>,
        mpsc::Receiver<(IpAddr, Vec<u8>)>,
    ),
}

impl Transport<UdpConnection> for UdpTransport {
    async fn new(
        keypair: TransportKeypair,
        listen_port: u16,
        is_gateway: bool,
        max_upstream_rate: BytesPerSecond,
    ) -> Result<Arc<RwLock<Self>>, TransportError>
    where
        Self: Sized,
    {
        // Bind the UDP socket to the specified port
        let socket = UdpSocket::bind(("0.0.0.0", listen_port))
            .await
            .map_err(|e| TransportError::NetworkError(e))?;

        let new_transport = Arc::new(RwLock::new(UdpTransport {
            connections: DashMap::new(),
            keypair,
            listen_port,
            is_gateway,
            max_upstream_rate,
            send_queue: mpsc::channel(SEND_QUEUE_SIZE),
        }));

        let transport_clone = new_transport.clone();

        // Spawn a task for listening to incoming UDP packets
        task::spawn(async move {
            loop {
                // TODO: Potentially inefficient to allocate a new buffer for every
                //       received packet, consider some kind of buffer pool.
                let mut buf = vec![0u8; MAX_PACKET_SIZE];

                match socket.recv_from(&mut buf).await {
                    Ok((size, addr)) => {
                        buf.truncate(size);

                        let message = (addr.ip(), buf);

                        // Handle the message (existing logic)
                        match transport_clone.read().await.connections.get(&addr) {
                            Some(connection) => {
                                if let Err(e) = connection.receive_queue.0.send(message).await {
                                    tracing::warn!("Failed to send message: {:?}", e);
                                }
                            }
                            None => {
                                transport_clone
                                    .read()
                                    .await
                                    .handle_unrecognized_message(message);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to receive UDP packet: {:?}", e);
                    }
                }
            }
        });

        Ok(new_transport)
    }

    async fn connect(
        &self,
        remote_public_key: TransportPublicKey,
        remote_ip_address: IpAddr,
        remote_port: u16,
        remote_is_gateway: bool,
        timeout: std::time::Duration,
    ) -> Result<UdpConnection, TransportError> {
        let key = random::<[u8; 16]>();
        let outbound_sym_key: Aes128;
        unsafe {
            outbound_sym_key = Aes128::new_from_slice(&key)
                .map_err(|e| TransportError::CryptoError(e.to_string()))?;
        }

        let intro_packet = remote_public_key
            .encrypt(&key)
            .map_err(|e| TransportError::CryptoError(e.to_string()))?;

        todo!()
    }

    fn update_max_upstream_rate(&self, max_upstream_rate: BytesPerSecond) {
        todo!()
    }

    async fn listen_for_connection(&self) -> Result<UdpConnection, TransportError> {
        todo!()
    }
}

impl UdpTransport {
    fn handle_unrecognized_message(&self, message: (IpAddr, Vec<u8>)) {
        if !self.is_gateway {
            tracing::warn!(
                "Received unrecognized message, ignoring because not a gateway {:?}",
                message
            );
        } else {
            match &message {
                (source, data) => {
                    tracing::debug!(
                        "Received unrecognized message, attempting to parse {:?}",
                        message
                    );

                    // use self.keypair to decrypt the message, which should contain a symmetric key
                    todo!()
                }
                _ => {
                    tracing::warn!("Received unrecognized message, ignoring {:?}", message);
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum InternalMessage {
    UdpPacketReceived { source: SocketAddr, data: Vec<u8> },
}
