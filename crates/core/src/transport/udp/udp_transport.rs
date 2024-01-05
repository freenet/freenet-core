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
use crate::transport::udp::udp_connection::UdpConnection;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::task;

pub(crate) struct UdpTransport {
    connections: DashMap<SocketAddr, UdpConnection>,
    channel: (
        mpsc::Sender<InternalMessage>,
        mpsc::Receiver<InternalMessage>,
    ),
    keypair: Keypair,
    listen_port: u16,
    is_gateway: bool,
    max_upstream_rate: BytesPerSecond,
}

impl Transport<UdpConnection> for UdpTransport {
    async fn new(
        keypair: Keypair,
        listen_port: u16,
        is_gateway: bool,
        max_upstream_rate: BytesPerSecond,
    ) -> Result<Self, TransportError>
    where
        Self: Sized,
    {
        // Bind the UDP socket to the specified port
        let socket = UdpSocket::bind(("0.0.0.0", listen_port))
            .await
            .map_err(|e| TransportError::NetworkError(e))?;

        let (tx, rx) = mpsc::channel(100); // Adjust the channel size as needed

        // Clone the socket handle for the async task
        let socket_clone = socket.clone();

        let new_transport = UdpTransport {
            connections: DashMap::new(),
            channel: (tx, rx),
            keypair,
            listen_port,
            is_gateway,
            max_upstream_rate,
        };

        // Spawn a task for listening to incoming UDP packets
        task::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024); // Initial capacity
            loop {
                // Ensure the buffer has space
                buf.reserve(1024 - buf.len());

                match socket_clone.recv_from(&mut buf.chunk_mut()).await {
                    Ok((size, addr)) => {
                        unsafe {
                            buf.advance(size);
                        }

                        // Create an InternalMessage
                        let message = InternalMessage::UdpPacketReceived {
                            source: addr,
                            data: buf.split().freeze(),
                        };

                        match new_transport.connections[&addr] {
                            Some(connection) => {
                                // Send the message to the connection
                                if let Err(e) = connection.channel.0.send(message).await {
                                    tracing::warn!("Failed to send message: {:?}", e);
                                }
                            }
                            None => {
                                // Send the message to the transport
                                if let Err(e) = new_transport.channel.0.send(message).await {
                                    tracing::warn!("Failed to send message: {:?}", e);
                                }
                            }
                        }
                    }
                }

                // Clear the buffer for the next packet
                buf.clear();
            }
        });

        Ok(new_transport)
    }

    async fn connect(
        &self,
        remote_public_key: PublicKey,
        remote_ip_address: IpAddr,
        remote_port: u16,
        remote_is_gateway: bool,
        timeout: Duration,
    ) -> Result<UdpConnection, TransportError> {
        todo!()
    }

    fn update_max_upstream_rate(&self, max_upstream_rate: BytesPerSecond) {
        todo!()
    }

    async fn listen_for_connection(&self) -> Result<UdpConnection, TransportError> {
        todo!()
    }
}

enum InternalMessage {
    UdpPacketReceived { source: SocketAddr, data: Bytes },
}
