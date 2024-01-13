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
use crate::transport::udp_connection::UdpConnectionInfo;
use dashmap::DashMap;
use rand::random;
use std::net::SocketAddr;
use std::sync::Arc;
use std::vec::Vec;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, RwLock};
use tokio::task;

/// The maximum size of a received UDP packet, MTU is 1500
/// so this should be more than enough.
const MAX_PACKET_SIZE: usize = 2048;

const RECEIVE_QUEUE_SIZE: usize = 100;

pub(crate) struct UdpTransport {
    pub(super) connection_raw_packet_senders: DashMap<SocketAddr, Sender<(SocketAddr, Vec<u8>)>>,
    pub(super) connection_info: DashMap<SocketAddr, UdpConnectionInfo>,
    keypair: TransportKeypair,
    listen_port: u16,
    is_gateway: bool,
    max_upstream_rate: BytesPerSecond,
    send_queue: Sender<(SocketAddr, Vec<u8>)>,
}

impl UdpTransport {
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
            .map_err(|e| TransportError(e.to_string()))?;

        // Channel buffer is zero so senders will block until the receiver is ready,
        // important for bandwidth limiting
        let (send_queue, mut send_queue_receiver) = mpsc::channel(0);

        let new_transport = Arc::new(RwLock::new(UdpTransport {
            connection_raw_packet_senders: DashMap::new(),
            connection_info: DashMap::new(),
            keypair,
            listen_port,
            is_gateway,
            max_upstream_rate,
            send_queue,
        }));

        let new_transport_clone = Arc::clone(&new_transport);
        task::spawn(async move {
            loop {
                let mut buf = vec![0u8; MAX_PACKET_SIZE];
                tokio::select! {
                    recv_result = socket.recv_from(&mut buf) => {
                        match &recv_result {
                            Ok((size, addr)) => {
                                buf.truncate(*size);

                                let message: (SocketAddr, Vec<u8>) = (*addr, buf);

                                match new_transport_clone.read().await.connection_raw_packet_senders.get(&addr) {
                                    Some(e) => {
                                        if let Err(e) = e.value().send(message).await {
                                            tracing::warn!("Failed to send raw packet to connection sender: {:?}", e);
                                        }
                                    }
                                    None => {
                                        new_transport_clone
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
                    },

                    // Handling of outbound packets
                    send_message = send_queue_receiver.recv() => {
                        if let Some((ip_addr, data)) = &send_message {
                            if let Err(e) = socket.send_to(&data, ip_addr).await {
                                tracing::warn!("Failed to send UDP packet: {:?}", e);
                            }
                        }
                    },
                }
            }
        });

        Ok(new_transport)
    }

    async fn connect(
        &self,
        remote_public_key: TransportPublicKey,
        socket_addr: SocketAddr,
        remote_is_gateway: bool,
        timeout: std::time::Duration,
    ) -> Result<UdpConnectionInfo, TransportError> {
        let key = random::<[u8; 16]>();
        let outbound_sym_key: Aes128 =
            Aes128::new_from_slice(&key).map_err(|e| TransportError(e.to_string()))?;

        let intro_packet = remote_public_key
            .encrypt(&key)
            .map_err(|e| TransportError(e.to_string()))?;
    }

    pub async fn send_raw_message(
        &self,
        socket_addr: SocketAddr,
        data: Vec<u8>,
    ) -> Result<(), TransportError> {
        self.send_queue
            .send((socket_addr, data))
            .await
            .map_err(|e| TransportError(e.to_string()))?;
        Ok(())
    }

    fn update_max_upstream_rate(&mut self, max_upstream_rate: BytesPerSecond) {
        self.max_upstream_rate = max_upstream_rate;
    }

    async fn listen_for_connection(&self) -> Result<UdpConnectionInfo, TransportError> {
        todo!()
    }

    fn handle_unrecognized_message(&self, message: (SocketAddr, Vec<u8>)) {
        if !self.is_gateway {
            tracing::warn!(
                "Received unrecognized message, ignoring because not a gateway {:?}",
                message
            );
        } else {
            match &message {
                (source, data) => {
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
