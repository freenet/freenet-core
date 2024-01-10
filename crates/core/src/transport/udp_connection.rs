use crate::transport::errors::ConnectionError;
use crate::transport::udp_transport::UdpTransport;
use crate::transport::{ConnectionEvent, SenderStream};
use aes::Aes128;
use libp2p_identity::PublicKey;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::task;

pub struct UdpConnection {
    transport: Arc<RwLock<UdpTransport>>,
    pub(in crate::transport) raw_packets: (
        mpsc::Sender<(SocketAddr, RawPacket)>,
        mpsc::Receiver<(SocketAddr, RawPacket)>,
    ),
    pub(in crate::transport) decrypted_packets: (
        mpsc::Sender<(SocketAddr, Vec<u8>)>,
        mpsc::Receiver<(SocketAddr, Vec<u8>)>,
    ),
    outbound_symmetric_key: Option<Aes128>,
    inbound_symmetric_key: Option<Aes128>,
    remote_is_gateway: bool,
}

impl UdpConnection {
    pub(in crate::transport) async fn new(
        transport: Arc<RwLock<UdpTransport>>,
        remote_addr: SocketAddr,
        remote_public_key: PublicKey,
        remote_is_gateway: bool,
    ) -> Result<Self, ConnectionError> {
        todo!()
    }

    fn spawn_message_handler(&mut self) {
        task::spawn(async move {
            loop {
                tokio::select! {
                        Some((addr, message)) = self.raw_packets.1.recv() => {
                            // Handle the raw packet
                            self.handle_raw_packet(addr, message).await;
                        }
                        Some((addr, message)) = self.decrypted_packets.1.recv() => {
                            // Handle the decrypted packet
                            self.handle_decrypted_packet(addr, message).await;
                        }
                }
            }
        });
    }

    fn remote_ip_address(&self) -> IpAddr {
        todo!()
    }

    fn remote_public_key(&self) -> PublicKey {
        todo!()
    }

    fn remote_port(&self) -> u16 {
        todo!()
    }

    fn outbound_symmetric_key(&self) -> Vec<u8> {
        todo!()
    }

    fn inbound_symmetric_key(&self) -> Vec<u8> {
        todo!()
    }

    async fn read_event(&self) -> Result<ConnectionEvent, ConnectionError> {
        todo!()
    }

    async fn handle_raw_packet(&mut self, addr: SocketAddr, message: RawPacket) {
        match message {
            RawPacket::Message(data) => {
                // Decrypt the message
                let decrypted_message = self.decrypt_message(data);

                // Send the decrypted message to the decrypted packets channel
                if let Err(e) = self
                    .decrypted_packets
                    .0
                    .send((addr, decrypted_message))
                    .await
                {
                    tracing::warn!("Failed to send decrypted message: {:?}", e);
                }
            }
            RawPacket::Terminate => {
                // Remove the connection from the transport
                self.transport.write().await.connections.remove(&addr);
            }
        }
    }
}


enum RawPacket {
    Message(Vec<u8>),
    Terminate,
}
