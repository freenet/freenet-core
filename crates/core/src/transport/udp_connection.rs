use crate::transport::errors::ConnectionError;
use crate::transport::udp_transport::UdpTransport;
use crate::transport::ConnectionEvent;
use aes_gcm::{
    aead::{generic_array::GenericArray, Aead},
    Aes128Gcm,
};
use libp2p_identity::PublicKey;
use rand::rngs::OsRng;
use rand::RngCore;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use thiserror::Error;
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
    outbound_symmetric_key: Option<Aes128Gcm>,
    inbound_symmetric_key: Option<Aes128Gcm>,
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
                            self.handle_raw_packet(&addr, &message).await;
                        }
                        Some((addr, message)) = self.decrypted_packets.1.recv() => {
                            // Handle the decrypted packet
                            self.handle_decrypted_packet(&addr, &message).await;
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

    async fn handle_raw_packet(&mut self, addr: &SocketAddr, message: &RawPacket) {
        match message {
            RawPacket::Message(data) => {
                // Decrypt the message
                let decrypted_message = self.decrypt_message(data).unwrap();

                // Send the decrypted message to the decrypted packets channel
                if let Err(e) = self
                    .decrypted_packets
                    .0
                    .send((addr.clone(), decrypted_message))
                    .await
                {
                    tracing::warn!("Failed to send decrypted message: {:?}", e);
                }
            }
            RawPacket::Terminate => {
                todo!()
            }
        }
    }

    async fn handle_decrypted_packet(
        &mut self,
        addr: &SocketAddr,
        message: &Vec<u8>,
    ) -> Result<(), ConnectionError> {
        todo!()
    }

    // Encrypts the data and prepends the nonce to the ciphertext
    fn encrypt_message(&self, data: &[u8]) -> Result<Vec<u8>, ConnectionError> {
        let mut nonce = [0u8; 12]; // 12 bytes nonce for AES-GCM
        OsRng.fill_bytes(&mut nonce);

        let cipher = self
            .outbound_symmetric_key
            .ok_or(ConnectionError::ProtocolError(
                "Don't have outbound symmetric key".to_string(),
            ))?;
        let encrypted_data = cipher
            .encrypt(GenericArray::from_slice(&nonce), data)
            .map_err(|e| ConnectionError::AesGcmError(e))?;

        // Prepend the nonce to the ciphertext
        let mut result = Vec::with_capacity(nonce.len() + encrypted_data.len());
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&encrypted_data);

        Ok(result)
    }

    // Decrypts the data, assuming the nonce is prepended to the ciphertext
    fn decrypt_message(&self, data: &[u8]) -> Result<Vec<u8>, ConnectionError> {
        // Extract the nonce from the beginning of the data
        if data.len() < 12 {
            return Err(ConnectionError::ProtocolError(
                "Data is too short to contain nonce".to_string(),
            ));
        }
        let nonce = &data[..12];
        let ciphertext = &data[12..];

        let cipher = self
            .inbound_symmetric_key
            .ok_or(ConnectionError::ProtocolError(
                "Don't have inbound symmetric key".to_string(),
            ))?;
        cipher
            .decrypt(GenericArray::from_slice(nonce), ciphertext)
            .map_err(|e| ConnectionError::AesGcmError(e))
    }
}

enum RawPacket {
    Message(Vec<u8>),
    Terminate,
}

#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("timeout occurred")]
    Timeout,

    #[error("message too big, size: {size}, max size: {max_size}")]
    MessageTooBig { size: usize, max_size: usize },

    #[error("stream closed unexpectedly")]
    Closed,

    #[error("protocol error: {0}")]
    ProtocolError(String),

    #[error("aes-gcm error: {0}")]
    AesGcmError(aes_gcm::Error),
}
