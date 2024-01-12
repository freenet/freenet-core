use crate::transport::udp_transport::UdpTransport;
use crate::transport::ConnectionEvent;
use aes_gcm::{
    aead::{generic_array::GenericArray, Aead},
    Aes128Gcm,
};
use libp2p_identity::PublicKey;
use rand::rngs::OsRng;
use rand::RngCore;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task;

/*
 NOTES:
    The receiver thread should be set up when the channel is created, it shouldn't be stored
    in the struct because only the receiver thread loop should be able to access it.


*/

pub struct UdpConnectionInfo {
    outbound_symmetric_key: Option<Aes128Gcm>,
    inbound_symmetric_key: Option<Aes128Gcm>,
    inbound_intro_packet: Option<Vec<u8>>,
    outbound_intro_packet: Option<Vec<u8>>,
    remote_public_key: Option<PublicKey>,
    remote_is_gateway: bool,
}

impl UdpConnectionInfo {
    pub(in crate::transport) async fn new(
        transport: Arc<RwLock<UdpTransport>>,
        remote_addr: SocketAddr,
        remote_public_key: PublicKey,
        remote_is_gateway: bool,
    ) -> Result<Self, ConnectionError> {
        let mut connection = Self {
            transport,
            raw_packets: PacketQueue::new(),
            decrypted_packets: PacketQueue::new(),
            outbound_symmetric_key: None,
            inbound_symmetric_key: None,
            inbound_intro_packet: None,
            outbound_intro_packet: None,
            remote_public_key: Some(remote_public_key),
            remote_is_gateway,
        };

        task::spawn(async move {
            loop {
                tokio::select! {
                    Some((addr, message)) = raw_packets_receiver_clone.recv() => {
                        // Handle the raw packet
                        Self::handle_raw_packet(&addr, &message).await;
                    },
                    Some((addr, message)) = decrypted_packets_receiver_clone.recv() => {
                        // Handle the decrypted packet
                        Self::handle_decrypted_packet(&addr, &message).await;
                    },
                }
            }
        });

        Ok(connection)
    }

    async fn handle_raw_packet(addr: &SocketAddr, message: &RawPacket) {
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

    async fn handle_decrypted_packet(addr: &SocketAddr, message: &Vec<u8>) {
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

pub struct PacketQueue<T> {
    pub sender: Arc<RwLock<mpsc::Sender<(SocketAddr, T)>>>,
    pub receiver: Arc<RwLock<mpsc::Receiver<(SocketAddr, T)>>>,
}

impl<T> PacketQueue<T> {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self {
            sender: Arc::new(RwLock::new(sender)),
            receiver: Arc::new(RwLock::new(receiver)),
        }
    }

    fn sender_clone(&self) -> Arc<RwLock<mpsc::Sender<(SocketAddr, T)>>> {
        self.sender.clone()
    }

    fn receiver_clone(&self) -> Arc<RwLock<mpsc::Receiver<(SocketAddr, T)>>> {
        self.receiver.clone()
    }
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
