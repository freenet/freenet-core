use aes_gcm::{
    aead::{generic_array::GenericArray, Aead},
    Aes128Gcm,
};
use libp2p_identity::PublicKey;
use rand::rngs::OsRng;
use rand::RngCore;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::sync::mpsc;

use super::PacketData;

/*
 NOTES:
    The receiver thread should be set up when the channel is created, it shouldn't be stored
    in the struct because only the receiver thread loop should be able to access it.
*/

type ConnectionHandlerMessage = (SocketAddr, Vec<u8>);

// todo: maybe makes more sense to switch naming here? this should be FreenetProtocol
// or FreenetConnection since it's our own custom transport protocol over UDP
// and what we are calling UdpTransport
// should be called FreenetConnectionHandler since it's what is doing
pub(super) struct UdpConnection {
    outbound_symmetric_key: Option<Aes128Gcm>,
    inbound_symmetric_key: Option<Aes128Gcm>,
    inbound_intro_packet: Option<Vec<u8>>,
    outbound_intro_packet: Option<Vec<u8>>,
    remote_public_key: Option<PublicKey>,
    remote_is_gateway: bool,
    remote_addr: SocketAddr,
    connection_handler_sender: mpsc::Sender<ConnectionHandlerMessage>,
}

impl UdpConnection {
    pub(super) async fn new(
        remote_addr: SocketAddr,
        remote_public_key: PublicKey,
        remote_is_gateway: bool,
        connection_handler_sender: mpsc::Sender<ConnectionHandlerMessage>,
    ) -> Result<Self, ConnectionError> {
        let connection = Self {
            outbound_symmetric_key: None,
            inbound_symmetric_key: None,
            inbound_intro_packet: None,
            outbound_intro_packet: None,
            remote_public_key: Some(remote_public_key),
            remote_is_gateway,
            remote_addr,
            connection_handler_sender,
        };
        Ok(connection)
    }

    async fn handle_raw_packet(&self, data: PacketData) {
        // Decrypt the message
        let decrypted_message = self.decrypt_message(data).unwrap();

        // Send the decrypted message to the decrypted packets channel
        if let Err(e) = self
            .connection_handler_sender
            .send((self.remote_addr, decrypted_message))
            .await
        {
            tracing::warn!("Failed to send decrypted message: {:?}", e);
        }
    }

    async fn terminate(&self) {
        todo!()
    }

    // Encrypts the data and prepends the nonce to the ciphertext
    fn encrypt_message(&self, data: &[u8]) -> Result<Vec<u8>, ConnectionError> {
        let mut nonce = [0u8; 12]; // 12 bytes nonce for AES-GCM
        OsRng.fill_bytes(&mut nonce);

        let cipher = self
            .outbound_symmetric_key
            .as_ref()
            .ok_or(ConnectionError::ProtocolError(
                "Don't have outbound symmetric key".to_string(),
            ))?;
        let encrypted_data = cipher
            .encrypt(GenericArray::from_slice(&nonce), data)
            .map_err(ConnectionError::AesGcmError)?;

        // Prepend the nonce to the ciphertext
        let mut result = Vec::with_capacity(nonce.len() + encrypted_data.len());
        result.extend_from_slice(&nonce);
        result.extend_from_slice(&encrypted_data);

        Ok(result)
    }

    // Decrypts the data, assuming the nonce is prepended to the ciphertext
    fn decrypt_message(&self, data: PacketData) -> Result<Vec<u8>, ConnectionError> {
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
            .as_ref()
            .ok_or(ConnectionError::ProtocolError(
                "Don't have inbound symmetric key".to_string(),
            ))?;
        cipher
            .decrypt(GenericArray::from_slice(nonce), ciphertext)
            .map_err(ConnectionError::AesGcmError)
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
