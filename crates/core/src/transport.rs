#![allow(dead_code)] // TODO: Remove before integration
//! Freenet Transport protocol implementation.
//!
//! # Transport
//!
//! The transport layer is responsible for reliably sending and receiving messages
//! over the network.
//!
//! ## Message Streaming
//!
//! The transport layer supports two types of messages:
//!
//! - Short messages that can fit in a single UDP packet.
//! - Streamed messages that are split into multiple UDP packets.
//!
//! The purpose of streamed messages is to allow a node to start sending a message before
//! it has been received completely (although it must know the message size before starting
//! to send).
//!
//! ## Congestion Control
//!
//! The transport layer implements a simple congestion control algorithm which assumes
//! that congestion won't occur if the upstream rate is less than `max_upstream_rate`.
//! Choosing an appropriate and conservative value for `max_upstream_rate` is therefore
//! important to avoid congestion.
//!
//! ## Encryption
//!
//! Each peer chooses a symmetric key that is used to encrypt *inbound* messages for that peer,
//! the exception is inbound connections to the gateway peer which will use the key provided
//! by the peer initiating the connection in both directions.
//!
//! Each peer initiates a connection by encrypting its chosen key with the public key of the
//! peer it is connecting to. The encrypted key is then sent to the peer in the first message
//! of the connection, repeated until a correctly encrypted response is received. The peer
//! receiving the message will decrypt the key and use it to encrypt future messages.
//!
//! ## Opening a Connection
//!
//! ### Neither peer is a gateway
//!
//! 1. Peer A sends a `ConnectionStart` message to Peer B with its chosen symmetric key
//!    encrypted with Peer B's public key, resending every 200ms until...
//! 2. Peer B receives the message and decrypts the symmetric key, it then sends a `ConnectionAck`
//!    message to Peer A encrypted with the symmetric key.
//! 3. Peer B stores the `ConnectionStart` and `ConnectionAck` messages in [UdpConnection] and
//!    if its sees that message again it resends the `ConnectionAck` message.

mod bw;
mod connection_handler;
mod connection_info;
mod crypto;
mod symmetric_message;

use aes_gcm::{
    aead::{generic_array::GenericArray, AeadMut},
    aes::cipher::Unsigned,
    AeadCore, AeadInPlace, Aes128Gcm,
};

use self::{
    connection_handler::MAX_PACKET_SIZE, connection_info::ConnectionError,
    crypto::TransportPublicKey,
};

struct ReceiverStream {}

impl ReceiverStream {
    /// Will await until a full message is received, does error handling, reassembling the message from parts, decryption, etc.
    async fn receive_message(&self) -> Result<Vec<u8>, ConnectionError> {
        todo!()
    }

    async fn read_part(&self) -> Result<StreamedMessagePart, ConnectionError> {
        todo!()
    }
}

struct StreamedMessagePart {
    data: PacketData,
    part_start_position: usize,
    message_size: usize,
}

// todo: split this into type for handling inbound (encrypted)/outbound (decrypted) packets for clarity
struct PacketData<const N: usize = MAX_PACKET_SIZE> {
    data: [u8; N],
    size: usize,
}

// FIXME: this **MUST** be different for every packet, but it doesn't need to be randomly generated
//        for every packet, for example it's fine to increment it by 1 for every packet. In future
//        we can take advantage of this to avoid sending the entire 12 bytes of the nonce in every
//        packet, but any such mechanism **must** be resistant to packet loss.
static NONCE: [u8; 12] = [0; 12];

impl<const N: usize> PacketData<N> {
    fn encrypted_with_cipher(data: &[u8], cipher: &mut Aes128Gcm) -> Self {
        debug_assert!(data.len() <= MAX_PACKET_SIZE - <Aes128Gcm as AeadCore>::TagSize::to_usize());
        let mut buffer = [0u8; N];
        let payload: aes_gcm::aead::Payload = data.into();
        let encrypted = cipher.encrypt(&NONCE.into(), data).unwrap();
        let tag = cipher
            .encrypt_in_place_detached(&NONCE.into(), payload.aad, &mut buffer[..data.len()])
            .unwrap();
        let tag_size = <Aes128Gcm as AeadCore>::TagSize::to_usize();
        buffer[..tag_size].copy_from_slice(tag.as_slice());
        buffer[tag_size..tag_size + encrypted.len()].copy_from_slice(&encrypted[..]);
        Self {
            data: buffer,
            size: encrypted.len(),
        }
    }

    fn encrypted_with_remote(data: &[u8], remote_key: &TransportPublicKey) -> Self {
        let encrypted_data: Vec<u8> = remote_key.encrypt(data);
        debug_assert!(encrypted_data.len() <= MAX_PACKET_SIZE);
        let mut data = [0; N];
        data.copy_from_slice(&encrypted_data[..]);
        Self {
            data,
            size: encrypted_data.len(),
        }
    }

    fn send_data(&self) -> &[u8] {
        &self.data[..self.size]
    }

    fn decrypt(&self, inbound_sym_key: &mut Aes128Gcm) -> Result<Self, aes_gcm::Error> {
        let tag_size = <Aes128Gcm as AeadCore>::TagSize::to_usize();
        debug_assert!(self.data.len() <= MAX_PACKET_SIZE + tag_size);
        let mut buffer = [0u8; N];
        // inbound_sym_key.decrypt_in_place(&NONCE.into(), encrypted_data, &mut buffer[..])?;
        let payload: aes_gcm::aead::Payload = self.data.as_slice().into();
        // let _d = inbound_sym_key.decrypt(&NONCE.into(), &self.data[..]);
        let tag = GenericArray::from_slice(&self.data[..tag_size]);
        inbound_sym_key.decrypt_in_place_detached(
            &NONCE.into(),
            payload.aad,
            &mut buffer[..self.data[tag_size..].len()],
            tag,
        )?;
        Ok(Self {
            data: self.data,
            size: self.data.len(),
        })
    }
}

struct SenderStream {}

impl SenderStream {
    /// Will await until the message is sent, handles breaking the message into parts, encryption, etc.
    async fn send_message(&self, _data: &[u8]) -> Result<(), SenderStreamError> {
        todo!()
    }
}

struct BytesPerSecond(f64);

impl BytesPerSecond {
    pub fn new(bytes_per_second: f64) -> Self {
        assert!(bytes_per_second >= 0.0);
        Self(bytes_per_second)
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SenderStreamError {
    #[error("stream closed unexpectedly")]
    Closed,
    #[error("message too big, size: {size}, max size: {max_size}")]
    MessageExceedsLength { size: usize, max_size: usize },
}
