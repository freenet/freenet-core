#![allow(dead_code)] // TODO: Remove before integration

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

pub(crate) mod errors;
mod udp_transport;
mod udp_connection;
mod crypto;

use errors::*;

enum ConnectionEvent {
    /// A short message that can fit in a single UDP packet.
    Message(Vec<u8>),
    /// A message that is streamed over multiple UDP packets.
    StreamedMessage(ReceiverStream),
    Disconnected,
}

struct ReceiverStream {}

impl ReceiverStream {
    async fn read_part(&self) -> Result<StreamedMessagePart, ConnectionError> {
        todo!()
    }
}

pub(crate) struct StreamedMessagePart {
    data: Vec<u8>,
    part_start_position: usize,
    message_size: usize,
}

pub(crate) struct SenderStream {}

impl SenderStream {
    /// Will block until the message is sent, data must fit in a single UDP packet.
    async fn send_part(&self, data: Vec<u8>) -> Result<(), SenderStreamError> {
        todo!()
    }
}

pub(crate) struct BytesPerSecond(f64);

impl BytesPerSecond {
    pub fn new(bytes_per_second: f64) -> Self {
        assert!(bytes_per_second >= 0.0);
        Self(bytes_per_second)
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }
}
