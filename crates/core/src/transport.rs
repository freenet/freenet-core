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

mod bw;
mod connection_handler;
mod crypto;
mod packet_data;
mod peer_connection;
mod received_packet_tracker;
mod sent_packet_tracker;
mod symmetric_message;

type MessagePayload = Vec<u8>;
type MessageId = u32;

use self::packet_data::PacketData;
use std::time::Duration;

/// We can wait up to 100ms to confirm a message was received, this allows us to batch
/// receipts together and send them in a single message.
const MAX_CONFIRMATION_DELAY: Duration = Duration::from_millis(100);

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
