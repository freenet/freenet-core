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

use libp2p_identity::{Keypair, PublicKey};
use std::net::IpAddr;
use std::time::Duration;
use thiserror::Error;

// Define a custom error type for the transport layer
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("network error: {0}")]
    NetworkError(#[from] std::io::Error),

    #[error("connection error: {0}")]
    ConnectionError(#[from] ConnectionError),

    #[error("initialization error: {0}")]
    InitializationError(String),
}

// Define a custom error type for the connection
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
}

trait Transport<C: Connection> {
    fn new(
        keypair: Keypair,
        listen_port: u16,
        is_gateway: bool,
        max_upstream_rate : BytesPerSecond,
    ) -> Result<Self, TransportError>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn connect(
        &self,
        remote_public_key: PublicKey,
        remote_ip_address: IpAddr,
        remote_port: u16,
        remote_is_gateway: bool,
        timeout: Duration,
    ) -> Result<C, TransportError> {
        todo!()
    }

    fn update_max_upstream_rate(&self, max_upstream_rate : BytesPerSecond) {
        todo!()
    }

    async fn listen_for_connection(&self) -> Result<C, TransportError> {
        todo!()
    }
}

pub trait Connection {
    fn remote_ip_address(&self) -> IpAddr;

    fn remote_public_key(&self) -> PublicKey;

    fn remote_port(&self) -> u16;

    fn outbound_symmetric_key(&self) -> Vec<u8>;

    fn inbound_symmetric_key(&self) -> Vec<u8>;

    async fn read_event(&self) -> Result<ConnectionEvent, ConnectionError>;

    async fn send_short_message(&self, message: Vec<u8>) -> Result<(), ConnectionError>;

    async fn send_streamed_message(&self, message_length : usize) -> Result<SenderStream, ConnectionError>;
}

enum SendMessageError {
    MessageTooBig { max_size: usize },
    Closed,
}

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

pub(crate) enum SenderStreamError {
    Closed,
}

pub(crate) struct BytesPerSecond(f64);

impl BytesPerSecond {
    pub fn new(bytes_per_second: f64) -> Self {
        assert!(bytes_per_second >= 0.0);
        Self(bytes_per_second)
    }
}