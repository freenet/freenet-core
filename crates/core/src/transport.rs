#![allow(dead_code)] // TODO: Remove before integration

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

    #[error("message too big, max size: {max_size}")]
    MessageTooBig { max_size: usize },

    #[error("stream closed unexpectedly")]
    Closed,

    #[error("protocol error: {0}")]
    ProtocolError(String),
}

trait Transport<C: Connection> {
    fn new(keypair: Keypair, listen_port: u16, is_gateway: bool) -> Result<Self, TransportError>
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

    async fn listen_for_connection(&self) -> Result<C, TransportError> {
        todo!()
    }
}

pub trait Connection {
    async fn remote_ip_address(&self) -> IpAddr;

    async fn remote_public_key(&self) -> PublicKey;

    async fn remote_port(&self) -> u16;

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

struct StreamedMessagePart {
    data: Vec<u8>,
    part_start_position: usize,
    message_size: usize,
}

struct SenderStream {}

impl SenderStream {
    /// Will block until the message is sent, data must fit in a single UDP packet.
    async fn send(&self, data: Vec<u8>) -> Result<(), SenderStreamError> {
        todo!()
    }
}

enum SenderStreamError {
    Closed,
}
