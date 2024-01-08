use serde_with::SerializeDisplay;
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

    #[error("crypto error: {0}")]
    CryptoError(String),
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

#[derive(Debug, Error)]
enum SendMessageError {
    #[error("message too big, size: {size}, max size: {max_size}")]
    MessageTooBig { size: usize, max_size: usize },
    #[error("stream closed unexpectedly")]
    Closed,
}

#[derive(Debug, Error)]
pub(crate) enum SenderStreamError {
    #[error("stream closed unexpectedly")]
    Closed,
    #[error("message too big, size: {size}, max size: {max_size}")]
    MessageExceedsLength { size: usize, max_size: usize },
}
