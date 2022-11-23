mod client_events;

#[cfg(target_family = "unix")]
mod regular;
#[cfg(target_family = "unix")]
pub use regular::*;

#[cfg(target_family = "wasm")]
mod wasm;
#[cfg(target_family = "wasm")]
pub use wasm::*;

pub use client_events::*;

type HostResult = Result<HostResponse, ClientError>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Deserialization(#[from] rmp_serde::decode::Error),
    #[error(transparent)]
    Serialization(#[from] rmp_serde::encode::Error),
    #[error("channel closed")]
    ChannelClosed,
    #[cfg(target_family = "unix")]
    #[error(transparent)]
    ConnectionError(#[from] tokio_tungstenite::tungstenite::Error),
    #[cfg(target_family = "wasm")]
    #[error("request error: {0}")]
    ConnectionError(serde_json::Value),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("unhandled error: {0}")]
    OtherError(Box<dyn std::error::Error + Send + Sync>),
}
