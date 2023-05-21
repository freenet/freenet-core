//! A node client API. Intended to be used from applications (web or otherwise) using the
//! node capabilities to execute contract, delegate, etc. instructions and communicating
//! over the network.
//!
//! Communication, independent of the transport, revolves around the [`ClientRequest`]
//! and [`HostResponse`] types.
//!
//! Currently the clients available are:
//! - `websocket`:
//!   - `regular` (native): Using TCP transport directly, for native applications programmed in Rust.
//!   - `browser` (wasm): Via wasm-bindgen (and by extension web-sys).
//!               (In order to use this client from JS/Typescript refer to the Typescript std lib).
mod client_events;

#[cfg(all(target_family = "unix", feature = "net"))]
mod regular;
#[cfg(all(target_family = "unix", feature = "net"))]
pub use regular::*;

#[cfg(all(
    target_arch = "wasm32",
    target_vendor = "unknown",
    target_os = "unknown",
    feature = "net"
))]
mod browser;
#[cfg(all(
    target_arch = "wasm32",
    target_vendor = "unknown",
    target_os = "unknown",
    feature = "net"
))]
pub use browser::*;

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
    #[cfg(all(target_family = "unix", feature = "net"))]
    #[error(transparent)]
    ConnectionError(#[from] tokio_tungstenite::tungstenite::Error),
    #[cfg(all(target_family = "wasm", feature = "net"))]
    #[error("request error: {0}")]
    ConnectionError(serde_json::Value),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("unhandled error: {0}")]
    OtherError(Box<dyn std::error::Error + Send + Sync>),
}

pub trait TryFromTsStd<T>: Sized {
    fn try_decode(value: T) -> Result<Self, WsApiError>;
}

#[derive(thiserror::Error, Debug)]
pub enum WsApiError {
    #[error("Failed decoding msgpack message from client request: {cause}")]
    MsgpackDecodeError { cause: String },
    #[error("Unsupported contract version")]
    UnsupportedContractVersion,
    #[error("Failed unpacking contract container")]
    UnpackingContractContainerError(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl WsApiError {
    pub fn deserialization(cause: String) -> Self {
        Self::MsgpackDecodeError { cause }
    }
}
