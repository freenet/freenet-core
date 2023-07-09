//! A node client API. Intended to be used from applications (web or otherwise) using the
//! node capabilities to execute contract, component, etc. instructions and communicating
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

#[cfg(any(unix, windows))]
mod regular;
#[cfg(any(unix, windows))]
pub use regular::*;

#[cfg(target_family = "wasm")]
mod browser;
#[cfg(target_family = "wasm")]
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
    #[cfg(any(unix, windows))]
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
