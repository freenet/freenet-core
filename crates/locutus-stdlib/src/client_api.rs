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

#[cfg(target_family = "unix")]
mod regular;
#[cfg(target_family = "unix")]
pub use regular::*;

#[cfg(all(
    target_arch = "wasm32",
    target_vendor = "unknown",
    target_os = "unknown"
))]
mod browser;
#[cfg(all(
    target_arch = "wasm32",
    target_vendor = "unknown",
    target_os = "unknown"
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
