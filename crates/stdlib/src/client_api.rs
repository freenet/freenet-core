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

#[cfg(all(any(unix, windows), feature = "net"))]
mod regular;
#[cfg(all(any(unix, windows), feature = "net"))]
pub use regular::*;

#[cfg(all(target_family = "wasm", feature = "net"))]
mod browser;
#[cfg(all(target_family = "wasm", feature = "net"))]
pub use browser::*;

pub use client_events::*;

#[cfg(feature = "net")]
type HostResult = Result<HostResponse, ClientError>;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Deserialization(#[from] bincode::Error),
    #[error("channel closed")]
    ChannelClosed,
    #[cfg(all(any(unix, windows), feature = "net"))]
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

pub trait TryFromFbs<T>: Sized {
    fn try_decode_fbs(value: T) -> Result<Self, WsApiError>;
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum WsApiError {
    #[error("Unsupported contract version")]
    UnsupportedContractVersion,
    #[error("Failed unpacking contract container")]
    UnpackingContractContainerError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Failed decoding message from client request: {cause}")]
    DeserError { cause: String },
}

impl WsApiError {
    pub fn deserialization(cause: String) -> Self {
        Self::DeserError { cause }
    }

    pub fn into_fbs_bytes(self) -> Vec<u8> {
        use crate::host_response_generated::host_response::{
            finish_host_response_buffer, Error, ErrorArgs, HostResponse, HostResponseArgs,
            HostResponseType,
        };
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let as_msg = format!("{self}");
        let msg_offset = builder.create_string(&as_msg);
        let err_offset = Error::create(
            &mut builder,
            &ErrorArgs {
                msg: Some(msg_offset),
            },
        );
        let res = HostResponse::create(
            &mut builder,
            &HostResponseArgs {
                response_type: HostResponseType::Error,
                response: Some(err_offset.as_union_value()),
            },
        );
        finish_host_response_buffer(&mut builder, res);
        builder.finished_data().to_vec()
    }
}
