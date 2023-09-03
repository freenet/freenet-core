//! Standard library provided by the Freenet project to be able to write Locutus-compatible contracts.
#[doc(hidden)]
pub mod buf;
mod code_hash;
mod contract_interface;
mod delegate_interface;
pub(crate) mod global;
mod parameters;
mod versioning;

#[allow(dead_code, unused_imports, clippy::all)]
#[path = "../../../target/flatbuffers/client_request_generated.rs"]
pub(crate) mod client_request_generated;
#[allow(dead_code, unused_imports, clippy::all)]
#[path = "../../../target/flatbuffers/common_generated.rs"]
pub(crate) mod common_generated;
#[allow(dead_code, unused_imports, clippy::all)]
#[path = "../../../target/flatbuffers/host_response_generated.rs"]
pub(crate) mod host_response_generated;

pub mod client_api;
#[cfg(all(feature = "log", target_family = "wasm"))]
pub mod log;
pub mod rand;
#[cfg(all(feature = "time", target_family = "wasm"))]
pub mod time;
#[cfg(feature = "archive")]
pub mod web;

/// Locutus stdlib prelude.
pub mod prelude {
    pub use super::WasmLinearMem;
    pub use crate::code_hash::*;
    pub use crate::contract_interface::wasm_interface::*;
    pub use crate::contract_interface::*;
    pub use crate::delegate_interface::wasm_interface::*;
    pub use crate::delegate_interface::*;
    pub use crate::parameters::*;
    pub use crate::versioning::*;
    pub use locutus_macros::{contract, delegate};

    pub use bincode;
    pub use blake3;
    pub use serde_json;
    pub use tracing;
    pub use tracing_subscriber;
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct WasmLinearMem {
    pub start_ptr: *const u8,
    pub size: u64,
}
