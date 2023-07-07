//! Standard library provided by the Freenet project to be able to write Locutus-compatible contracts.
#[doc(hidden)]
pub mod buf;
#[cfg(all(feature = "net", any(unix, windows, target_family = "wasm")))]
pub mod client_api;
mod contract_interface;
mod bincode_interface;
mod delegate_interface;
pub(crate) mod global;
#[cfg(target_family = "wasm")]
pub mod time;
mod versioning;
#[cfg(feature = "archive")]
pub mod web;

/// Locutus stdlib prelude.
pub mod prelude {
    pub use super::WasmLinearMem;
    pub use crate::contract_interface::wasm_interface::*;
    pub use crate::contract_interface::*;
    pub use crate::delegate_interface::wasm_interface::*;
    pub use crate::delegate_interface::*;
    pub use crate::versioning::*;
    pub use locutus_macros::{component, contract};

    pub use bincode;
    pub use blake2;
    pub use semver::Version;
    pub use tracing;
    pub use tracing_subscriber;
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct WasmLinearMem {
    pub start_ptr: *const u8,
    pub size: u64,
}
