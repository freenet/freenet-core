//! Standard library provided by the Freenet project to be able to write Locutus-compatible contracts.
#[doc(hidden)]
pub mod buf;
#[cfg(all(
    feature = "net",
    any(
        all(
            target_arch = "wasm32",
            target_vendor = "unknown",
            target_os = "unknown"
        ),
        target_family = "unix"
    )
))]
pub mod client_api;
mod component_interface;
mod contract_interface;
pub(crate) mod global;
pub mod rand;
#[cfg(feature = "time")]
pub mod time;
mod versioning;
#[cfg(feature = "archive")]
pub mod web;

/// Locutus stdlib prelude.
pub mod prelude {
    pub use super::WasmLinearMem;
    pub use crate::component_interface::wasm_interface::*;
    pub use crate::component_interface::*;
    pub use crate::contract_interface::wasm_interface::*;
    pub use crate::contract_interface::*;
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
