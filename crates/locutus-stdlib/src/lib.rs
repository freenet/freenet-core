//! Standard library provided by the Freenet project to be able to write Locutus-compatible contracts.
pub mod buf;
mod component_interface;
pub mod contract_interface;
#[cfg(feature = "xz2")]
pub mod web;

/// Locutus stdlib prelude.
pub mod prelude {
    pub use super::WasmLinearMem;
    pub use crate::component_interface::wasm_interface::*;
    pub use crate::component_interface::*;
    pub use crate::contract_interface::wasm_interface::*;
    pub use crate::contract_interface::*;
    pub use locutus_macros::{component, contract};

    pub use bincode;
    pub use blake2;
    pub use env_logger;
    pub use log;
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct WasmLinearMem {
    pub start_ptr: *const u8,
    pub size: u64,
}
