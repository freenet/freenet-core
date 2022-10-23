//! Standard library provided by the Freenet project to be able to write Locutus-compatible contracts.
pub mod buf;
pub mod interface;
#[cfg(feature = "xz2")]
pub mod web;

pub use blake2;
pub use locutus_macros::contract;

/// Locutus stdlib prelude.
pub mod prelude {
    pub use crate::interface::wasm_interface::*;
    pub use crate::interface::*;
    pub use locutus_macros::contract;

    pub use bincode;
    pub use env_logger;
    pub use log;
}
