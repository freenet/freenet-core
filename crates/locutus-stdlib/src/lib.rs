//! Standard library provided by the Freenet project to be able to write Locutus-compatible contracts.
pub mod buf;
pub mod interface;
#[cfg(feature = "xz2")]
pub mod web;

pub use blake2;
/// Generate the necessary code for the WASM runtime to interact with your contract ergonomically and safely.
pub use locutus_macros::contract;

/// Locutus stdlib prelude.
pub mod prelude {
    pub use crate::interface::*;
    pub use locutus_macros::contract;
}
