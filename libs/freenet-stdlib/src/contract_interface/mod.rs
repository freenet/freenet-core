//! Interface and related utilities for interaction with the compiled WASM contracts.
//! Contracts have an isomorphic interface which partially maps to this interface,
//! allowing interaction between the runtime and the contracts themselves.
//!
//! This abstraction layer shouldn't leak beyond the contract handler.

pub(crate) const CONTRACT_KEY_SIZE: usize = 32;

mod code;
mod contract;
pub mod encoding;
mod error;
mod key;
mod state;
mod trait_def;
mod update;
pub(crate) mod wasm_interface;
mod wrapped;

#[cfg(all(test, any(unix, windows)))]
mod tests;

// Re-export all public types
pub use code::ContractCode;
pub use contract::Contract;
pub use error::ContractError;
pub use key::{ContractInstanceId, ContractKey};
pub use state::{State, StateDelta, StateSummary};
pub use trait_def::ContractInterface;
pub use update::{
    RelatedContract, RelatedContracts, RelatedMode, UpdateData, UpdateModification, ValidateResult,
};
pub use wrapped::{WrappedContract, WrappedState};
