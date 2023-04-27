extern crate core;

mod contract;
mod contract_store;
mod delegate;
mod delegate_store;
pub(crate) mod error;
mod native_api;
mod runtime;
mod secrets_store;
mod state_store;
mod store;
#[cfg(test)]
pub(crate) mod tests;
pub mod util;

type DynError = Box<dyn std::error::Error + Send + Sync>;

pub use locutus_stdlib;
pub use prelude::*;

pub mod prelude {
    pub use super::contract::ContractRuntimeInterface;
    pub use super::contract_store::ContractStore;
    pub use super::delegate::{DelegateExecError, DelegateRuntimeInterface};
    pub use super::delegate_store::DelegateStore;
    pub use super::error::ContractError;
    pub use super::error::RuntimeResult;
    pub use super::runtime::{ContractExecError, Runtime};
    pub use super::secrets_store::SecretsStore;
    pub use super::state_store::{StateStorage, StateStore, StateStoreError};
    pub use locutus_stdlib::prelude::*;
}
