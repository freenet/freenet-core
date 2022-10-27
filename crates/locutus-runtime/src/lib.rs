mod component;
mod component_store;
mod contract;
mod contract_store;
mod runtime;
mod secrets_store;
mod state_store;

type DynError = Box<dyn std::error::Error + Send + Sync>;

pub use locutus_stdlib;
pub use prelude::*;

pub mod prelude {
    pub use super::contract::{ContractRuntimeInterface, WrappedContract, WrappedState};
    pub use super::contract_store::ContractStore;
    pub use super::runtime::{ContractExecError, Runtime};
    pub use super::secrets_store::SecretsStore;
    pub use super::state_store::{StateStorage, StateStore, StateStoreError};
    pub use super::RuntimeResult;
    pub use locutus_stdlib::prelude::*;
}

pub type RuntimeResult<T> = std::result::Result<T, ContractRuntimeError>;

#[derive(thiserror::Error, Debug)]
pub enum ContractRuntimeError {
    #[error(transparent)]
    Any(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    BufferError(#[from] locutus_stdlib::buf::Error),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    SecretStoreError(#[from] secrets_store::SecretStoreError),

    #[error(transparent)]
    Serialization(#[from] bincode::Error),

    // component runtime errors
    #[error("component {0} not found in store")]
    ComponentNotFound(ComponentKey),

    #[error(transparent)]
    ComponentExecError(#[from] component::ComponentExecError),

    // contract runtime  errors
    #[error("contract {0} not found in store")]
    ContractNotFound(ContractKey),

    #[error(transparent)]
    ContractExecError(#[from] runtime::ContractExecError),

    #[error("failed while unwrapping contract to raw bytes")]
    UnwrapContract,

    // wasm runtime errors
    #[cfg(test)]
    #[error(transparent)]
    WasiEnvError(#[from] wasmer_wasi::WasiStateCreationError),

    #[cfg(test)]
    #[error(transparent)]
    WasiError(#[from] wasmer_wasi::WasiError),

    #[error(transparent)]
    WasmCompileError(#[from] wasmer::CompileError),

    #[error(transparent)]
    WasmExportError(#[from] wasmer::ExportError),

    #[error(transparent)]
    WasmInstantiationError(#[from] wasmer::InstantiationError),

    #[error(transparent)]
    WasmMemError(#[from] wasmer::MemoryError),

    #[error(transparent)]
    WasmRtError(#[from] wasmer::RuntimeError),
}
