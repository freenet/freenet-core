mod contract;
mod contract_store;
mod runtime;
mod state_store;

type DynError = Box<dyn std::error::Error + Send + Sync>;

pub use prelude::*;

pub mod prelude {
    pub use super::contract::{WrappedContract, WrappedState};
    pub use super::contract_store::ContractStore;
    pub use super::runtime::{ExecError, Runtime, RuntimeInterface};
    pub use super::state_store::{StateStorage, StateStore, StateStoreError};
    pub use super::RuntimeResult;
    pub use locutus_stdlib::prelude::*;
}

pub type RuntimeResult<T> = std::result::Result<T, ContractRuntimeError>;

#[derive(thiserror::Error, Debug)]
pub enum ContractRuntimeError {
    #[error(transparent)]
    BufferError(#[from] BufferError),

    #[error("contract {0} not found in store")]
    ContractNotFound(ContractKey),

    #[error(transparent)]
    ExecError(#[from] runtime::ExecError),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("failed while unwrapping contract to raw bytes")]
    UnwrapContract,

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
