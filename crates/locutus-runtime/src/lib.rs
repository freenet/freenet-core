mod contract;
mod interface;
mod rt;
mod store;

pub use contract::{Contract, ContractKey, ContractValue};
pub use interface::{ContractUpdateError, RuntimeInterface};
pub use rt::Runtime;
pub use store::{ContractStore, ContractStoreError};

#[derive(thiserror::Error, Debug)]
pub enum ContractRuntimeError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("failed while unwrapping contract to raw bytes")]
    UnwrapContract,

    #[error(transparent)]
    UpdateError(#[from] ContractUpdateError),

    #[error(transparent)]
    WasmCompileError(#[from] wasmer::CompileError),
}
