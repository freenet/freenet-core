mod contract;
mod interface;

pub use contract::{Contract, ContractKey, ContractValue};
pub use interface::ContractRuntime;

#[derive(thiserror::Error, Debug)]
pub enum ContractRuntimeError {
    #[error("failed while unwrapping contract to raw bytes")]
    UnwrapContract,
}
