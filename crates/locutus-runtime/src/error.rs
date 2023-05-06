use std::fmt::Display;

use locutus_stdlib::prelude::{ContractKey, DelegateKey};

use crate::{delegate, runtime, secrets_store};

pub type RuntimeResult<T> = std::result::Result<T, ContractError>;

#[derive(Debug)]
pub struct ContractError(Box<RuntimeInnerError>);

impl ContractError {
    pub fn is_contract_exec_error(&self) -> bool {
        matches!(&*self.0, RuntimeInnerError::ContractExecError(_))
    }

    pub fn is_delegate_exec_error(&self) -> bool {
        matches!(&*self.0, RuntimeInnerError::DelegateExecError(_))
    }
}

impl Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for ContractError {}

impl From<RuntimeInnerError> for ContractError {
    fn from(err: RuntimeInnerError) -> Self {
        Self(Box::new(err))
    }
}

macro_rules! impl_err {
    ($type:ty) => {
        impl From<$type> for ContractError {
            fn from(err: $type) -> Self {
                Self(Box::new(RuntimeInnerError::from(err)))
            }
        }
    };
}

impl_err!(Box<dyn std::error::Error + Send + Sync>);
impl_err!(locutus_stdlib::buf::Error);
impl_err!(std::io::Error);
impl_err!(secrets_store::SecretStoreError);
impl_err!(bincode::Error);
impl_err!(delegate::DelegateExecError);
impl_err!(runtime::ContractExecError);
#[cfg(test)]
impl_err!(wasmer_wasi::WasiStateCreationError);
#[cfg(test)]
impl_err!(wasmer_wasi::WasiError);
impl_err!(wasmer::CompileError);
impl_err!(wasmer::ExportError);
impl_err!(wasmer::InstantiationError);
impl_err!(wasmer::MemoryError);
impl_err!(wasmer::RuntimeError);

#[derive(thiserror::Error, Debug)]
pub(crate) enum RuntimeInnerError {
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

    // delegate runtime errors
    #[error("delegate {0} not found in store")]
    DelegateNotFound(DelegateKey),

    #[error(transparent)]
    DelegateExecError(#[from] delegate::DelegateExecError),

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
