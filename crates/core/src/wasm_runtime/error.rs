use std::fmt::Display;

use freenet_stdlib::prelude::{ContractKey, DelegateKey};

use super::{delegate, engine, runtime, secrets_store};

pub type RuntimeResult<T> = std::result::Result<T, ContractError>;

#[derive(thiserror::Error, Debug)]
pub struct ContractError(Box<RuntimeInnerError>);

impl ContractError {
    pub(crate) fn deref(&self) -> &RuntimeInnerError {
        &self.0
    }
}

impl Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

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

impl_err!(anyhow::Error);
impl_err!(freenet_stdlib::memory::buf::Error);
impl_err!(std::io::Error);
impl_err!(secrets_store::SecretStoreError);
impl_err!(bincode::Error);
impl_err!(delegate::DelegateExecError);
impl_err!(runtime::ContractExecError);
impl_err!(engine::WasmError);

#[derive(thiserror::Error, Debug)]
pub(crate) enum RuntimeInnerError {
    #[error(transparent)]
    Any(#[from] anyhow::Error),

    #[error(transparent)]
    BufferError(#[from] freenet_stdlib::memory::buf::Error),

    #[error("{0}")]
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

    // contract runtime errors
    #[error("contract {0} not found in store")]
    ContractNotFound(ContractKey),

    #[error(transparent)]
    ContractExecError(#[from] runtime::ContractExecError),

    // wasm engine errors (all backend-specific errors unified here)
    #[error(transparent)]
    WasmError(#[from] engine::WasmError),
}
