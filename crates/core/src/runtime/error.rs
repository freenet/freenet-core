use std::fmt::Display;

use freenet_stdlib::prelude::{ContractKey, DelegateKey, SecretsId};

use crate::DynError;

use super::{delegate, secrets_store, wasm_runtime, DelegateExecError};

pub type RuntimeResult<T> = std::result::Result<T, ContractError>;

#[derive(thiserror::Error, Debug)]
pub struct ContractError(Box<RuntimeInnerError>);

impl ContractError {
    pub fn is_contract_exec_error(&self) -> bool {
        matches!(&*self.0, RuntimeInnerError::ContractExecError(_))
    }

    pub fn is_delegate_exec_error(&self) -> bool {
        matches!(&*self.0, RuntimeInnerError::DelegateExecError(_))
    }

    pub fn delegate_auth_access(&self) -> Option<&SecretsId> {
        match &*self.0 {
            RuntimeInnerError::DelegateExecError(DelegateExecError::UnauthorizedSecretAccess {
                secret,
                ..
            }) => Some(secret),
            _ => None,
        }
    }

    pub fn is_execution_error(&self) -> bool {
        use RuntimeInnerError::*;
        matches!(
            &*self.0,
            WasmInstantiationError(_) | WasmExportError(_) | WasmCompileError(_)
        )
    }

    pub fn delegate_is_missing(&self) -> bool {
        matches!(&*self.0, RuntimeInnerError::DelegateNotFound(_))
    }

    pub fn secret_is_missing(&self) -> bool {
        matches!(
            &*self.0,
            RuntimeInnerError::SecretStoreError(secrets_store::SecretStoreError::MissingSecret(_))
        )
    }

    pub fn get_secret_id(&self) -> SecretsId {
        match &*self.0 {
            RuntimeInnerError::SecretStoreError(
                secrets_store::SecretStoreError::MissingSecret(id),
            ) => id.clone(),
            _ => panic!(),
        }
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

impl_err!(Box<dyn std::error::Error + Send + Sync>);
impl_err!(freenet_stdlib::memory::buf::Error);
impl_err!(std::io::Error);
impl_err!(secrets_store::SecretStoreError);
impl_err!(bincode::Error);
impl_err!(delegate::DelegateExecError);
impl_err!(wasm_runtime::ContractExecError);
impl_err!(wasmer::CompileError);
impl_err!(wasmer::ExportError);
impl_err!(wasmer::InstantiationError);
impl_err!(wasmer::MemoryError);
impl_err!(wasmer::RuntimeError);

#[derive(thiserror::Error, Debug)]
pub(crate) enum RuntimeInnerError {
    #[error(transparent)]
    Any(#[from] DynError),

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

    // contract runtime  errors
    #[error("contract {0} not found in store")]
    ContractNotFound(ContractKey),

    #[error(transparent)]
    ContractExecError(#[from] wasm_runtime::ContractExecError),

    #[error("failed while unwrapping contract to raw bytes")]
    UnwrapContract,

    // wasm runtime errors
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
