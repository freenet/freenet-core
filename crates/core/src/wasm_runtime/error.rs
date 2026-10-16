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

/// The text a refused delegate run reports, followed by a space and the
/// delegate key (`RuntimeInnerError::DelegateParamsMismatch`). Clients receive
/// it inside `DelegateError::ExecutionError`.
pub(crate) const DELEGATE_PARAMS_MISMATCH: &str =
    "delegate request parameters do not match the delegate key";

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

    /// The parameters supplied for a delegate run do not derive its key
    /// under the code hash this node indexed for it. Refused before the
    /// delegate runs; see `Runtime::prepare_delegate_call`.
    #[error("{msg} {0}", msg = DELEGATE_PARAMS_MISMATCH)]
    DelegateParamsMismatch(DelegateKey),

    /// A delegate offered for storage carries a key that is not derived from
    /// its own code and parameters, so the node cannot store it under an
    /// identity it has verified. See `DelegateStore::verify_delegate_identity`.
    ///
    /// This is the delegate sibling of [`Self::ContractIdentityMismatch`]. It is
    /// a separate variant rather than a shared one because the two carry
    /// different key types, and a caller matching on the error wants to know
    /// which store refused.
    #[error(
        "delegate {key} identity does not match its code: {detail} \
         (the key a delegate is stored under must be derived from its bytes)"
    )]
    DelegateIdentityMismatch {
        key: Box<DelegateKey>,
        detail: String,
    },

    #[error(transparent)]
    DelegateExecError(#[from] delegate::DelegateExecError),

    // contract runtime errors
    #[error("contract {0} not found in store")]
    ContractNotFound(ContractKey),

    /// A contract offered for storage carries a key that is not derived from
    /// its own code and parameters, so the node cannot store it under an
    /// identity it has verified. See `ContractStore::verify_contract_identity`.
    #[error(
        "contract {key} identity does not match its code: {detail} \
         (the key a contract is stored under must be derived from its bytes)"
    )]
    ContractIdentityMismatch {
        key: Box<ContractKey>,
        detail: String,
    },

    #[error(transparent)]
    ContractExecError(#[from] runtime::ContractExecError),

    // wasm engine errors (all backend-specific errors unified here)
    #[error(transparent)]
    WasmError(#[from] engine::WasmError),
}
