use freenet_stdlib::prelude::{DelegateError, DelegateKey, SecretsId};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DelegateExecError {
    /// Guest-returned errors reach this variant through the `From` impl, which
    /// bounds the guest-supplied text. Constructing it directly (tests, mocks)
    /// bypasses that bound, so only do so with host-authored text.
    #[error(transparent)]
    DelegateError(DelegateError),

    #[error("Permission denied: secret {secret} cannot be accesed by {delegate} at this time")]
    UnauthorizedSecretAccess {
        secret: SecretsId,
        delegate: DelegateKey,
    },

    #[error("Received an unexpected message from the client apps: {0}")]
    UnexpectedMessage(&'static str),
}

impl From<DelegateError> for DelegateExecError {
    /// Delegate WASM chooses the text of the error it returns; bound it here so
    /// every log line and client reply built from this error is bounded.
    fn from(err: DelegateError) -> Self {
        Self::DelegateError(super::super::error::bound_delegate_error(err))
    }
}
