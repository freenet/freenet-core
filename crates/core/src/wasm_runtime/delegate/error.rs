use freenet_stdlib::prelude::{DelegateError, DelegateKey, SecretsId};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DelegateExecError {
    #[error(transparent)]
    DelegateError(#[from] DelegateError),

    #[error("Permission denied: secret {secret} cannot be accesed by {delegate} at this time")]
    UnauthorizedSecretAccess {
        secret: SecretsId,
        delegate: DelegateKey,
    },

    #[error("Received an unexpected message from the client apps: {0}")]
    UnexpectedMessage(&'static str),
}
