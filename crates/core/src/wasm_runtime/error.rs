use std::fmt::Display;

use freenet_stdlib::prelude::{ContractKey, DelegateError, DelegateKey};

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

/// The text a refused `UnregisterDelegate` reports, followed by a space and
/// the delegate key (`RuntimeInnerError::DelegateUnregisterNotLocal`). Clients
/// receive it inside `DelegateError::ExecutionError`.
pub(crate) const DELEGATE_UNREGISTER_NOT_LOCAL: &str =
    "only a local client may unregister a delegate";

/// The text a refused `UnregisterDelegate` reports on a node running in hosted
/// mode when the client presented no user token, followed by a space and the
/// delegate key
/// (`RuntimeInnerError::DelegateUnregisterHosted`). Clients receive it inside
/// `DelegateError::ExecutionError`.
pub(crate) const DELEGATE_UNREGISTER_HOSTED: &str =
    "a hosted node unregisters a delegate only for a client with a user token";

/// The text a delegate run refused on a hosted node reports when the run has
/// no user, followed by a space and the delegate key
/// (`RuntimeInnerError::DelegateHostedRunWithoutUser`). Clients receive it
/// inside `DelegateError::ExecutionError`.
pub(crate) const DELEGATE_HOSTED_RUN_WITHOUT_USER: &str =
    "a hosted node runs a delegate only on behalf of a user";

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

    /// An `UnregisterDelegate` from a connection that is not local. Refused
    /// before anything is removed; see the `UnregisterDelegate` arm of
    /// `Executor::delegate_request`.
    #[error("{msg} {0}", msg = DELEGATE_UNREGISTER_NOT_LOCAL)]
    DelegateUnregisterNotLocal(DelegateKey),

    /// An `UnregisterDelegate` from a client of a node running in hosted mode
    /// that presented no user token. Refused before anything is removed; see
    /// the `UnregisterDelegate` arm of `Executor::delegate_request`.
    #[error("{msg} {0}", msg = DELEGATE_UNREGISTER_HOSTED)]
    DelegateUnregisterHosted(DelegateKey),

    /// An `ApplicationMessages` run with no user on a node running in hosted
    /// mode. On a hosted node every delegate run belongs to a user, so a run
    /// without one is refused before the delegate runs; see the
    /// `ApplicationMessages` arm of `Executor::delegate_request_as`.
    #[error("{msg} {0}", msg = DELEGATE_HOSTED_RUN_WITHOUT_USER)]
    DelegateHostedRunWithoutUser(DelegateKey),

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

/// Longest guest-supplied text kept in a host error, in bytes.
///
/// Contract and delegate WASM choose the text of the errors they return, and
/// the host logs those errors (several times, to more than one sink) and
/// forwards them to clients. Without a cap, the only bound is the guest's
/// linear memory.
pub(crate) const MAX_GUEST_ERROR_TEXT_BYTES: usize = 1024;

/// Truncate guest-supplied text to [`MAX_GUEST_ERROR_TEXT_BYTES`] on a char
/// boundary, appending a marker that records the original length. Text already
/// within the limit is returned unchanged.
pub(crate) fn truncate_guest_text(mut text: String) -> String {
    if text.len() <= MAX_GUEST_ERROR_TEXT_BYTES {
        return text;
    }
    let original_len = text.len();
    let mut cut = MAX_GUEST_ERROR_TEXT_BYTES;
    while !text.is_char_boundary(cut) {
        cut -= 1;
    }
    text.truncate(cut);
    text.push_str(&format!("... [truncated, {original_len} bytes total]"));
    text
}

/// Bound every guest-supplied string in a contract error. This is the single
/// point where the stdlib error returned by contract WASM becomes a host
/// error (`ContractExecError`'s `From` impl).
pub(crate) fn bound_contract_error(
    err: freenet_stdlib::prelude::ContractError,
) -> freenet_stdlib::prelude::ContractError {
    use freenet_stdlib::prelude::ContractError as E;
    // `ContractError` is `#[non_exhaustive]`; variants without text pass through.
    #[allow(clippy::wildcard_enum_match_arm)]
    match err {
        E::Deser(s) => E::Deser(truncate_guest_text(s)),
        E::InvalidUpdateWithInfo { reason } => E::InvalidUpdateWithInfo {
            reason: truncate_guest_text(reason),
        },
        E::Other(s) => E::Other(truncate_guest_text(s)),
        other => other,
    }
}

/// Bound every guest-supplied string in a delegate error. This is the single
/// point where the stdlib error returned by delegate WASM becomes a host
/// error (`DelegateExecError`'s `From` impl).
pub(crate) fn bound_delegate_error(err: DelegateError) -> DelegateError {
    // `DelegateError` is `#[non_exhaustive]`.
    #[allow(clippy::wildcard_enum_match_arm)]
    match err {
        DelegateError::Deser(s) => DelegateError::Deser(truncate_guest_text(s)),
        DelegateError::Other(s) => DelegateError::Other(truncate_guest_text(s)),
        other => other,
    }
}

#[cfg(test)]
mod guest_text_tests {
    use super::*;
    use freenet_stdlib::prelude::ContractError as StdContractError;

    #[test]
    fn short_text_is_unchanged() {
        assert_eq!(truncate_guest_text("boom".into()), "boom");
        let exact = "a".repeat(MAX_GUEST_ERROR_TEXT_BYTES);
        assert_eq!(truncate_guest_text(exact.clone()), exact);
    }

    #[test]
    fn oversize_text_is_truncated_on_char_boundary() {
        // 3-byte chars: 1024 is not a multiple of 3, so a naive cut would split one.
        let big = "\u{20AC}".repeat(1_000_000);
        let out = truncate_guest_text(big);
        assert!(
            out.len() < MAX_GUEST_ERROR_TEXT_BYTES + 64,
            "len {}",
            out.len()
        );
        assert!(out.contains("[truncated, 3000000 bytes total]"));
    }

    #[test]
    fn contract_exec_error_from_guest_error_is_bounded() {
        let big = "x".repeat(10_000_000);
        for e in [
            StdContractError::Other(big.clone()),
            StdContractError::Deser(big.clone()),
            StdContractError::InvalidUpdateWithInfo {
                reason: big.clone(),
            },
        ] {
            let exec: super::runtime::ContractExecError = e.into();
            assert!(
                exec.to_string().len() < 2048,
                "len {}",
                exec.to_string().len()
            );
        }
        let short: super::runtime::ContractExecError =
            StdContractError::Other("nope".into()).into();
        assert_eq!(short.to_string(), "nope");
    }

    #[test]
    fn delegate_exec_error_from_guest_error_is_bounded() {
        let big = "y".repeat(10_000_000);
        for e in [
            DelegateError::Other(big.clone()),
            DelegateError::Deser(big.clone()),
        ] {
            let exec: delegate::DelegateExecError = e.into();
            assert!(
                exec.to_string().len() < 2048,
                "len {}",
                exec.to_string().len()
            );
        }
        let short: delegate::DelegateExecError = DelegateError::Other("nope".into()).into();
        assert_eq!(short.to_string(), "nope");
    }
}
