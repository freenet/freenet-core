use crate::node::OpManager;
use crate::operations::OpError;
use freenet_stdlib::client_api::RequestError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Node not connected to network")]
    Disconnected,
    /// Registration of a subscriber notifier was rejected (e.g. the
    /// per-contract subscriber limit was reached). Carries the underlying
    /// `RequestError` so the detail reaches the client instead of being
    /// flattened into the generic `UnexpectedOpState` (#4681).
    #[error("subscriber registration rejected: {0}")]
    Registration(Box<RequestError>),
    #[error("Peer has not joined the network yet (no ring location established)")]
    PeerNotJoined,
    #[error("No ring connections found")]
    EmptyRing,
    #[error("Node error: {0}")]
    Node(String),
    #[error(transparent)]
    Contract(#[from] crate::contract::ContractError),
    #[error(transparent)]
    Op(#[from] OpError),
    #[error(transparent)]
    Executor(#[from] crate::contract::ExecutorError),
    #[error(transparent)]
    Panic(#[from] tokio::task::JoinError),
}

impl From<crate::ring::RingError> for Error {
    fn from(err: crate::ring::RingError) -> Self {
        match err {
            crate::ring::RingError::PeerNotJoined => Error::PeerNotJoined,
            crate::ring::RingError::EmptyRing => Error::EmptyRing,
            other @ crate::ring::RingError::ConnError(_)
            | other @ crate::ring::RingError::NoHostingPeers(_) => Error::Node(other.to_string()),
        }
    }
}

/// Check that the peer has completed network join before allowing operations.
/// For gateways: always ready (their address is set from config).
/// For regular peers: must wait for handshake to complete (peer_ready flag).
pub(crate) fn ensure_peer_ready(op_manager: &OpManager) -> Result<std::net::SocketAddr, Error> {
    if !op_manager.is_gateway
        && !op_manager
            .peer_ready
            .load(std::sync::atomic::Ordering::SeqCst)
    {
        return Err(Error::PeerNotJoined);
    }
    Ok(op_manager.ring.connection_manager.peer_addr()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::client_api::ContractError as StdContractError;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

    /// #4681 regression: a rejected subscriber registration must surface the
    /// underlying `RequestError` detail to the client, not the generic
    /// "unexpected op state" that the old fake-success path collapsed to. The
    /// `client_event_handling` catch-all wraps the `Error`'s `Display` into the
    /// client-facing `OperationError { cause }`, so the detail MUST appear in
    /// the `Error::Registration` display.
    #[test]
    fn registration_error_display_carries_underlying_detail() {
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([7u8; 32]),
            CodeHash::new([0u8; 32]),
        );
        let inner = RequestError::ContractError(StdContractError::Subscribe {
            key,
            cause: "subscriber limit (100) reached for contract".into(),
        });
        let err = Error::Registration(Box::new(inner));
        let rendered = err.to_string();
        assert!(
            rendered.contains("subscriber limit"),
            "the registration error must carry the underlying cause, got: {rendered}"
        );
        // Must NOT collapse to the generic op-state message the old path used.
        assert_ne!(
            rendered,
            Error::Op(OpError::UnexpectedOpState).to_string(),
            "registration failure must not degrade to the generic UnexpectedOpState message"
        );
    }
}
