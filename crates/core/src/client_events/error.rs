use crate::node::OpManager;
use crate::operations::OpError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Node not connected to network")]
    Disconnected,
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
