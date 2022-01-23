//! Types and definitions to handle all socket communication for the peer nodes.

use libp2p::swarm::ProtocolsHandlerUpgrErr;
use serde::{Deserialize, Serialize};

use super::PeerKey;
use crate::message::Message;

#[cfg(test)]
pub(crate) mod in_memory;
pub(crate) mod p2p_protoc;

// TODO: use this constants when we do real net i/o
// const PING_EVERY: Duration = Duration::from_secs(30);
// const DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);

pub(crate) type ConnResult<T> = std::result::Result<T, ConnectionError>;

#[async_trait::async_trait]
pub(crate) trait ConnectionBridge {
    fn add_connection(&mut self, peer: PeerKey) -> ConnResult<()>;

    fn drop_connection(&mut self, peer: &PeerKey);

    async fn send(&self, target: &PeerKey, msg: Message) -> ConnResult<()>;
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub(crate) enum ConnectionError {
    #[error("location unknown for this node")]
    LocationUnknown,
    #[error("unable to send message")]
    SendNotCompleted,
    #[error("error while de/serializing message")]
    #[serde(skip)]
    Serialization(#[from] Option<Box<bincode::ErrorKind>>),

    // errors produced while handling the connection:
    #[serde(skip)]
    #[error("IO connection error")]
    IOError(#[from] Option<std::io::Error>),
    #[serde(skip)]
    #[error("upgrade connection error")]
    NegotiationError(#[from] Option<Box<ProtocolsHandlerUpgrErr<Self>>>),
}

impl Clone for ConnectionError {
    fn clone(&self) -> Self {
        match self {
            Self::LocationUnknown => Self::LocationUnknown,
            Self::Serialization(_) => Self::Serialization(None),
            Self::SendNotCompleted => Self::SendNotCompleted,
            Self::IOError(_) => Self::IOError(None),
            Self::NegotiationError(_) => Self::NegotiationError(None),
        }
    }
}
