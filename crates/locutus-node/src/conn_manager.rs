//! Types and definitions to handle all socket communication for the peer nodes.

use std::{fmt::Display, time::Duration};

use libp2p::{core::PublicKey, PeerId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    message::{Message, Transaction},
    ring::Location,
    StdResult,
};

pub mod in_memory;

const PING_EVERY: Duration = Duration::from_secs(30);
const DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);

// pub(crate) type RemoveConnHandler<'t> = Box<dyn FnOnce(&'t PeerKey, String)>;
pub(crate) type Result<T> = StdResult<T, ConnError>;

#[async_trait::async_trait]
pub(crate) trait ConnectionBridge {
    fn add_connection(&mut self, peer: PeerKeyLocation, unsolicited: bool);

    /// # Cancellation Safety
    /// This async fn must be cancellation safe!
    async fn recv(&self) -> Result<Message>;

    async fn send(&self, target: &PeerKeyLocation, msg: Message) -> Result<()>;
}

/// A protocol used to send and receive data over the network.
pub(crate) trait Transport {
    fn is_open(&self) -> bool;
    fn location(&self) -> Option<Location>;
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct PeerKey(pub(crate) PeerId);

impl Display for PeerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<PublicKey> for PeerKey {
    fn from(val: PublicKey) -> Self {
        PeerKey(PeerId::from(val))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
/// The Location of a PeerKey in the ring. This location allows routing towards the peer.
pub(crate) struct PeerKeyLocation {
    pub peer: PeerKey,
    pub location: Option<Location>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnError {
    #[error("received unexpected response type for a sent request: {0}")]
    UnexpectedResponseMessage(Message),
    #[error("location unknown for this node")]
    LocationUnknown,
    #[error("expected transaction id was {0} but received {1}")]
    UnexpectedTx(Transaction, Transaction),
    #[error("error while de/serializing message")]
    Serialization(#[from] Box<bincode::ErrorKind>),
    #[error("connection negotiation between two peers failed")]
    NegotationFailed,
}

mod serialization {
    use super::*;

    impl Serialize for PeerKey {
        fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(&self.0.to_bytes())
        }
    }

    impl<'de> Deserialize<'de> for PeerKey {
        fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
            Ok(PeerKey(
                PeerId::from_bytes(&bytes).expect("failed deserialization of PeerKey"),
            ))
        }
    }
}
