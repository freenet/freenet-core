//! Types and definitions to handle all socket communication for the peer nodes.

use std::fmt::Display;

use libp2p::{core::PublicKey, identity::Keypair, PeerId};

use crate::{
    message::Message,
    ring::{Location, PeerKeyLocation},
};

pub mod in_memory;

// TODO: use this constants when we do real net i/o
// const PING_EVERY: Duration = Duration::from_secs(30);
// const DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);

// pub(crate) type RemoveConnHandler<'t> = Box<dyn FnOnce(&'t PeerKey, String)>;
pub(crate) type Result<T> = std::result::Result<T, ConnError>;

#[async_trait::async_trait]
pub(crate) trait ConnectionBridge {
    /// Returns the peer key for this connection.
    fn peer_key(&self) -> PeerKey;

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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct PeerKey(PeerId);

impl PeerKey {
    pub fn random() -> Self {
        PeerKey::from(Keypair::generate_ed25519().public())
    }
}

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

impl From<PeerId> for PeerKey {
    fn from(val: PeerId) -> Self {
        PeerKey(val)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnError {
    // TODO: clean up comments/variants
    // #[error("received unexpected response type for a sent request: {0}")]
    // UnexpectedResponseMessage(Message),
    #[error("location unknown for this node")]
    LocationUnknown,
    // #[error("expected transaction id was {0} but received {1}")]
    // UnexpectedTx(Transaction, Transaction),
    #[error("error while de/serializing message")]
    Serialization(#[from] Box<bincode::ErrorKind>),
    // #[error("connection negotiation between two peers failed")]
    // NegotationFailed,
}

mod serialization {
    use libp2p::PeerId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::PeerKey;

    impl Serialize for PeerKey {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_bytes(&self.0.to_bytes())
        }
    }

    impl<'de> Deserialize<'de> for PeerKey {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
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
