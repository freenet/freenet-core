//! Types and definitions to handle all socket communication for the peer nodes.

use super::PeerKey;
use crate::{message::Message, ring::PeerKeyLocation};

pub(crate) mod in_memory;
pub(crate) mod locutus_protoc;

// TODO: use this constants when we do real net i/o
// const PING_EVERY: Duration = Duration::from_secs(30);
// const DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);

pub(crate) type ConnResult<T> = std::result::Result<T, ConnectionError>;

#[async_trait::async_trait]
pub(crate) trait ConnectionBridge {
    /// Returns the peer key for this connection.
    fn peer_key(&self) -> PeerKey;

    fn add_connection(&mut self, peer: PeerKeyLocation, unsolicited: bool);

    fn drop_connection(&mut self, peer: PeerKey);

    /// # Cancellation Safety
    /// This async fn must be cancellation safe!
    async fn recv(&self) -> ConnResult<Message>;

    async fn send(&self, target: PeerKey, msg: Message) -> ConnResult<()>;
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnectionError {
    #[error("location unknown for this node")]
    LocationUnknown,
    #[error("error while de/serializing message")]
    Serialization(#[from] Box<bincode::ErrorKind>),
}
