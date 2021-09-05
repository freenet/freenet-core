//! Manages connections.

use std::{fmt::Display, net::SocketAddr, sync::atomic::AtomicU64, time::Duration};

use libp2p::{core::PublicKey, PeerId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    message::{Message, MsgTypeId},
    ring_proto::Location,
    StdResult,
};

const _PING_EVERY: Duration = Duration::from_secs(30);
const _DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);
static HANDLE_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) type Channel<'a> = (PeerId, &'a [u8]);
pub(crate) type RemoveConnHandler<'t> = Box<dyn FnOnce(&'t PeerKey, String)>;
pub(crate) type Result<T> = StdResult<T, ConnError>;

// pub(crate) type ListenerCallback2 = for<'t> Box<dyn FnOnce(&'t PeerKey, Message) -> Result<()>>;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct ListeningHandler(u64);

impl ListeningHandler {
    pub fn new() -> Self {
        Self(HANDLE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

/// Types which impl this trait are responsible for the following responsabilities:
/// - establishing reliable connections to other peers
/// - keep connections alive or reconnecting to other peers
/// - securely transmitting messages between peers
///
/// This implementing types manage the lower level connection details of the the network,
/// usually working at the transport layer over UDP or TCP and performing NAT traversal
/// to establish connections between peers.
pub(crate) trait ConnectionManager: Clone + Send + Sync {
    /// The transport being used to manage networking.
    type Transport: Transport;

    /// Register a handler callback for when a connection is removed.
    fn on_remove_conn(&self, func: RemoveConnHandler);

    fn listen<F>(&self, msg_type: MsgTypeId, callback: F) -> ListeningHandler
    where
        F: FnOnce(PeerKey, Message) -> Result<()> + Send + Sync + 'static;

    fn transport(&self) -> Self::Transport;

    /// Initiate a connection with a given peer. At this stage NAT traversal
    /// has been succesful and the [`Transport`] has established a connection.
    fn add_connection(&self, peer_key: PeerKeyLocation, unsolicited: bool);

    /// Send a message to a given peer which has already been identified and  
    /// which has established a connection with this peer, register a callback action
    /// with the manager for when a response is received.
    fn send<F>(&self, to: &PeerKey, callback: F)
    where
        F: FnOnce(PeerKey, Message) -> Result<()> + Send + Sync + 'static;
}

/// A protocol used to send and receive data over the network.
pub(crate) trait Transport {
    fn send(&mut self, peer: PeerKey, message: &[u8]);
    fn is_open(&self) -> bool;
    fn recipient(&self) -> Channel;
}

// pub(crate) struct ConnectionManager {
//     key: Keypair,
//     transport: Box<dyn Transport>,
//     open_connections: HashMap<Peer, Connection>,
// }

// impl ConnectionManager {
//     fn new(transport: Box<dyn Transport>, key: Keypair) -> Self {
//         Self {
//             key,
//             transport,
//             open_connections: HashMap::new(),
//         }
//     }
// }

// enum Connection {
//     Symmetric {},
//     Outbound {},
//     Inbound {},
// }

#[derive(Debug, PartialEq, Eq, Hash)]
struct Peer {
    addr: SocketAddr,
    port: u16,
    label: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct PeerKey(PeerId);

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

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
/// The Location of a PeerKey in the ring. This location allows routing towards the peer.
pub(crate) struct PeerKeyLocation {
    pub peer: PeerKey,
    pub location: Location,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnError {
    #[error("received unexpected response type for a sent request: {0}")]
    UnexpectedResponseMessage(Message),
    #[error("location unknown for this node")]
    LocationUnknown,
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
