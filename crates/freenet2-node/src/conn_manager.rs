//! Types and definitions to handle all socket communication for the peer nodes.

use std::{fmt::Display, net::SocketAddr, sync::atomic::AtomicU64, time::Duration};

use libp2p::{core::PublicKey, PeerId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    message::{Message, MsgTypeId, Transaction},
    ring_proto::Location,
    StdResult,
};

pub mod in_memory;

const _PING_EVERY: Duration = Duration::from_secs(30);
const _DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);
static HANDLE_ID: AtomicU64 = AtomicU64::new(0);

// pub(crate) type RemoveConnHandler<'t> = Box<dyn FnOnce(&'t PeerKey, String)>;
pub(crate) type Result<T> = StdResult<T, ConnError>;

/// 3 words size for 64-bit platforms.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct ListenerHandle(u64);

impl ListenerHandle {
    pub fn new() -> Self {
        ListenerHandle(HANDLE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

impl Default for ListenerHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Types which impl this trait are responsible for the following responsabilities:
/// - establishing reliable connections to other peers,
///   including any handshake procedures
/// - keep connections alive or reconnecting to other peers
/// - securely transmitting messages between peers
/// - serializing and deserializing messages
///
/// The implementing types manage the lower level connection details of the the network,
/// usually working at the transport layer over UDP or TCP and performing NAT traversal
/// to establish connections between peers.
pub(crate) trait ConnectionBridge: Send + Sync {
    /// The transport being used to manage networking.
    type Transport: Transport;

    /// Register a handler callback for when a connection is removed.
    // fn on_remove_conn(&self, func: RemoveConnHandler);

    /// Start listening for incoming connections and process any incoming messages with
    /// the provided function.
    fn listen<F>(&self, tx_type: MsgTypeId, listen_fn: F) -> ListenerHandle
    where
        F: Fn(PeerKeyLocation, Message) -> Result<()> + Send + Sync + 'static;

    /// Listens to inbound replies for a previously broadcasted transaction to the network,
    /// if a reply is detected performs a callback.
    // FIXME: the fn could take arguments by ref if necessary but due to
    // https://github.com/rust-lang/rust/issues/70263 it won't compile
    // can workaround by wrapping up the fn to express lifetime constraints,
    // consider this, meanwhile passing by value is fine
    fn listen_to_replies<F>(&self, tx_id: Transaction, callback: F)
    where
        F: Fn(PeerKeyLocation, Message) -> Result<()> + Send + Sync + 'static;

    fn transport(&self) -> &Self::Transport;

    /// Initiate a connection with a given peer. At this stage NAT traversal
    /// has been succesful and the [`Transport`] has established a connection.
    fn add_connection(&self, peer_key: PeerKeyLocation, unsolicited: bool);

    /// Sends a message to a given peer which has already been identified and  
    /// which has established a connection with this peer, registers a callback action
    /// with the manager for when a response is received.
    fn send_with_callback<F>(
        &self,
        to: PeerKeyLocation,
        tx_id: Transaction,
        msg: Message,
        callback: F,
    ) -> Result<()>
    where
        F: Fn(PeerKeyLocation, Message) -> Result<()> + Send + Sync + 'static;

    /// Send a message to a given peer which has already been identified and  
    /// which has established a connection with this peer.
    fn send(&self, to: PeerKeyLocation, tx_id: Transaction, msg: Message) -> Result<()>;

    /// Remove a listener for a given transaction.
    fn remove_listener(&self, tx_id: Transaction);
}

/// A protocol used to send and receive data over the network.
pub(crate) trait Transport {
    fn is_open(&self) -> bool;
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Peer {
    addr: SocketAddr,
    port: u16,
    label: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct PeerKey(PeerId);

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
