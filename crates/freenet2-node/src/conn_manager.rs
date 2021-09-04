//! Manages connections.

use std::{fmt::Display, net::SocketAddr, time::Duration};

use libp2p::{core::PublicKey, PeerId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    message::{JoinResponse, Message},
    ring_proto::Location,
};

const _PING_EVERY: Duration = Duration::from_secs(30);
const _DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);

pub(crate) type Channel<'a> = (PeerId, &'a [u8]);
pub(crate) type RemoveConnHandler<'t> = Box<dyn FnOnce(&'t PeerKey, String)>;
pub(crate) type ListenerCallback<'t> = Box<dyn FnOnce(&'t PeerKey, Box<dyn Message>)>;
pub(crate) type SendCallback<'t> = Box<dyn FnOnce(&'t PeerKey, Box<dyn Message>)>;

pub(crate) struct ListeningHandler;

/// Responsible for securely transmitting Messages between Peers.
pub(crate) trait ConnectionManager {
    type Transport: Transport;

    /// Register a handler callback for when a connection is removed.
    fn on_remove_conn(&mut self, func: RemoveConnHandler);

    fn listen(&mut self, call_back: ListenerCallback) -> ListeningHandler;

    fn transport(&self) -> Self::Transport;

    /// Initiate a connection with a given peer. At this stage NAT traversal
    /// has been succesful and the [`Self::Transport`] has established a connection.
    fn add_connection(&mut self, peer_key: PeerKey, unsolicited: bool);

    /// Send a message to a given peer which has already been identified and  
    /// which has established a connection with this peer, register a callback action
    /// with the manager for when a response is received.
    fn send(&mut self, to: &PeerKey, call_back: SendCallback);
}

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

#[derive(Debug, Serialize, Deserialize)]
/// The Location of a PeerKey in the ring. This location allows routing towards the peer.
pub(crate) struct PeerKeyLocation {
    peer: PeerKey,
    location: Location,
}

mod serialization {
    use super::*;

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
