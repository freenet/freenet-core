//! Manages connections.

use std::{net::SocketAddr, time::Duration};

use libp2p::PeerId;

use crate::{message::Message, ring_proto::Location};

type Channel<'a> = (PeerId, &'a [u8]);
pub(crate) type RemoveConnHandle = Box<dyn FnOnce(PeerId, String) -> ()>;
pub(crate) type ListenerReaction = Box<dyn FnOnce(PeerId, Box<dyn Message>) -> ()>;

trait Transport {
    fn send(&mut self, peer: PeerId, message: &[u8]);
    fn is_open(&self) -> bool;
    fn recipient(&self) -> Channel;
}

const _PING_EVERY: Duration = Duration::from_secs(30);
const _DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);

pub(crate) struct ListenHandler;

/// Responsible for securely transmitting Messages between Peers.
pub(crate) trait ConnectionManager {
    /// Register a handle connection removal action.
    fn on_remove_conn(&mut self, func: RemoveConnHandle);

    fn listen(&mut self, reaction: ListenerReaction) -> ListenHandler;
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

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PeerKey;

/// The Location of a PeerKey in the ring. This location allows routing towards the peer.
pub(crate) struct PeerKeyLocation {
    peer: PeerId,
    location: Location,
}
