//! Manages
use std::{convert::TryFrom, net::SocketAddr, time::Duration};

use libp2p::PeerId;

type Channel<'a> = (PeerId, &'a [u8]);
pub(crate) type RemoveConnHandle = Box<dyn FnOnce(PeerId, String) -> ()>;

trait Transport {
    fn send(&mut self, peer: PeerId, message: &[u8]);
    fn is_open(&self) -> bool;
    fn recipient(&self) -> Channel;
}

const _PING_EVERY: Duration = Duration::from_secs(30);
const _DROP_CONN_AFTER: Duration = Duration::from_secs(30 * 10);

/// Responsible for securely transmitting Messages between Peers.
pub(crate) trait ConnectionManager {
    /// Register a handle for the remove connection action.
    fn on_remove_conn(&mut self, func: RemoveConnHandle);
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

/// An abstract location on the 1D ring.
#[derive(PartialEq, PartialOrd)]
pub(crate) struct Location(f64);

impl Location {
    pub fn new() -> Self {
        use rand::prelude::*;
        let mut rng = rand::thread_rng();
        Location(rng.gen_range(0.0..=1.0))
    }
}

impl TryFrom<f64> for Location {
    type Error = ();

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            Err(())
        } else {
            Ok(Location(value))
        }
    }
}

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
