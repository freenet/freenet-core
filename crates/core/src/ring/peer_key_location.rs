use super::Location;
use crate::node::PeerId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::{fmt::Display, hash::Hash};

/// Explicit representation of peer address state.
///
/// This enum distinguishes between:
/// - `Unknown`: The address is not known by the sender (e.g., a peer doesn't know its own external address).
///   The first recipient should fill this in from the packet source address.
/// - `Known`: The address is known and explicitly specified.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Debug)]
#[allow(dead_code)] // Will be used as refactoring progresses
pub enum PeerAddr {
    /// Address unknown - will be filled in by first recipient from packet source.
    /// Used when a peer doesn't know its own external address (e.g., behind NAT).
    Unknown,
    /// Known address - explicitly specified.
    Known(SocketAddr),
}

#[allow(dead_code)] // Will be used as refactoring progresses
impl PeerAddr {
    /// Returns the socket address if known, None otherwise.
    pub fn as_known(&self) -> Option<&SocketAddr> {
        match self {
            PeerAddr::Known(addr) => Some(addr),
            PeerAddr::Unknown => None,
        }
    }

    /// Returns true if the address is known.
    pub fn is_known(&self) -> bool {
        matches!(self, PeerAddr::Known(_))
    }

    /// Returns true if the address is unknown.
    pub fn is_unknown(&self) -> bool {
        matches!(self, PeerAddr::Unknown)
    }
}

impl From<SocketAddr> for PeerAddr {
    fn from(addr: SocketAddr) -> Self {
        PeerAddr::Known(addr)
    }
}

impl std::fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerAddr::Unknown => write!(f, "<unknown>"),
            PeerAddr::Known(addr) => write!(f, "{}", addr),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
/// The location of a peer in the ring. This location allows routing towards the peer.
pub struct PeerKeyLocation {
    pub peer: PeerId,
    // TODO: this shouldn't e an option, when we are using this struct the location is always known
    /// An unspecified location means that the peer hasn't been asigned a location, yet.
    pub location: Option<Location>,
}

impl PeerKeyLocation {
    #[cfg(test)]
    pub fn random() -> Self {
        PeerKeyLocation {
            peer: PeerId::random(),
            location: Some(Location::random()),
        }
    }
}

impl From<PeerId> for PeerKeyLocation {
    fn from(peer: PeerId) -> Self {
        PeerKeyLocation {
            peer,
            location: None,
        }
    }
}

impl std::fmt::Debug for PeerKeyLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Display for PeerKeyLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.location {
            Some(loc) => write!(f, "{} (@ {loc})", self.peer),
            None => write!(f, "{}", self.peer),
        }
    }
}
