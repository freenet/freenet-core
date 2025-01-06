use super::Location;
use crate::node::PeerId;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, hash::Hash, time::Instant};

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
/// The location of a peer in the ring. This location allows routing towards the peer.
pub struct PeerKeyLocation {
    pub peer: PeerId,
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

#[derive(Clone, Debug)]
pub struct Connection {
    pub(crate) location: PeerKeyLocation,
    pub(crate) open_at: Instant,
}

#[cfg(test)]
impl Connection {
    pub fn new(peer: PeerId, location: Location) -> Self {
        Connection {
            location: PeerKeyLocation {
                peer,
                location: Some(location),
            },
            open_at: Instant::now(),
        }
    }

    pub fn get_location(&self) -> &PeerKeyLocation {
        &self.location
    }
}

