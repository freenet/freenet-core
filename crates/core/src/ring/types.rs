use std::{
    fmt::Display,
    hash::Hash,
    time::Instant,
};
use serde::{Deserialize, Serialize};
use crate::node::PeerId;
use super::Location;

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
pub(crate) struct Connection {
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

#[derive(PartialEq, Clone, Copy)]
pub(crate) struct Score(pub(crate) f64);

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl Eq for Score {}
