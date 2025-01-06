use super::Location;
use super::PeerKeyLocation;

use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Connection {
    pub(crate) location: PeerKeyLocation,
    pub(crate) open_at: Instant,
}

#[cfg(test)]
use crate::node::PeerId;

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
