use super::PeerKeyLocation;
use crate::node::PeerId;
use super::Location;
use std::time::Instant;

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
