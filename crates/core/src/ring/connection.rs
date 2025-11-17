use super::PeerKeyLocation;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Connection {
    pub(crate) location: PeerKeyLocation,
    pub(crate) open_at: Instant,
}
