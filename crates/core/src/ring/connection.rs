use std::time::Instant;

use super::PeerKeyLocation;

#[derive(Clone, Debug)]
pub struct Connection {
    pub(crate) location: PeerKeyLocation,
    /// Timestamp when this connection was established.
    /// Used to calculate connection duration for disconnect events.
    pub(crate) connected_at: Instant,
}

impl Connection {
    /// Creates a new connection with the current timestamp.
    pub fn new(location: PeerKeyLocation) -> Self {
        Self {
            location,
            connected_at: Instant::now(),
        }
    }

    /// Returns the duration since the connection was established in milliseconds.
    pub fn duration_ms(&self) -> u64 {
        self.connected_at.elapsed().as_millis() as u64
    }
}
