use tokio::time::Instant;

use super::PeerKeyLocation;

#[derive(Clone, Debug)]
pub struct Connection {
    pub(crate) location: PeerKeyLocation,
    /// Timestamp when this connection was established.
    /// Used to calculate connection duration for disconnect events.
    pub(crate) connected_at: Instant,
    /// The peer's per-address wire-byte totals `(sent, received)` when this
    /// connection joined the ring, so the disconnect event can report this
    /// connection's bytes rather than the address's lifetime totals (which
    /// include earlier NAT attempts and previous connections).
    ///
    /// Known gaps (best effort): if the bounded per-peer table evicts and
    /// recreates the entry mid-connection, the totals restart and the
    /// reported bytes undercount (still positive) or are `None`; and a
    /// connection moved to a new address by
    /// `ConnectionManager::update_peer_identity` gets a fresh baseline for
    /// the new address, so bytes sent before the move are not reported.
    pub(crate) bytes_at_connect: (u64, u64),
}

impl Connection {
    /// Creates a new connection with the current timestamp.
    pub fn new(location: PeerKeyLocation) -> Self {
        let bytes_at_connect = location
            .socket_addr()
            .and_then(|addr| crate::transport::TRANSPORT_METRICS.connection_peer_bytes(addr))
            .unwrap_or((0, 0));
        Self {
            location,
            connected_at: Instant::now(),
            bytes_at_connect,
        }
    }

    /// Bytes `(sent, received)` on this connection given the peer's current
    /// per-address totals. A direction is `None` when the totals went
    /// backwards (the per-peer entry was evicted and recreated meanwhile), or
    /// both are when the peer is no longer tracked at all.
    pub fn bytes_since_connect(&self, current: Option<(u64, u64)>) -> (Option<u64>, Option<u64>) {
        let Some((sent, received)) = current else {
            return (None, None);
        };
        let (sent0, received0) = self.bytes_at_connect;
        (sent.checked_sub(sent0), received.checked_sub(received0))
    }

    /// Returns the duration since the connection was established in milliseconds.
    pub fn duration_ms(&self) -> u64 {
        self.connected_at.elapsed().as_millis() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_since_connect_subtracts_the_baseline() {
        let mut conn = Connection::new(PeerKeyLocation::new(
            crate::transport::TransportKeypair::new().public().clone(),
            "198.51.100.40:7000".parse().unwrap(),
        ));
        conn.bytes_at_connect = (100, 1_000);
        assert_eq!(
            conn.bytes_since_connect(Some((400, 1_500))),
            (Some(300), Some(500))
        );
        assert_eq!(
            conn.bytes_since_connect(Some((50, 1_500))),
            (None, Some(500))
        );
        assert_eq!(conn.bytes_since_connect(None), (None, None));
    }
}
