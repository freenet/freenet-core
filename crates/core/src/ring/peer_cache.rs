//! Peer address cache for fast reconnection after restart.
//!
//! Persists known peer addresses and public keys to disk so that after a brief
//! restart, the node can attempt direct transport connections to previously-known
//! peers, reusing existing NAT holes before they expire.
//!
//! Note: Uses `SystemTime` rather than the `TimeSource` trait because cache entries
//! must survive process restarts. `Instant`/`TimeSource` values reset on restart
//! and cannot represent "when was this entry saved?" across process boundaries.

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::ring::connection_manager::ConnectionManager;
use crate::transport::TransportPublicKey;

/// Maximum number of peers to cache.
const MAX_CACHED_PEERS: usize = 50;

/// Cached peers older than this are discarded on load.
/// Kept short because the main value is reusing NAT holes,
/// which typically expire in 30s-5min.
const CACHE_EXPIRY: Duration = Duration::from_secs(300);

const CACHE_FILENAME: &str = "peer_cache.json";

/// A single cached peer entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CachedPeer {
    pub pub_key: TransportPublicKey,
    pub addr: SocketAddr,
    /// When this entry was saved (for expiry).
    pub saved_at: SystemTime,
}

/// The on-disk peer cache.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct PeerCache {
    pub peers: Vec<CachedPeer>,
}

impl PeerCache {
    /// Load the peer cache from the given data directory, filtering expired entries.
    pub fn load(data_dir: &Path) -> Self {
        let path = data_dir.join(CACHE_FILENAME);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %path.display(), error = %e, "Failed to read peer cache");
                }
                return Self::default();
            }
        };
        let mut cache: PeerCache = match serde_json::from_str(&content) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to parse peer cache, starting fresh");
                return Self::default();
            }
        };
        let now = SystemTime::now();
        cache.peers.retain(|p| {
            now.duration_since(p.saved_at)
                .map(|age| age < CACHE_EXPIRY)
                .unwrap_or(false)
        });
        tracing::info!(count = cache.peers.len(), "Loaded peer cache");
        cache
    }

    /// Save the peer cache to the given data directory (atomic write).
    pub fn save(&self, data_dir: &Path) -> Result<(), std::io::Error> {
        let path = data_dir.join(CACHE_FILENAME);
        let tmp_path = data_dir.join(format!(".{}.tmp", CACHE_FILENAME));
        let content = serde_json::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(&tmp_path, &content)?;
        std::fs::rename(&tmp_path, &path)?;
        tracing::debug!(count = self.peers.len(), "Saved peer cache");
        Ok(())
    }

    /// Build a peer cache snapshot from the current ring connections.
    pub fn snapshot_from(conn_manager: &ConnectionManager) -> Self {
        let now = SystemTime::now();
        let connections = conn_manager.get_connections_by_location();
        let mut peers: Vec<CachedPeer> = connections
            .values()
            .flatten()
            .filter_map(|conn| {
                let pkl = &conn.location;
                pkl.socket_addr().map(|addr| CachedPeer {
                    pub_key: pkl.pub_key.clone(),
                    addr,
                    saved_at: now,
                })
            })
            .collect();
        // Limit to MAX_CACHED_PEERS, preferring the most recent (they're all "now" but
        // truncate in case there are more than the limit).
        peers.truncate(MAX_CACHED_PEERS);
        PeerCache { peers }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    use crate::transport::TransportKeypair;

    fn make_pub_key() -> TransportPublicKey {
        TransportKeypair::new().public().clone()
    }

    fn make_cached_peer(port: u16) -> CachedPeer {
        CachedPeer {
            pub_key: make_pub_key(),
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), port)),
            saved_at: SystemTime::now(),
        }
    }

    #[test]
    fn test_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let cache = PeerCache {
            peers: vec![make_cached_peer(1000), make_cached_peer(1001)],
        };
        cache.save(dir.path()).unwrap();
        let loaded = PeerCache::load(dir.path());
        assert_eq!(loaded.peers.len(), 2);
        assert_eq!(loaded.peers[0].addr, cache.peers[0].addr);
        assert_eq!(loaded.peers[1].addr, cache.peers[1].addr);
    }

    #[test]
    fn test_cache_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let old_peer = CachedPeer {
            pub_key: make_pub_key(),
            addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 1000)),
            saved_at: SystemTime::now() - Duration::from_secs(600),
        };
        let fresh_peer = make_cached_peer(1001);
        let cache = PeerCache {
            peers: vec![old_peer, fresh_peer.clone()],
        };
        cache.save(dir.path()).unwrap();
        let loaded = PeerCache::load(dir.path());
        assert_eq!(loaded.peers.len(), 1);
        assert_eq!(loaded.peers[0].addr, fresh_peer.addr);
    }

    #[test]
    fn test_missing_cache_file() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = PeerCache::load(dir.path());
        assert!(loaded.peers.is_empty());
    }

    #[test]
    fn test_corrupt_cache_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CACHE_FILENAME), "not valid json").unwrap();
        let loaded = PeerCache::load(dir.path());
        assert!(loaded.peers.is_empty());
    }
}
