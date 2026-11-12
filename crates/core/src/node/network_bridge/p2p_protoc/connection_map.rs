//! The connection map, keyed by socket address, whose keys change only through
//! [`ConnectionMap::insert`] and [`ConnectionMap::remove`].
//!
//! The connection manager keeps the set of addresses with an open transport
//! (`ConnectionManager::has_open_transport`), which the relay reads for the
//! QUIC-native decision. That set must hold exactly this map's keys. The map
//! is a private field and holds the manager itself, so the only ways to add
//! or remove a key also note it in the set: anything outside this module reads
//! the map through `Deref` (a shared `BTreeMap`) and changes an entry in place
//! through [`ConnectionMap::get_mut`] or [`ConnectionMap::iter_mut`], none of
//! which can add, remove or replace a key. A map that is dropped, including
//! one replaced or taken out of its field whole, closes its keys in the set.
//! The compiler enforces all of this, so no source scan has to.
//!
//! What it does not cover: two live maps over one manager. Production builds
//! one; a second would share the set, and dropping it would close addresses
//! the first still holds.

use std::collections::BTreeMap;
use std::collections::btree_map;
use std::net::SocketAddr;
use std::ops::Deref;

use super::ConnectionEntry;
use crate::ring::ConnectionManager;

/// Connections by socket address (the transport-level identifier); the
/// source of truth for active connections. A `BTreeMap` for deterministic
/// iteration order in simulation tests.
pub(super) struct ConnectionMap {
    map: BTreeMap<SocketAddr, ConnectionEntry>,
    /// Whose open-transport set mirrors the keys. `None` only in tests that
    /// build a map with no connection manager ([`Self::untracked`]).
    transports: Option<ConnectionManager>,
}

impl ConnectionMap {
    /// An empty map whose keys `transports`' open-transport set mirrors.
    pub(super) fn new(transports: ConnectionManager) -> Self {
        Self {
            map: BTreeMap::new(),
            transports: Some(transports),
        }
    }

    /// An empty map mirrored nowhere, for tests with no connection manager.
    #[cfg(test)]
    pub(super) fn untracked() -> Self {
        Self {
            map: BTreeMap::new(),
            transports: None,
        }
    }

    /// Add or replace the connection at `addr`, noting the address in the
    /// open-transport set.
    pub(super) fn insert(
        &mut self,
        addr: SocketAddr,
        entry: ConnectionEntry,
    ) -> Option<ConnectionEntry> {
        if let Some(transports) = &self.transports {
            transports.note_transport_open(addr);
        }
        self.map.insert(addr, entry)
    }

    /// Remove the connection at `addr`, and the address from the
    /// open-transport set.
    pub(super) fn remove(&mut self, addr: SocketAddr) -> Option<ConnectionEntry> {
        if let Some(transports) = &self.transports {
            transports.note_transport_closed(addr);
        }
        self.map.remove(&addr)
    }

    /// The entry at `addr`, to change in place.
    pub(super) fn get_mut(&mut self, addr: &SocketAddr) -> Option<&mut ConnectionEntry> {
        self.map.get_mut(addr)
    }

    /// Every entry, to change in place.
    pub(super) fn iter_mut(&mut self) -> btree_map::IterMut<'_, SocketAddr, ConnectionEntry> {
        self.map.iter_mut()
    }
}

impl Drop for ConnectionMap {
    fn drop(&mut self) {
        if let Some(transports) = &self.transports {
            for addr in self.map.keys() {
                transports.note_transport_closed(*addr);
            }
        }
    }
}

impl Deref for ConnectionMap {
    type Target = BTreeMap<SocketAddr, ConnectionEntry>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tokio::sync::mpsc;
    use tokio::time::Instant;

    use super::*;
    use crate::node::network_bridge::p2p_protoc::zombie_sweep::LinkUseStamp;

    fn entry(connection_id: u64) -> ConnectionEntry {
        let (sender, _receiver) = mpsc::channel(1);
        let now = Instant::now();
        ConnectionEntry {
            sender,
            identity: Default::default(),
            receipt: None,
            connection_id,
            created_at: now,
            link_use: LinkUseStamp::new(now),
            remote_version: None,
        }
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([192, 0, 2, 1], port))
    }

    fn keys(map: &ConnectionMap) -> BTreeSet<SocketAddr> {
        map.keys().copied().collect()
    }

    /// Insert, replace, in-place changes and remove keep the open-transport
    /// set equal to the map's keys.
    #[tokio::test]
    async fn the_open_transport_set_follows_inserts_and_removes() {
        let manager = ConnectionManager::test_default();
        let mut map = ConnectionMap::new(manager.clone());
        map.insert(addr(1), entry(1));
        map.insert(addr(2), entry(2));
        assert_eq!(manager.open_transport_addrs(), keys(&map));
        // Replacing an entry keeps its address open.
        assert!(map.insert(addr(1), entry(3)).is_some());
        map.get_mut(&addr(2)).expect("present").connection_id = 4;
        for (_, entry) in map.iter_mut() {
            entry.remote_version = None;
        }
        assert_eq!(manager.open_transport_addrs(), keys(&map));
        assert!(map.remove(addr(1)).is_some());
        assert_eq!(manager.open_transport_addrs(), keys(&map));
        // Removing an address the map does not hold changes neither.
        assert!(map.remove(addr(9)).is_none());
        assert_eq!(manager.open_transport_addrs(), keys(&map));
    }

    /// A map replaced or taken out of its place whole closes its addresses,
    /// so the set holds the keys of the map that replaced it.
    #[tokio::test]
    async fn a_replaced_map_closes_its_addresses() {
        let manager = ConnectionManager::test_default();
        let mut map = ConnectionMap::new(manager.clone());
        map.insert(addr(1), entry(1));
        map.insert(addr(2), entry(2));
        map = ConnectionMap::new(manager.clone());
        assert!(manager.open_transport_addrs().is_empty());
        map.insert(addr(3), entry(3));
        let taken = std::mem::replace(&mut map, ConnectionMap::new(manager.clone()));
        assert_eq!(keys(&taken), BTreeSet::from([addr(3)]));
        drop(taken);
        assert_eq!(manager.open_transport_addrs(), keys(&map));
        assert!(manager.open_transport_addrs().is_empty());
    }
}
