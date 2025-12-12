use dashmap::DashMap;
use parking_lot::Mutex;
use rand::prelude::IndexedRandom;
use std::collections::{btree_map::Entry, BTreeMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::topology::{Limits, TopologyManager};

use super::*;

#[derive(Clone)]
pub(crate) struct TransientEntry {
    /// Entry tracking a transient connection that hasn't been added to the ring topology yet.
    /// Transient connections are typically unsolicited inbound connections to gateways.
    /// Advertised location for the transient peer, if known at admission time.
    pub location: Option<Location>,
}

#[derive(Clone)]
pub(crate) struct ConnectionManager {
    /// Pending connection reservations, keyed by socket address.
    pending_reservations: Arc<RwLock<BTreeMap<SocketAddr, Location>>>,
    /// Mapping from socket address to location for established connections.
    pub(super) location_for_peer: Arc<RwLock<BTreeMap<SocketAddr, Location>>>,
    pub(super) topology_manager: Arc<RwLock<TopologyManager>>,
    connections_by_location: Arc<RwLock<BTreeMap<Location, Vec<Connection>>>>,
    /// Interim connections ongoing handshake or successfully open connections
    /// Is important to keep track of this so no more connections are accepted prematurely.
    own_location: Arc<AtomicU64>,
    /// Our own socket address, set once we know it (e.g., from gateway observation).
    own_addr: Arc<Mutex<Option<SocketAddr>>>,
    is_gateway: bool,
    /// Transient connections keyed by socket address.
    transient_connections: Arc<DashMap<SocketAddr, TransientEntry>>,
    transient_in_use: Arc<AtomicUsize>,
    transient_budget: usize,
    transient_ttl: Duration,
    pub min_connections: usize,
    pub max_connections: usize,
    pub rnd_if_htl_above: usize,
    pub pub_key: Arc<TransportPublicKey>,
}

impl ConnectionManager {
    pub fn new(config: &NodeConfig) -> Self {
        let min_connections = if let Some(v) = config.min_number_conn {
            v
        } else {
            Ring::DEFAULT_MIN_CONNECTIONS
        };

        let mut max_connections = if let Some(v) = config.max_number_conn {
            v
        } else {
            Ring::DEFAULT_MAX_CONNECTIONS
        };
        // Gateways benefit from a wider neighbor set for forwarding; default to a higher cap when unset.
        if config.is_gateway && config.max_number_conn.is_none() {
            max_connections = 20;
        }

        let max_upstream_bandwidth = if let Some(v) = config.max_upstream_bandwidth {
            v
        } else {
            Ring::DEFAULT_MAX_UPSTREAM_BANDWIDTH
        };

        let max_downstream_bandwidth = if let Some(v) = config.max_downstream_bandwidth {
            v
        } else {
            Ring::DEFAULT_MAX_DOWNSTREAM_BANDWIDTH
        };

        let rnd_if_htl_above = if let Some(v) = config.rnd_if_htl_above {
            v
        } else {
            Ring::DEFAULT_RAND_WALK_ABOVE_HTL
        };

        let own_location = if let Some(location) = config.location {
            AtomicU64::new(u64::from_le_bytes(location.as_f64().to_le_bytes()))
        } else if let Some(addr) = config.own_addr {
            // if the address is set, compute location from it (gateway case)
            let location = Location::from_address(&addr);
            AtomicU64::new(u64::from_le_bytes(location.as_f64().to_le_bytes()))
        } else {
            // for location here consider -1 == None
            AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()))
        };

        Self::init(
            max_upstream_bandwidth,
            max_downstream_bandwidth,
            min_connections,
            max_connections,
            rnd_if_htl_above,
            (
                config.key_pair.public().clone(),
                config.own_addr,
                own_location,
            ),
            config.is_gateway,
            config.transient_budget,
            config.transient_ttl,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn init(
        max_upstream_bandwidth: Rate,
        max_downstream_bandwidth: Rate,
        min_connections: usize,
        max_connections: usize,
        rnd_if_htl_above: usize,
        (pub_key, own_addr, own_location): (TransportPublicKey, Option<SocketAddr>, AtomicU64),
        is_gateway: bool,
        transient_budget: usize,
        transient_ttl: Duration,
    ) -> Self {
        let topology_manager = Arc::new(RwLock::new(TopologyManager::new(Limits {
            max_upstream_bandwidth,
            max_downstream_bandwidth,
            min_connections,
            max_connections,
        })));

        Self {
            connections_by_location: Arc::new(RwLock::new(BTreeMap::new())),
            location_for_peer: Arc::new(RwLock::new(BTreeMap::new())),
            pending_reservations: Arc::new(RwLock::new(BTreeMap::new())),
            topology_manager,
            own_location: own_location.into(),
            own_addr: Arc::new(Mutex::new(own_addr)),
            is_gateway,
            transient_connections: Arc::new(DashMap::new()),
            transient_in_use: Arc::new(AtomicUsize::new(0)),
            transient_budget,
            transient_ttl,
            min_connections,
            max_connections,
            rnd_if_htl_above,
            pub_key: Arc::new(pub_key),
        }
    }

    /// Whether a node should accept a new node connection or not based
    /// on the relative location and other conditions.
    ///
    /// # Panic
    /// Will panic if the node checking for this condition has no location assigned.
    pub fn should_accept(&self, location: Location, addr: SocketAddr) -> bool {
        // Don't accept connections from ourselves
        if let Some(own_addr) = self.get_own_addr() {
            if own_addr == addr {
                tracing::warn!(
                    addr = %addr,
                    peer_location = %location,
                    "should_accept: rejecting self-connection attempt"
                );
                return false;
            }
        }
        let open = self.connection_count();
        let reserved_before = self.pending_reservations.read().len();

        tracing::debug!(
            addr = %addr,
            peer_location = %location,
            open,
            reserved_before,
            is_gateway = self.is_gateway,
            min = self.min_connections,
            max = self.max_connections,
            rnd_if_htl_above = self.rnd_if_htl_above,
            "should_accept: evaluating direct acceptance guard"
        );

        if self.is_gateway && (open > 0 || reserved_before > 0) {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                open,
                reserved_before,
                "Gateway evaluating additional direct connection (post-bootstrap)"
            );
        }

        if self.location_for_peer.read().get(&addr).is_some() {
            // We've already accepted this peer (pending or active); treat as a no-op acceptance.
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                "Peer already pending/connected; acknowledging acceptance"
            );
            return true;
        }

        {
            let mut pending = self.pending_reservations.write();
            pending.insert(addr, location);
        }

        let total_conn = match reserved_before
            .checked_add(1)
            .and_then(|val| val.checked_add(open))
        {
            Some(val) => val,
            None => {
                tracing::error!(
                    addr = %addr,
                    peer_location = %location,
                    reserved_before,
                    open,
                    "connection counters would overflow; rejecting connection"
                );
                self.pending_reservations.write().remove(&addr);
                return false;
            }
        };

        if open == 0 {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                "should_accept: first connection -> accepting"
            );
            return true;
        }

        let accepted = if total_conn < self.min_connections {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                total_conn,
                "should_accept: accepted (below min connections)"
            );
            true
        } else if total_conn >= self.max_connections {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                total_conn,
                "should_accept: rejected (max connections reached)"
            );
            false
        } else {
            let accepted = self
                .topology_manager
                .write()
                .evaluate_new_connection(location, Instant::now())
                .unwrap_or(true);

            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                total_conn,
                accepted,
                "should_accept: topology manager decision"
            );
            accepted
        };
        tracing::info!(
            addr = %addr,
            peer_location = %location,
            accepted,
            total_conn,
            open_connections = open,
            reserved_connections = self.pending_reservations.read().len(),
            max_connections = self.max_connections,
            min_connections = self.min_connections,
            "should_accept: final decision"
        );
        if !accepted {
            self.pending_reservations.write().remove(&addr);
        } else {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                total_conn,
                "should_accept: accepted (reserving spot)"
            );
            self.record_pending_location(addr, location);
        }
        accepted
    }

    /// Record the advertised location for a peer that we have decided to accept.
    ///
    /// This makes the peer discoverable to the routing layer even before the connection
    /// is fully established. The entry is removed automatically if the handshake fails
    /// via `prune_in_transit_connection`.
    pub fn record_pending_location(&self, addr: SocketAddr, location: Location) {
        let mut locations = self.location_for_peer.write();
        let entry = locations.entry(addr);
        match entry {
            Entry::Occupied(_) => {
                tracing::debug!(
                    addr = %addr,
                    peer_location = %location,
                    "record_pending_location: location already known"
                );
            }
            Entry::Vacant(v) => {
                tracing::debug!(
                    addr = %addr,
                    peer_location = %location,
                    "record_pending_location: registering advertised location for peer"
                );
                v.insert(location);
            }
        }
    }

    /// Update this node location.
    pub fn update_location(&self, loc: Option<Location>) {
        if let Some(loc) = loc {
            self.own_location.store(
                u64::from_le_bytes(loc.as_f64().to_le_bytes()),
                std::sync::atomic::Ordering::Release,
            );
        } else {
            self.own_location.store(
                u64::from_le_bytes((-1f64).to_le_bytes()),
                std::sync::atomic::Ordering::Release,
            )
        }
    }

    /// Returns this node location in the ring, if any (must have joined the ring already).
    ///
    /// # Panic
    ///
    /// Will panic if the node has no address assigned yet.
    pub fn own_location(&self) -> PeerKeyLocation {
        let location = f64::from_le_bytes(
            self.own_location
                .load(std::sync::atomic::Ordering::Acquire)
                .to_le_bytes(),
        );
        let _location = if (location - -1f64).abs() < f64::EPSILON {
            None
        } else {
            Some(Location::new(location))
        };
        let addr = self.get_own_addr().expect("own address not set");
        PeerKeyLocation::new((*self.pub_key).clone(), addr)
    }

    /// Returns our own socket address if set.
    pub fn get_own_addr(&self) -> Option<SocketAddr> {
        *self.own_addr.lock()
    }

    /// Look up a PeerKeyLocation by socket address from connections_by_location or transient connections.
    pub fn get_peer_by_addr(&self, addr: SocketAddr) -> Option<PeerKeyLocation> {
        // Check connections by location
        let connections = self.connections_by_location.read();
        for conns in connections.values() {
            for conn in conns {
                if conn.location.socket_addr() == Some(addr) {
                    return Some(conn.location.clone());
                }
            }
        }
        drop(connections);

        // Check transient connections
        if self.transient_connections.contains_key(&addr) {
            // For transient connections, we don't have full peer info yet
            // Return None since we don't have the public key
            return None;
        }
        None
    }

    /// Look up a PeerKeyLocation by socket address from connections_by_location or transient connections.
    /// Used for connection-based routing when we need full peer info from just an address.
    pub fn get_peer_location_by_addr(&self, addr: SocketAddr) -> Option<PeerKeyLocation> {
        // Check connections by location
        let connections = self.connections_by_location.read();
        for conns in connections.values() {
            for conn in conns {
                if conn.location.socket_addr() == Some(addr) {
                    return Some(conn.location.clone());
                }
            }
        }
        drop(connections);

        // Transient connections don't have full PeerKeyLocation info
        None
    }

    pub fn is_gateway(&self) -> bool {
        self.is_gateway
    }

    /// Attempts to register a transient connection, enforcing the configured budget.
    /// Returns `false` when the budget is exhausted, leaving the map unchanged.
    pub fn try_register_transient(&self, addr: SocketAddr, location: Option<Location>) -> bool {
        if self.transient_connections.contains_key(&addr) {
            if let Some(mut entry) = self.transient_connections.get_mut(&addr) {
                entry.location = location;
            }
            return true;
        }

        let current = self.transient_in_use.load(Ordering::Acquire);
        if current >= self.transient_budget {
            return false;
        }

        self.transient_connections
            .insert(addr, TransientEntry { location });
        let prev = self.transient_in_use.fetch_add(1, Ordering::SeqCst);
        if prev >= self.transient_budget {
            // Undo if we raced past the budget.
            self.transient_connections.remove(&addr);
            self.transient_in_use.fetch_sub(1, Ordering::SeqCst);
            return false;
        }

        true
    }

    /// Drops a transient connection and returns its metadata, if it existed.
    /// Also decrements the transient budget counter.
    pub fn drop_transient(&self, addr: SocketAddr) -> Option<TransientEntry> {
        let removed = self
            .transient_connections
            .remove(&addr)
            .map(|(_, entry)| entry);
        if removed.is_some() {
            self.transient_in_use.fetch_sub(1, Ordering::SeqCst);
        }
        removed
    }

    /// Check whether a peer is currently tracked as transient.
    pub fn is_transient(&self, addr: SocketAddr) -> bool {
        self.transient_connections.contains_key(&addr)
    }

    /// Current number of tracked transient connections.
    pub fn transient_count(&self) -> usize {
        self.transient_in_use.load(Ordering::Acquire)
    }

    /// Maximum transient slots allowed.
    pub fn transient_budget(&self) -> usize {
        self.transient_budget
    }

    /// Time-to-live for transients before automatic drop.
    pub fn transient_ttl(&self) -> Duration {
        self.transient_ttl
    }

    /// Sets the own address if it is not already set, or returns the current address.
    pub fn try_set_own_addr(&self, addr: SocketAddr) -> Option<SocketAddr> {
        let mut own_addr = self.own_addr.lock();
        if own_addr.is_none() {
            *own_addr = Some(addr);
            None
        } else {
            *own_addr
        }
    }

    pub fn prune_alive_connection(&self, addr: SocketAddr) -> Option<Location> {
        self.prune_connection(addr, true)
    }

    pub fn prune_in_transit_connection(&self, addr: SocketAddr) -> Option<Location> {
        self.prune_connection(addr, false)
    }

    pub fn add_connection(
        &self,
        loc: Location,
        addr: SocketAddr,
        pub_key: TransportPublicKey,
        was_reserved: bool,
    ) {
        tracing::debug!(
            addr = %addr,
            peer_location = %loc,
            was_reserved = %was_reserved,
            "Adding connection to topology"
        );
        debug_assert!(self.get_own_addr().expect("should be set") != addr);
        if was_reserved {
            self.pending_reservations.write().remove(&addr);
        }
        let mut lop = self.location_for_peer.write();
        let previous_location = lop.insert(addr, loc);
        drop(lop);

        // Enforce the global cap when adding a new peer (relocations reuse the existing slot).
        if previous_location.is_none() && self.connection_count() >= self.max_connections {
            tracing::warn!(
                addr = %addr,
                peer_location = %loc,
                max = self.max_connections,
                "add_connection: rejecting new connection to enforce cap"
            );
            // Roll back bookkeeping since we're refusing the connection.
            self.location_for_peer.write().remove(&addr);
            if was_reserved {
                self.pending_reservations.write().remove(&addr);
            }
            return;
        }

        if let Some(prev_loc) = previous_location {
            tracing::debug!(
                addr = %addr,
                prev_location = %prev_loc,
                new_location = %loc,
                "add_connection: replacing existing connection for peer"
            );
            let mut cbl = self.connections_by_location.write();
            if let Some(prev_list) = cbl.get_mut(&prev_loc) {
                if let Some(pos) = prev_list
                    .iter()
                    .position(|c| c.location.socket_addr() == Some(addr))
                {
                    prev_list.swap_remove(pos);
                }
                if prev_list.is_empty() {
                    cbl.remove(&prev_loc);
                }
            }
        }

        {
            let mut cbl = self.connections_by_location.write();
            cbl.entry(loc).or_default().push(Connection {
                location: PeerKeyLocation::new(pub_key, addr),
            });
        }
    }

    pub fn update_peer_identity(
        &self,
        old_addr: SocketAddr,
        new_addr: SocketAddr,
        new_pub_key: TransportPublicKey,
    ) -> bool {
        if old_addr == new_addr {
            tracing::debug!(
                addr = %old_addr,
                "update_peer_identity: same address; skipping"
            );
            return false;
        }

        let mut loc_for_peer = self.location_for_peer.write();
        let Some(loc) = loc_for_peer.remove(&old_addr) else {
            tracing::debug!(
                old_addr = %old_addr,
                new_addr = %new_addr,
                "update_peer_identity: old peer entry not found"
            );
            return false;
        };

        tracing::debug!(
            old_addr = %old_addr,
            new_addr = %new_addr,
            peer_location = %loc,
            "Updating peer identity for active connection"
        );
        loc_for_peer.insert(new_addr, loc);
        drop(loc_for_peer);

        let mut cbl = self.connections_by_location.write();
        let entry = cbl.entry(loc).or_default();
        if let Some(conn) = entry
            .iter_mut()
            .find(|conn| conn.location.socket_addr() == Some(old_addr))
        {
            // Update the public key and address to match the new peer
            conn.location.pub_key = new_pub_key.clone();
            conn.location.set_addr(new_addr);
        } else {
            tracing::warn!(
                old_addr = %old_addr,
                peer_location = %loc,
                "update_peer_identity: connection entry missing; creating placeholder"
            );
            entry.push(Connection {
                location: PeerKeyLocation::new(new_pub_key, new_addr),
            });
        }

        true
    }

    fn prune_connection(&self, addr: SocketAddr, is_alive: bool) -> Option<Location> {
        let connection_type = if is_alive { "active" } else { "in transit" };
        tracing::debug!(
            addr = %addr,
            connection_type,
            "Pruning connection"
        );

        let mut locations_for_peer = self.location_for_peer.write();

        let Some(loc) = locations_for_peer.remove(&addr) else {
            if is_alive {
                tracing::debug!("no location found for peer, skip pruning");
                return None;
            } else {
                let removed = self.pending_reservations.write().remove(&addr).is_some();
                if !removed {
                    tracing::warn!(
                        addr = %addr,
                        "prune_connection: no pending reservation to release for in-transit peer"
                    );
                }
            }
            return None;
        };

        let conns = &mut *self.connections_by_location.write();
        if let Some(conns) = conns.get_mut(&loc) {
            if let Some(pos) = conns
                .iter()
                .position(|c| c.location.socket_addr() == Some(addr))
            {
                conns.swap_remove(pos);
            }
        }

        if !is_alive {
            self.pending_reservations.write().remove(&addr);
        }

        Some(loc)
    }

    pub(crate) fn connection_count(&self) -> usize {
        // Count only established connections tracked by location buckets.
        self.connections_by_location
            .read()
            .values()
            .map(|conns| conns.len())
            .sum()
    }

    #[allow(dead_code)]
    pub(super) fn get_open_connections(&self) -> usize {
        self.connection_count()
    }

    #[allow(dead_code)]
    pub(crate) fn get_reserved_connections(&self) -> usize {
        self.pending_reservations.read().len()
    }

    pub fn has_connection_or_pending(&self, addr: SocketAddr) -> bool {
        self.location_for_peer.read().contains_key(&addr)
            || self.pending_reservations.read().contains_key(&addr)
    }

    pub(crate) fn get_connections_by_location(&self) -> BTreeMap<Location, Vec<Connection>> {
        self.connections_by_location.read().clone()
    }

    /// Route an op to the most optimal target.
    pub fn routing(
        &self,
        target: Location,
        requesting: Option<SocketAddr>,
        skip_list: impl Contains<SocketAddr>,
        router: &Router,
    ) -> Option<PeerKeyLocation> {
        let candidates = self.routing_candidates(target, requesting, skip_list);

        if candidates.is_empty() {
            return None;
        }

        router.select_peer(candidates.iter(), target).cloned()
    }

    /// Gather routing candidates after applying skip/transient filters.
    pub fn routing_candidates(
        &self,
        target: Location,
        requesting: Option<SocketAddr>,
        skip_list: impl Contains<SocketAddr>,
    ) -> Vec<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        let candidates: Vec<PeerKeyLocation> = connections
            .values()
            .filter_map(|conns| {
                let conn = conns.choose(&mut rand::rng())?;
                let addr = conn.location.socket_addr()?;
                if self.is_transient(addr) {
                    return None;
                }
                if let Some(requester) = requesting {
                    if requester == addr {
                        return None;
                    }
                }
                (!skip_list.has_element(addr)).then_some(conn.location.clone())
            })
            .collect();

        tracing::debug!(
            total_locations = connections.len(),
            candidates = candidates.len(),
            target_location = %target,
            self_addr = self
                .get_own_addr()
                .as_ref()
                .map(|a| a.to_string())
                .unwrap_or_else(|| "unknown".into()),
            "routing candidates for next hop (non-transient only)"
        );

        candidates
    }

    pub fn num_connections(&self) -> usize {
        let connections = self.connections_by_location.read();
        let total: usize = connections.values().map(|v| v.len()).sum();
        tracing::debug!(
            unique_locations = connections.len(),
            total_connections = total,
            "num_connections called"
        );
        total
    }

    #[allow(dead_code)]
    pub(super) fn connected_peers(&self) -> impl Iterator<Item = SocketAddr> {
        let read = self.location_for_peer.read();
        read.keys().copied().collect::<Vec<_>>().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::rate::Rate;
    use crate::transport::TransportKeypair;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    #[test]
    fn rejects_self_connection() {
        // Create a peer id for our node
        let keypair = TransportKeypair::new();
        let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        // Create connection manager with this address
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                keypair.public().clone(),
                Some(addr),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
        );

        // Verify the connection manager has our address set
        assert_eq!(cm.get_own_addr(), Some(addr));

        // Attempt to accept a connection from ourselves - should be rejected
        let location = Location::new(0.5);
        let accepted = cm.should_accept(location, addr);
        assert!(!accepted, "should_accept must reject self-connection");
    }

    #[test]
    fn accepts_connection_from_different_peer() {
        // Create our node's address
        let own_keypair = TransportKeypair::new();
        let own_addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        // Create connection manager with our address
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                own_keypair.public().clone(),
                Some(own_addr),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
        );

        // Create a different peer
        let other_addr: SocketAddr = "127.0.0.2:8001".parse().unwrap();

        // Attempt to accept a connection from a different peer - should be accepted
        let location = Location::new(0.6);
        let accepted = cm.should_accept(location, other_addr);
        assert!(
            accepted,
            "should_accept must accept connection from different peer"
        );
    }
}
