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
                tracing::warn!(%addr, "should_accept: rejecting self-connection attempt");
                return false;
            }
        }

        tracing::info!("Checking if should accept connection");
        let open = self.connection_count();
        let reserved_before = self.pending_reservations.read().len();

        tracing::info!(
            %addr,
            open,
            reserved_before,
            is_gateway = self.is_gateway,
            min = self.min_connections,
            max = self.max_connections,
            rnd_if_htl_above = self.rnd_if_htl_above,
            "should_accept: evaluating direct acceptance guard"
        );

        if self.is_gateway && (open > 0 || reserved_before > 0) {
            tracing::info!(
                %addr,
                open,
                reserved_before,
                "Gateway evaluating additional direct connection (post-bootstrap)"
            );
        }

        if self.location_for_peer.read().get(&addr).is_some() {
            // We've already accepted this peer (pending or active); treat as a no-op acceptance.
            tracing::debug!(%addr, "Peer already pending/connected; acknowledging acceptance");
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
                    %addr,
                    reserved_before,
                    open,
                    "connection counters would overflow; rejecting connection"
                );
                self.pending_reservations.write().remove(&addr);
                return false;
            }
        };

        if open == 0 {
            tracing::debug!(%addr, "should_accept: first connection -> accepting");
            return true;
        }

        let accepted = if total_conn < self.min_connections {
            tracing::info!(%addr, total_conn, "should_accept: accepted (below min connections)");
            true
        } else if total_conn >= self.max_connections {
            tracing::info!(%addr, total_conn, "should_accept: rejected (max connections reached)");
            false
        } else {
            let accepted = self
                .topology_manager
                .write()
                .evaluate_new_connection(location, Instant::now())
                .unwrap_or(true);

            tracing::info!(
                %addr,
                total_conn,
                accepted,
                "should_accept: topology manager decision"
            );
            accepted
        };
        tracing::info!(
            %addr,
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
            tracing::info!(%addr, total_conn, "should_accept: accepted (reserving spot)");
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
                tracing::info!(
                    %addr,
                    %location,
                    "record_pending_location: location already known"
                );
            }
            Entry::Vacant(v) => {
                tracing::info!(
                    %addr,
                    %location,
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
        tracing::info!(%addr, %loc, %was_reserved, "Adding connection to topology");
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
                %addr,
                %loc,
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
            tracing::info!(
                %addr,
                %prev_loc,
                %loc,
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
            tracing::debug!(%old_addr, "update_peer_identity: same address; skipping");
            return false;
        }

        let mut loc_for_peer = self.location_for_peer.write();
        let Some(loc) = loc_for_peer.remove(&old_addr) else {
            tracing::debug!(
                %old_addr,
                %new_addr,
                "update_peer_identity: old peer entry not found"
            );
            return false;
        };

        tracing::info!(%old_addr, %new_addr, %loc, "Updating peer identity for active connection");
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
                %old_addr,
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
        tracing::debug!(%addr, "Pruning {} connection", connection_type);

        let mut locations_for_peer = self.location_for_peer.write();

        let Some(loc) = locations_for_peer.remove(&addr) else {
            if is_alive {
                tracing::debug!("no location found for peer, skip pruning");
                return None;
            } else {
                let removed = self.pending_reservations.write().remove(&addr).is_some();
                if !removed {
                    tracing::warn!(
                        %addr,
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
            target = %target,
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
    use crate::router::Router;
    use crate::topology::rate::Rate;
    use crate::transport::TransportKeypair;
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    fn make_connection_manager(
        own_addr: Option<SocketAddr>,
        min_conn: usize,
        max_conn: usize,
        is_gateway: bool,
    ) -> ConnectionManager {
        let keypair = TransportKeypair::new();
        let own_location = if let Some(addr) = own_addr {
            AtomicU64::new(u64::from_le_bytes(
                Location::from_address(&addr).as_f64().to_le_bytes(),
            ))
        } else {
            AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()))
        };

        ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            min_conn,
            max_conn,
            7,
            (keypair.public().clone(), own_addr, own_location),
            is_gateway,
            10,
            Duration::from_secs(60),
        )
    }

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    // ============ Basic ConnectionManager tests ============

    #[test]
    fn test_connection_manager_initial_state() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        assert_eq!(cm.connection_count(), 0);
        assert_eq!(cm.get_own_addr(), Some(make_addr(8000)));
        assert!(!cm.is_gateway());
        assert_eq!(cm.min_connections, 5);
        assert_eq!(cm.max_connections, 20);
    }

    #[test]
    fn test_connection_manager_gateway_mode() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);
        assert!(cm.is_gateway());
    }

    #[test]
    fn rejects_self_connection() {
        let keypair = TransportKeypair::new();
        let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

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

        assert_eq!(cm.get_own_addr(), Some(addr));
        let location = Location::new(0.5);
        let accepted = cm.should_accept(location, addr);
        assert!(!accepted, "should_accept must reject self-connection");
    }

    #[test]
    fn accepts_connection_from_different_peer() {
        let own_keypair = TransportKeypair::new();
        let own_addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

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

        let other_addr: SocketAddr = "127.0.0.2:8001".parse().unwrap();
        let location = Location::new(0.6);
        let accepted = cm.should_accept(location, other_addr);
        assert!(
            accepted,
            "should_accept must accept connection from different peer"
        );
    }

    // ============ should_accept tests ============

    #[test]
    fn test_should_accept_below_min_connections() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        // First connection should always be accepted
        let addr1 = make_addr(8001);
        let loc1 = Location::new(0.1);
        assert!(cm.should_accept(loc1, addr1));

        // Below min connections, should accept more
        let addr2 = make_addr(8002);
        let loc2 = Location::new(0.2);
        assert!(cm.should_accept(loc2, addr2));
    }

    #[test]
    fn test_should_accept_at_max_connections() {
        // The should_accept logic calculates total_conn = reserved_before + 1 + open
        // It rejects when total_conn >= max_connections
        // So with max=4 and 3 open connections: total_conn = 0 + 1 + 3 = 4 >= 4, rejected
        // Therefore we can only have max_connections - 1 open before the next is rejected
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 4, false);
        let keypair = TransportKeypair::new();

        // Accept and add first connection (open=1, next attempt: 0+1+1=2 < 4)
        let addr1 = make_addr(8001);
        let loc1 = Location::new(0.1);
        assert!(cm.should_accept(loc1, addr1));
        cm.add_connection(loc1, addr1, keypair.public().clone(), true);

        // Accept and add second connection (open=2, next attempt: 0+1+2=3 < 4)
        let addr2 = make_addr(8002);
        let loc2 = Location::new(0.2);
        assert!(cm.should_accept(loc2, addr2));
        cm.add_connection(loc2, addr2, keypair.public().clone(), true);

        // Accept and add third connection (open=3, next attempt: 0+1+3=4 >= 4)
        let addr3 = make_addr(8003);
        let loc3 = Location::new(0.3);
        assert!(cm.should_accept(loc3, addr3));
        cm.add_connection(loc3, addr3, keypair.public().clone(), true);

        // Fourth connection should be rejected (open=3, total_conn=4 >= max=4)
        let addr4 = make_addr(8004);
        let loc4 = Location::new(0.4);
        assert!(!cm.should_accept(loc4, addr4));
    }

    #[test]
    fn test_should_accept_already_connected_peer() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        // Accept and add connection
        assert!(cm.should_accept(loc, addr));
        cm.add_connection(loc, addr, keypair.public().clone(), true);

        // Same peer should still return true (already connected)
        assert!(cm.should_accept(loc, addr));
    }

    // ============ add_connection / prune_connection tests ============

    #[test]
    fn test_add_connection() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        assert_eq!(cm.connection_count(), 0);
        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert_eq!(cm.connection_count(), 1);
    }

    #[test]
    fn test_prune_alive_connection() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert_eq!(cm.connection_count(), 1);

        let pruned_loc = cm.prune_alive_connection(addr);
        assert_eq!(pruned_loc, Some(loc));
        assert_eq!(cm.connection_count(), 0);
    }

    #[test]
    fn test_prune_nonexistent_connection() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(9999);
        let pruned_loc = cm.prune_alive_connection(addr);
        assert!(pruned_loc.is_none());
    }

    // ============ Transient connection tests ============

    #[test]
    fn test_transient_connection_registration() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        assert_eq!(cm.transient_count(), 0);
        assert!(!cm.is_transient(addr));

        // Register transient
        assert!(cm.try_register_transient(addr, Some(Location::new(0.5))));
        assert_eq!(cm.transient_count(), 1);
        assert!(cm.is_transient(addr));
    }

    #[test]
    fn test_transient_connection_budget_enforcement() {
        // Create manager with budget of 2
        let keypair = TransportKeypair::new();
        let own_addr = make_addr(8000);
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                keypair.public().clone(),
                Some(own_addr),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            2, // transient_budget = 2
            Duration::from_secs(60),
        );

        // First two should succeed
        assert!(cm.try_register_transient(make_addr(8001), None));
        assert!(cm.try_register_transient(make_addr(8002), None));
        assert_eq!(cm.transient_count(), 2);

        // Third should fail (over budget)
        assert!(!cm.try_register_transient(make_addr(8003), None));
        assert_eq!(cm.transient_count(), 2);
    }

    #[test]
    fn test_transient_connection_update_existing() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        assert!(cm.try_register_transient(addr, None));
        assert_eq!(cm.transient_count(), 1);

        // Updating existing should succeed without incrementing count
        assert!(cm.try_register_transient(addr, Some(Location::new(0.3))));
        assert_eq!(cm.transient_count(), 1);
    }

    #[test]
    fn test_drop_transient() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        cm.try_register_transient(addr, Some(Location::new(0.5)));
        assert_eq!(cm.transient_count(), 1);

        let entry = cm.drop_transient(addr);
        assert!(entry.is_some());
        assert_eq!(cm.transient_count(), 0);
        assert!(!cm.is_transient(addr));
    }

    #[test]
    fn test_drop_nonexistent_transient() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let entry = cm.drop_transient(make_addr(9999));
        assert!(entry.is_none());
    }

    // ============ update_location tests ============

    #[test]
    fn test_update_location() {
        let cm = make_connection_manager(None, 1, 10, false);

        // Initially no location
        cm.update_location(Some(Location::new(0.5)));
        // Location should be updated (we can't directly read it, but it shouldn't panic)
    }

    #[test]
    fn test_update_location_to_none() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        cm.update_location(None);
        // Should not panic
    }

    // ============ try_set_own_addr tests ============

    #[test]
    fn test_try_set_own_addr_when_unset() {
        let cm = make_connection_manager(None, 1, 10, false);

        let addr = make_addr(8000);
        let result = cm.try_set_own_addr(addr);
        assert!(result.is_none()); // Returns None when successfully set
        assert_eq!(cm.get_own_addr(), Some(addr));
    }

    #[test]
    fn test_try_set_own_addr_when_already_set() {
        let original_addr = make_addr(8000);
        let cm = make_connection_manager(Some(original_addr), 1, 10, false);

        let new_addr = make_addr(9000);
        let result = cm.try_set_own_addr(new_addr);
        assert_eq!(result, Some(original_addr)); // Returns existing when already set
        assert_eq!(cm.get_own_addr(), Some(original_addr)); // Unchanged
    }

    // ============ update_peer_identity tests ============

    #[test]
    fn test_update_peer_identity() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let new_keypair = TransportKeypair::new();

        let old_addr = make_addr(8001);
        let new_addr = make_addr(8002);
        let loc = Location::new(0.5);

        // Add initial connection
        cm.add_connection(loc, old_addr, keypair.public().clone(), false);
        assert_eq!(cm.connection_count(), 1);

        // Update peer identity
        let updated = cm.update_peer_identity(old_addr, new_addr, new_keypair.public().clone());
        assert!(updated);
        assert_eq!(cm.connection_count(), 1);

        // Old address should no longer be connected
        let connections = cm.get_connections_by_location();
        let conns = connections.get(&loc).unwrap();
        assert_eq!(conns[0].location.socket_addr(), Some(new_addr));
    }

    #[test]
    fn test_update_peer_identity_same_addr() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        cm.add_connection(loc, addr, keypair.public().clone(), false);

        // Same address should return false
        let updated = cm.update_peer_identity(addr, addr, keypair.public().clone());
        assert!(!updated);
    }

    #[test]
    fn test_update_peer_identity_nonexistent() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let old_addr = make_addr(9999);
        let new_addr = make_addr(9998);

        let updated = cm.update_peer_identity(old_addr, new_addr, keypair.public().clone());
        assert!(!updated);
    }

    // ============ routing tests ============

    #[test]
    fn test_routing_empty_connections() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let router = Router::new(&[]);

        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let result = cm.routing(Location::new(0.5), None, &empty_set, &router);
        assert!(result.is_none());
    }

    #[test]
    fn test_routing_with_connections() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let router = Router::new(&[]);

        // Add a connection
        let addr = make_addr(8001);
        let loc = Location::new(0.5);
        cm.add_connection(loc, addr, keypair.public().clone(), false);

        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let result = cm.routing(Location::new(0.5), None, &empty_set, &router);
        assert!(result.is_some());
    }

    #[test]
    fn test_routing_skips_requester() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let router = Router::new(&[]);

        let addr = make_addr(8001);
        let loc = Location::new(0.5);
        cm.add_connection(loc, addr, keypair.public().clone(), false);

        // Request from the same address should not route back to itself
        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let result = cm.routing(Location::new(0.5), Some(addr), &empty_set, &router);
        assert!(result.is_none());
    }

    #[test]
    fn test_routing_skips_skip_list() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let router = Router::new(&[]);

        let addr = make_addr(8001);
        let loc = Location::new(0.5);
        cm.add_connection(loc, addr, keypair.public().clone(), false);

        // Put peer in skip list
        let mut skip_list = HashSet::new();
        skip_list.insert(addr);

        let result = cm.routing(Location::new(0.5), None, &skip_list, &router);
        assert!(result.is_none());
    }

    #[test]
    fn test_routing_skips_transient() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let router = Router::new(&[]);

        let addr = make_addr(8001);
        let loc = Location::new(0.5);
        cm.add_connection(loc, addr, keypair.public().clone(), false);

        // Mark as transient
        cm.try_register_transient(addr, Some(loc));

        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let result = cm.routing(Location::new(0.5), None, &empty_set, &router);
        assert!(result.is_none());
    }

    // ============ get_connections_by_location tests ============

    #[test]
    fn test_get_connections_by_location() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr1 = make_addr(8001);
        let addr2 = make_addr(8002);
        let loc1 = Location::new(0.3);
        let loc2 = Location::new(0.7);

        cm.add_connection(loc1, addr1, keypair.public().clone(), false);
        cm.add_connection(loc2, addr2, keypair.public().clone(), false);

        let connections = cm.get_connections_by_location();
        assert_eq!(connections.len(), 2);
        assert!(connections.contains_key(&loc1));
        assert!(connections.contains_key(&loc2));
    }

    // ============ has_connection_or_pending tests ============

    #[test]
    fn test_has_connection_or_pending() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        assert!(!cm.has_connection_or_pending(addr));

        // After accepting (but not adding), should be pending
        cm.should_accept(loc, addr);
        assert!(cm.has_connection_or_pending(addr));

        // After adding, should still return true
        cm.add_connection(loc, addr, keypair.public().clone(), true);
        assert!(cm.has_connection_or_pending(addr));
    }

    // ============ record_pending_location tests ============

    #[test]
    fn test_record_pending_location() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        cm.record_pending_location(addr, loc);

        // Location should be recorded
        assert!(cm.location_for_peer.read().contains_key(&addr));
    }

    #[test]
    fn test_record_pending_location_already_exists() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        let loc1 = Location::new(0.5);
        let loc2 = Location::new(0.7);

        cm.record_pending_location(addr, loc1);
        cm.record_pending_location(addr, loc2);

        // Should keep original location
        let locations = cm.location_for_peer.read();
        assert_eq!(locations.get(&addr), Some(&loc1));
    }
}
