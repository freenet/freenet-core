//! # Lock Ordering
//!
//! To prevent deadlocks, all `RwLock`-protected fields in [`ConnectionManager`] **must** be
//! acquired in the following order whenever multiple locks are held simultaneously:
//!
//! 1. `location_for_peer`
//! 2. `connections_by_location`
//! 3. `pending_reservations`
//!
//! Acquiring locks in any other order risks an ABBA deadlock. This ordering was established
//! after a deadlock introduced in PR #3091 (fixed in PR #3095), where
//! `cleanup_stale_reservations` acquired `pending_reservations` before `connections_by_location`,
//! while `prune_connection` acquired them in the opposite order.

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{btree_map::Entry, BTreeMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use crate::config::GlobalRng;
use crate::topology::{Limits, TopologyManager};

use super::*;

#[derive(Clone)]
pub(crate) struct TransientEntry {
    /// Entry tracking a transient connection that hasn't been added to the ring topology yet.
    /// Transient connections are typically unsolicited inbound connections to gateways.
    /// Advertised location for the transient peer, if known at admission time.
    pub location: Option<Location>,
}

/// Maximum time a pending reservation can remain before being considered stale.
/// Reservations that exceed this TTL are cleaned up by `cleanup_stale_reservations`
/// to prevent permanent node isolation when CONNECT operations fail to complete.
const PENDING_RESERVATION_TTL: Duration = Duration::from_secs(60);

/// Maximum number of concurrent CONNECT operations a gateway will route simultaneously.
/// This prevents thundering-herd scenarios where many joiners hit the same gateway at once.
/// The value 8 balances throughput (parallel joins) against overload protection. Non-gateways
/// use `usize::MAX` since they see far fewer concurrent connects.
const MAX_CONCURRENT_GATEWAY_CONNECTS: usize = 8;

/// How long a peer address stays in the recently-failed cache after a NAT traversal
/// failure. During this window the address is added to the CONNECT bloom filter so
/// routing nodes avoid selecting the peer as an acceptor.
const FAILED_ADDR_TTL: Duration = Duration::from_secs(300);

/// RAII guard that releases a connect admission slot when dropped.
///
/// This ensures the slot is always released, even when `?` operators cause early returns
/// from error paths. Without this guard, transient errors between `try_admit_connect()`
/// and the manual `release_connect()` call would permanently leak slots, eventually
/// bricking the gateway (with only 8 slots available).
pub(crate) struct ConnectAdmissionGuard {
    connect_in_flight: Arc<AtomicUsize>,
    released: bool,
}

impl ConnectAdmissionGuard {
    fn new(connect_in_flight: Arc<AtomicUsize>) -> Self {
        Self {
            connect_in_flight,
            released: false,
        }
    }

    /// Explicitly release the slot (e.g., when forwarding to the next hop).
    /// After calling this, the Drop impl is a no-op.
    #[cfg(test)]
    pub fn release(mut self) {
        if !self.released {
            self.released = true;
            let prev = self.connect_in_flight.fetch_sub(1, Ordering::SeqCst);
            debug_assert!(prev > 0, "connect_in_flight underflow on explicit release");
        }
    }
}

impl Drop for ConnectAdmissionGuard {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            let prev = self.connect_in_flight.fetch_sub(1, Ordering::SeqCst);
            debug_assert!(prev > 0, "connect_in_flight underflow on drop");
        }
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionManager {
    /// Pending connection reservations, keyed by socket address.
    /// Each entry records the advertised location and the time the reservation was created,
    /// allowing stale entries to be expired via `PENDING_RESERVATION_TTL`.
    pending_reservations: Arc<RwLock<BTreeMap<SocketAddr, (Location, Instant)>>>,
    /// Mapping from socket address to location for established and in-progress connections.
    /// Entries are added by `add_connection` (established) and `record_pending_location`
    /// (speculative, during `should_accept`). Removed by `prune_connection` and
    /// `cleanup_stale_reservations` (orphan sweep).
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
    /// Number of CONNECT operations currently being routed through this node.
    connect_in_flight: Arc<AtomicUsize>,
    /// Maximum concurrent CONNECT operations this node will route.
    /// Gateways use a bounded value; non-gateways use usize::MAX (unlimited).
    max_concurrent_connects: usize,
    /// Addresses that recently failed NAT traversal. Pre-populated into the
    /// CONNECT `visited` bloom filter so routing nodes skip these peers.
    recently_failed_addrs: Arc<RwLock<BTreeMap<SocketAddr, Instant>>>,
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
            connect_in_flight: Arc::new(AtomicUsize::new(0)),
            max_concurrent_connects: if is_gateway {
                MAX_CONCURRENT_GATEWAY_CONNECTS
            } else {
                usize::MAX
            },
            recently_failed_addrs: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Try to admit a new connect operation for routing.
    /// Returns `None` if the node is at capacity for concurrent connects.
    /// Returns `Some(ConnectAdmissionGuard)` on success — the slot is automatically
    /// released when the guard is dropped, preventing leaks on error paths.
    #[must_use]
    pub fn try_admit_connect(&self) -> Option<ConnectAdmissionGuard> {
        loop {
            let current = self.connect_in_flight.load(Ordering::Acquire);
            if current >= self.max_concurrent_connects {
                return None;
            }
            if self
                .connect_in_flight
                .compare_exchange_weak(current, current + 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some(ConnectAdmissionGuard::new(self.connect_in_flight.clone()));
            }
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
        let now = Instant::now();
        let reserved_before = self
            .pending_reservations
            .read()
            .iter()
            .filter(|(_, (_, created))| now.duration_since(*created) <= PENDING_RESERVATION_TTL)
            .count();

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
            pending.insert(addr, (location, Instant::now()));
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
        tracing::debug!(
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

    /// Update this node location, only if not already set.
    ///
    /// This preserves configured locations (set during initialization) while allowing
    /// peers behind NAT to learn their location from the observed address.
    pub fn update_location(&self, loc: Option<Location>) {
        if let Some(loc) = loc {
            // Only update if current location is unset (-1.0)
            let current_bits = self.own_location.load(std::sync::atomic::Ordering::Acquire);
            let current_val = f64::from_le_bytes(current_bits.to_le_bytes());
            if current_val >= 0.0 {
                // Location already set (e.g., from config), don't overwrite
                tracing::debug!(
                    current_location = current_val,
                    new_location = loc.as_f64(),
                    "update_location: preserving existing location"
                );
                return;
            }
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

    /// Returns this node's PeerKeyLocation.
    ///
    /// If the node's external address is not yet known (e.g., peer behind NAT
    /// that hasn't received ObservedAddress yet), returns a PeerKeyLocation
    /// with PeerAddr::Unknown.
    pub fn own_location(&self) -> PeerKeyLocation {
        match self.get_own_addr() {
            Some(addr) => PeerKeyLocation::new((*self.pub_key).clone(), addr),
            None => PeerKeyLocation::with_unknown_addr((*self.pub_key).clone()),
        }
    }

    /// Returns our own socket address if set.
    pub fn get_own_addr(&self) -> Option<SocketAddr> {
        *self.own_addr.lock()
    }

    /// Returns our own socket address, or `RingError::PeerNotJoined` if not yet established.
    pub fn peer_addr(&self) -> Result<SocketAddr, super::RingError> {
        self.get_own_addr().ok_or(super::RingError::PeerNotJoined)
    }

    /// Returns the stored ring location, if set.
    /// This is the location that was set by update_location(), typically from
    /// the externally observed address received via ObservedAddress message.
    pub fn get_stored_location(&self) -> Option<Location> {
        let bits = self.own_location.load(std::sync::atomic::Ordering::Acquire);
        let val = f64::from_le_bytes(bits.to_le_bytes());
        if val < 0.0 {
            None
        } else {
            Some(Location::new(val))
        }
    }

    /// Look up a PeerKeyLocation by socket address from connections_by_location or transient connections.
    pub fn get_peer_by_addr(&self, addr: SocketAddr) -> Option<PeerKeyLocation> {
        // Check connections by location (direct address match)
        let connections = self.connections_by_location.read();
        for conns in connections.values() {
            for conn in conns {
                if conn.location.socket_addr() == Some(addr) {
                    return Some(conn.location.clone());
                }
            }
        }

        // Fallback: the transport address may differ from the advertised address
        // stored in PeerKeyLocation. Use location_for_peer (which maps transport
        // addresses to ring locations) to bridge the gap.
        if let Some(&location) = self.location_for_peer.read().get(&addr) {
            if let Some(conns) = connections.get(&location) {
                if let Some(conn) = conns.first() {
                    tracing::debug!(
                        requested_addr = %addr,
                        resolved_via = "location_for_peer",
                        location = %location,
                        "get_peer_by_addr: resolved via location_for_peer fallback"
                    );
                    return Some(conn.location.clone());
                }
            }
        }

        drop(connections);
        None
    }

    /// Look up the configured Location for a peer by socket address.
    /// This returns the actual ring location the peer was assigned, not the location
    /// computed from IP address (which would be different).
    #[allow(dead_code)] // Available for future use
    pub fn get_configured_location_for_peer(&self, addr: SocketAddr) -> Option<Location> {
        self.location_for_peer.read().get(&addr).copied()
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

    /// Look up a PeerKeyLocation by public key from connections_by_location.
    /// Used for finding connected peers when we only have their public key (e.g., from interest manager).
    pub fn get_peer_by_pub_key(
        &self,
        pub_key: &crate::transport::TransportPublicKey,
    ) -> Option<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        for conns in connections.values() {
            for conn in conns {
                if &conn.location.pub_key == pub_key {
                    return Some(conn.location.clone());
                }
            }
        }
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
            tracing::info!(
                addr = %addr,
                "try_set_own_addr: initialized own address"
            );
            None
        } else {
            tracing::debug!(
                existing = ?*own_addr,
                attempted = %addr,
                "try_set_own_addr: address already set, keeping existing"
            );
            *own_addr
        }
    }

    /// Sets the own address unconditionally.
    /// Used when a peer behind NAT learns their external address from ObservedAddress.
    pub fn set_own_addr(&self, addr: SocketAddr) {
        let mut own_addr = self.own_addr.lock();
        let old_addr = *own_addr;
        *own_addr = Some(addr);
        tracing::debug!(
            old_addr = ?old_addr,
            new_addr = %addr,
            "set_own_addr called"
        );
    }

    pub fn prune_alive_connection(&self, addr: SocketAddr) -> Option<Location> {
        self.prune_connection(addr, true)
    }

    pub fn prune_in_transit_connection(&self, addr: SocketAddr) -> Option<Location> {
        self.prune_connection(addr, false)
    }

    /// Get the duration of an existing connection by address in milliseconds.
    /// Returns None if the connection doesn't exist.
    pub fn get_connection_duration_ms(&self, addr: SocketAddr) -> Option<u64> {
        let loc = {
            let locations_for_peer = self.location_for_peer.read();
            locations_for_peer.get(&addr).cloned()?
        };

        let conns = self.connections_by_location.read();
        if let Some(conns) = conns.get(&loc) {
            for conn in conns {
                if conn.location.socket_addr() == Some(addr) {
                    return Some(conn.duration_ms());
                }
            }
        }
        None
    }

    pub fn add_connection(
        &self,
        loc: Location,
        addr: SocketAddr,
        pub_key: TransportPublicKey,
        was_reserved: bool,
    ) {
        tracing::info!(
            addr = %addr,
            peer_location = %loc,
            was_reserved = %was_reserved,
            "Adding connection to ring topology"
        );
        // Verify we're not adding a connection to ourselves (if we know our own address)
        debug_assert!(self.get_own_addr().map(|own| own != addr).unwrap_or(true));
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
            cbl.entry(loc)
                .or_default()
                .push(Connection::new(PeerKeyLocation::new(pub_key, addr)));
        }

        // Remove from transient connections if present, since we're now a full ring connection.
        if self.transient_connections.remove(&addr).is_some() {
            self.transient_in_use.fetch_sub(1, Ordering::SeqCst);
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
            entry.push(Connection::new(PeerKeyLocation::new(new_pub_key, new_addr)));
        }

        true
    }

    fn prune_connection(&self, addr: SocketAddr, is_alive: bool) -> Option<Location> {
        let connection_type = if is_alive { "active" } else { "in transit" };
        tracing::info!(
            addr = %addr,
            connection_type,
            "Pruning connection from ring topology"
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

        let cbl = &mut *self.connections_by_location.write();
        if let Some(bucket) = cbl.get_mut(&loc) {
            if let Some(pos) = bucket
                .iter()
                .position(|c| c.location.socket_addr() == Some(addr))
            {
                bucket.swap_remove(pos);
                if bucket.is_empty() {
                    cbl.remove(&loc);
                }
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

    /// Remove pending reservations that have exceeded `PENDING_RESERVATION_TTL`,
    /// and clean up orphaned `location_for_peer` entries that have no corresponding
    /// established connection or valid pending reservation.
    ///
    /// Orphaned entries arise when a CONNECT operation times out: `should_accept`
    /// inserts into both `pending_reservations` and `location_for_peer`, but
    /// abort handling may only clean the reservation, leaving a phantom location
    /// entry that permanently blocks gateway retries (see #3088).
    ///
    /// Returns the number of stale entries removed (reservations + orphaned locations).
    pub(crate) fn cleanup_stale_reservations(&self) -> usize {
        // Phase 1: Clean expired pending reservations and snapshot surviving addresses.
        // We release the pending_reservations lock before phase 2 to avoid deadlock:
        // prune_connection acquires location_for_peer(W) → pending_reservations(W),
        // so we must not hold pending_reservations while acquiring location_for_peer.
        let (stale_reservations, valid_pending_addrs) = {
            let now = Instant::now();
            let mut pending = self.pending_reservations.write();
            let before = pending.len();
            pending.retain(|addr, (_loc, created)| {
                let age = now.duration_since(*created);
                if age > PENDING_RESERVATION_TTL {
                    tracing::warn!(
                        addr = %addr,
                        age_secs = age.as_secs(),
                        "Removing stale pending reservation"
                    );
                    false
                } else {
                    true
                }
            });
            let stale = before - pending.len();
            let valid: Vec<SocketAddr> = pending.keys().copied().collect();
            (stale, valid)
        }; // pending_reservations lock released

        // Phase 2: Clean orphaned location_for_peer entries — addresses with no
        // established connection and no valid pending reservation. These phantoms
        // cause has_connection_or_pending() to permanently return true (#3088).
        //
        // Lock ordering matches prune_connection: location_for_peer(W) first,
        // then connections_by_location(R). No pending_reservations lock held.
        let orphaned_locations = {
            let mut locations = self.location_for_peer.write();
            let conns = self.connections_by_location.read();
            let before = locations.len();
            locations.retain(|addr, loc| {
                // Keep if there's an established connection at this location for this address
                if conns.get(loc).is_some_and(|conn_list| {
                    conn_list
                        .iter()
                        .any(|c| c.location.socket_addr() == Some(*addr))
                }) {
                    return true;
                }
                // Keep if there's a valid pending reservation (using phase 1 snapshot)
                if valid_pending_addrs.contains(addr) {
                    return true;
                }
                tracing::warn!(
                    addr = %addr,
                    location = %loc,
                    "Removing orphaned location_for_peer entry (no connection or reservation)"
                );
                false
            });
            before - locations.len()
        };

        stale_reservations + orphaned_locations
    }

    /// Check whether a peer address has an established connection in `connections_by_location`.
    /// Unlike `has_connection_or_pending`, this checks only fully established ring connections.
    pub fn is_in_ring(&self, addr: SocketAddr) -> bool {
        let connections = self.connections_by_location.read();
        connections
            .values()
            .any(|conns| conns.iter().any(|c| c.location.socket_addr() == Some(addr)))
    }

    pub fn has_connection_or_pending(&self, addr: SocketAddr) -> bool {
        if self.location_for_peer.read().contains_key(&addr) {
            return true;
        }
        let pending = self.pending_reservations.read();
        if let Some((_loc, created)) = pending.get(&addr) {
            return Instant::now().duration_since(*created) <= PENDING_RESERVATION_TTL;
        }
        false
    }

    pub(crate) fn inject_reservation(
        &self,
        addr: SocketAddr,
        location: Location,
        created: Instant,
    ) {
        self.pending_reservations
            .write()
            .insert(addr, (location, created));
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
        let (peer, _decision) = self.routing_with_telemetry(target, requesting, skip_list, router);
        peer
    }

    /// Route an op to the most optimal target, returning telemetry about the decision.
    pub fn routing_with_telemetry(
        &self,
        target: Location,
        requesting: Option<SocketAddr>,
        skip_list: impl Contains<SocketAddr>,
        router: &Router,
    ) -> (
        Option<PeerKeyLocation>,
        Option<crate::router::RoutingDecisionInfo>,
    ) {
        let candidates = self.routing_candidates(target, requesting, skip_list);

        if candidates.is_empty() {
            return (None, None);
        }

        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(candidates.iter(), target, 1);
        let peer = selected.into_iter().next().cloned();
        (peer, Some(decision))
    }

    /// Gather routing candidates after applying skip/transient filters.
    pub fn routing_candidates(
        &self,
        target: Location,
        requesting: Option<SocketAddr>,
        skip_list: impl Contains<SocketAddr>,
    ) -> Vec<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        // Sort keys for deterministic iteration order (HashMap iteration is non-deterministic)
        // This ensures GlobalRng is called in the same order across runs
        let mut sorted_keys: Vec<_> = connections.keys().collect();
        sorted_keys.sort();
        let candidates: Vec<PeerKeyLocation> = sorted_keys
            .into_iter()
            .filter_map(|loc| connections.get(loc))
            .filter_map(|conns| {
                // Sort connections for deterministic selection
                // (Vec ordering may vary based on async connection establishment order)
                let mut sorted_conns: Vec<_> = conns.iter().collect();
                sorted_conns.sort_by_key(|c| c.location.clone());
                let conn = GlobalRng::choose(&sorted_conns)?;
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

    /// Record that a transport-level connection to `addr` failed, so future
    /// CONNECT requests will mark it as visited in the bloom filter.
    pub fn record_failed_addr(&self, addr: SocketAddr) {
        self.recently_failed_addrs
            .write()
            .insert(addr, Instant::now());
    }

    /// Remove `addr` from the failed cache (e.g., after a successful connection).
    pub fn clear_failed_addr(&self, addr: SocketAddr) {
        self.recently_failed_addrs.write().remove(&addr);
    }

    /// Return addresses that failed NAT traversal within `FAILED_ADDR_TTL`.
    pub fn recently_failed_addrs(&self) -> Vec<SocketAddr> {
        let now = Instant::now();
        self.recently_failed_addrs
            .read()
            .iter()
            .filter(|(_, ts)| now.duration_since(**ts) < FAILED_ADDR_TTL)
            .map(|(addr, _)| *addr)
            .collect()
    }

    /// Remove entries older than `FAILED_ADDR_TTL`.
    pub fn cleanup_stale_failed_addrs(&self) -> usize {
        let now = Instant::now();
        let mut map = self.recently_failed_addrs.write();
        let before = map.len();
        map.retain(|_, ts| now.duration_since(*ts) < FAILED_ADDR_TTL);
        before - map.len()
    }

    /// Clear all recently-failed addresses. Used after suspend/resume when
    /// previously-unreachable peers may be reachable again.
    pub fn cleanup_all_failed_addrs(&self) {
        self.recently_failed_addrs.write().clear();
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

    fn age_reservation(cm: &ConnectionManager, addr: SocketAddr, age: Duration) {
        let mut pending = cm.pending_reservations.write();
        if let Some(entry) = pending.get_mut(&addr) {
            entry.1 = Instant::now() - age;
        }
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

    /// Test that should_accept rejects connections from own address
    ///
    /// **Bug scenario prevented (#1806, #1786, #1781, #1827):**
    /// A node must never accept a connection from itself. This can happen when:
    /// - A node's external address is incorrectly resolved to itself
    /// - NAT reflection causes packets to loop back
    /// - Gateway advertises its own address to peers
    ///
    /// Self-connections cause infinite routing loops and state corruption.
    /// This is the PRIMARY defense layer (runs in both debug and release).
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

    #[test]
    fn test_update_location_preserves_existing() {
        // Issue #2773: Once a location is set, subsequent update_location calls
        // should NOT overwrite it. This ensures distance-based tie-breaker uses
        // consistent locations throughout the peer's lifetime.
        let cm = make_connection_manager(None, 1, 10, false);

        // Set initial location
        let initial_loc = Location::new(0.3);
        cm.update_location(Some(initial_loc));
        assert_eq!(cm.get_stored_location(), Some(initial_loc));

        // Try to update to different location - should be ignored
        let new_loc = Location::new(0.7);
        cm.update_location(Some(new_loc));
        assert_eq!(
            cm.get_stored_location(),
            Some(initial_loc),
            "Location should be preserved, not overwritten"
        );
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

    // ============ Self-routing prevention tests ============
    //
    // These tests prevent regression of self-routing bugs that caused infinite loops
    // and network congestion. The bugs manifested in several ways:
    //
    // - #1806: Nodes routing messages back to themselves, creating routing loops
    // - #1786: Connection state inconsistency when a node appeared in its own peer list
    // - #1781: PUT operations failing due to self-connections in the routing path
    // - #1827: Gateway nodes incorrectly routing to themselves under high load
    //
    // The fix involves multiple defense layers:
    // 1. should_accept() rejects connection attempts from own address
    // 2. add_connection() has a debug_assert to catch violations in development
    // 3. routing_candidates() filters out the requester to prevent echo routing

    /// Test that add_connection rejects own address (debug mode only)
    ///
    /// **Bug scenario prevented (#1806, #1781, #1827):**
    /// A node could accidentally add itself to its own connection list, typically
    /// when processing connection messages with incorrectly resolved addresses.
    /// This caused infinite routing loops where a node would route to itself,
    /// then route again to itself, consuming CPU and blocking real operations.
    ///
    /// **Why debug-only is acceptable:**
    /// This test verifies a debug_assert that catches programming errors during
    /// development. In release builds, the should_accept() check (which IS tested
    /// in release mode) provides the primary defense. The debug_assert is a
    /// secondary safety net for catching bugs that bypass should_accept().
    ///
    /// **Note:** The should_accept() guard is tested in `rejects_self_connection`
    /// which runs in both debug and release modes.
    #[test]
    #[should_panic(expected = "assertion failed")]
    #[cfg(debug_assertions)]
    fn test_add_connection_rejects_own_address() {
        let own_addr = make_addr(8000);
        let cm = make_connection_manager(Some(own_addr), 1, 10, false);
        let keypair = TransportKeypair::new();
        let own_loc = Location::new(0.5);

        // This should panic in debug mode due to the debug_assert
        cm.add_connection(own_loc, own_addr, keypair.public().clone(), false);
    }

    /// Test that routing_candidates() respects requester parameter
    ///
    /// **Bug scenario prevented (#1806, #1786):**
    /// When processing a routing request, if the node sent the response back
    /// to the original requester as a "next hop", this created echo patterns
    /// where messages bounced between two nodes indefinitely. The requester
    /// parameter ensures we never route back to whoever sent us the request.
    ///
    /// **How the original bug manifested:**
    /// Node A sends GET to Node B. Node B, when selecting routing candidates,
    /// would sometimes select Node A as the best candidate (if A was close to
    /// the target location). This caused the GET to bounce back to A, which
    /// would send it back to B, creating a ping-pong loop.
    #[test]
    fn test_routing_candidates_respects_requester() {
        let own_addr = make_addr(8000);
        let cm = make_connection_manager(Some(own_addr), 1, 10, false);
        let keypair = TransportKeypair::new();

        // Add several peers
        let requester_addr = make_addr(8001);
        let requester_loc = Location::new(0.3);
        cm.add_connection(
            requester_loc,
            requester_addr,
            keypair.public().clone(),
            false,
        );

        let peer2_addr = make_addr(8002);
        let peer2_loc = Location::new(0.5);
        cm.add_connection(peer2_loc, peer2_addr, keypair.public().clone(), false);

        let peer3_addr = make_addr(8003);
        let peer3_loc = Location::new(0.7);
        cm.add_connection(peer3_loc, peer3_addr, keypair.public().clone(), false);

        // Get routing candidates with requester specified
        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let target = Location::new(0.5);
        let candidates = cm.routing_candidates(target, Some(requester_addr), &empty_set);

        // Should have 2 candidates (excluding requester)
        assert_eq!(
            candidates.len(),
            2,
            "routing_candidates should exclude requester"
        );

        // Verify requester is not in candidates
        for peer in &candidates {
            let addr = peer.socket_addr().expect("Peer should have address");
            assert_ne!(
                addr, requester_addr,
                "routing_candidates must not include requester address"
            );
        }

        // Verify other peers are included
        let addrs: HashSet<SocketAddr> =
            candidates.iter().filter_map(|p| p.socket_addr()).collect();
        assert!(addrs.contains(&peer2_addr));
        assert!(addrs.contains(&peer3_addr));
    }

    /// Test that should_accept rejects self-connection with IPv6 addresses
    ///
    /// **Bug scenario prevented (#1806, #1786):**
    /// Self-connection rejection must work regardless of address format.
    /// Early implementations only checked IPv4 addresses, allowing IPv6
    /// loopback connections to create self-loops.
    ///
    /// This test extends `rejects_self_connection` to cover IPv6 addresses.
    #[test]
    fn test_should_accept_rejects_own_addr_ipv6() {
        let keypair = TransportKeypair::new();

        // Test with IPv6 loopback
        let ipv6_addr: SocketAddr = "[::1]:8000".parse().unwrap();
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                keypair.public().clone(),
                Some(ipv6_addr),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
        );

        assert_eq!(cm.get_own_addr(), Some(ipv6_addr));
        let location = Location::new(0.5);
        let accepted = cm.should_accept(location, ipv6_addr);
        assert!(
            !accepted,
            "should_accept must reject self-connection for IPv6 addresses"
        );

        // Verify different address is accepted
        let other_addr: SocketAddr = "[::1]:8001".parse().unwrap();
        let accepted_other = cm.should_accept(location, other_addr);
        assert!(
            accepted_other,
            "should_accept must accept connection from different IPv6 address"
        );
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

    #[test]
    fn test_peer_addr_returns_address_when_joined() {
        let addr = make_addr(8000);
        let cm = make_connection_manager(Some(addr), 5, 20, false);

        let result = cm.peer_addr();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), addr);
    }

    #[test]
    fn test_peer_addr_returns_error_when_not_joined() {
        let cm = make_connection_manager(None, 5, 20, false);

        let result = cm.peer_addr();
        assert!(result.is_err());
        assert!(matches!(result, Err(super::RingError::PeerNotJoined)));
    }

    #[test]
    fn test_peer_addr_is_consistent_with_get_own_addr() {
        // When get_own_addr returns Some, peer_addr should return Ok with same value
        let addr = make_addr(9000);
        let cm = make_connection_manager(Some(addr), 5, 20, false);
        assert_eq!(cm.get_own_addr(), Some(addr));
        assert_eq!(cm.peer_addr().unwrap(), addr);

        // When get_own_addr returns None, peer_addr should return Err
        let cm_no_addr = make_connection_manager(None, 5, 20, false);
        assert_eq!(cm_no_addr.get_own_addr(), None);
        assert!(cm_no_addr.peer_addr().is_err());
    }

    // ============ pending_reservations TTL tests ============

    #[test]
    fn test_stale_pending_reservations_are_cleaned_up() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        let addr1 = make_addr(8001);
        let addr2 = make_addr(8002);
        let loc1 = Location::new(0.1);
        let loc2 = Location::new(0.2);

        // Create reservations via should_accept
        assert!(cm.should_accept(loc1, addr1));
        assert!(cm.should_accept(loc2, addr2));
        assert_eq!(cm.get_reserved_connections(), 2);

        // Cleanup should remove nothing when entries are fresh
        let removed = cm.cleanup_stale_reservations();
        assert_eq!(removed, 0);
        assert_eq!(cm.get_reserved_connections(), 2);

        // Backdate one entry to simulate expiration
        {
            let mut pending = cm.pending_reservations.write();
            if let Some(entry) = pending.get_mut(&addr1) {
                entry.1 = Instant::now() - Duration::from_secs(120);
            }
        }

        // Cleanup should remove only the stale entry
        let removed = cm.cleanup_stale_reservations();
        assert_eq!(removed, 1);
        assert_eq!(cm.get_reserved_connections(), 1);
        assert!(!cm.has_connection_or_pending(addr1));
        assert!(cm.has_connection_or_pending(addr2));
    }

    #[test]
    fn test_has_connection_or_pending_ignores_expired_reservation() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        // Create a reservation via should_accept
        assert!(cm.should_accept(loc, addr));
        assert!(cm.has_connection_or_pending(addr));

        // Backdate the reservation to simulate expiration
        {
            let mut pending = cm.pending_reservations.write();
            if let Some(entry) = pending.get_mut(&addr) {
                entry.1 = Instant::now() - Duration::from_secs(120);
            }
        }

        // Expired reservation should be invisible
        assert!(
            !cm.has_connection_or_pending(addr),
            "has_connection_or_pending should ignore expired reservations"
        );
    }

    #[test]
    fn test_has_connection_or_pending_sees_fresh_reservation() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        assert!(cm.should_accept(loc, addr));
        assert!(
            cm.has_connection_or_pending(addr),
            "has_connection_or_pending should see fresh reservations"
        );
    }

    #[test]
    fn test_has_connection_or_pending_always_sees_established() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        // Add an established connection
        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert!(
            cm.has_connection_or_pending(addr),
            "has_connection_or_pending should always see established connections"
        );
    }

    #[test]
    fn test_has_connection_or_pending_ttl_boundary() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(8001);
        assert!(cm.should_accept(Location::new(0.1), addr));

        age_reservation(&cm, addr, PENDING_RESERVATION_TTL - Duration::from_secs(1));
        assert!(cm.has_connection_or_pending(addr));

        age_reservation(&cm, addr, PENDING_RESERVATION_TTL + Duration::from_secs(1));
        assert!(!cm.has_connection_or_pending(addr));
    }

    #[test]
    fn test_re_reservation_after_expiry() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        assert!(cm.should_accept(loc, addr));
        assert!(cm.has_connection_or_pending(addr));

        age_reservation(&cm, addr, Duration::from_secs(120));
        assert!(!cm.has_connection_or_pending(addr));

        assert!(cm.should_accept(loc, addr));
        assert!(cm.has_connection_or_pending(addr));
    }

    #[test]
    fn test_cleanup_frees_slots_for_new_connections() {
        let cm = make_connection_manager(Some(make_addr(8000)), 2, 3, false);
        let addr1 = make_addr(8001);
        let addr2 = make_addr(8002);
        let addr3 = make_addr(8003);

        assert!(cm.should_accept(Location::new(0.1), addr1));
        assert!(cm.should_accept(Location::new(0.2), addr2));

        age_reservation(&cm, addr1, Duration::from_secs(120));
        assert_eq!(cm.cleanup_stale_reservations(), 1);

        assert!(cm.should_accept(Location::new(0.3), addr3));
    }

    // ============ Admission control tests ============

    #[test]
    fn test_admission_control_gateway_limit() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);

        // Gateway should have max_concurrent_connects = MAX_CONCURRENT_GATEWAY_CONNECTS
        let mut guards = Vec::new();
        for _ in 0..MAX_CONCURRENT_GATEWAY_CONNECTS {
            let guard = cm.try_admit_connect();
            assert!(guard.is_some(), "should admit within limit");
            guards.push(guard.unwrap());
        }
        assert!(
            cm.try_admit_connect().is_none(),
            "should reject when at capacity"
        );
    }

    #[test]
    fn test_admission_control_release_frees_slot() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);

        // Fill up all slots
        let mut guards = Vec::new();
        for _ in 0..MAX_CONCURRENT_GATEWAY_CONNECTS {
            guards.push(cm.try_admit_connect().unwrap());
        }
        assert!(cm.try_admit_connect().is_none());

        // Release one slot by dropping the guard
        guards.pop();
        assert!(
            cm.try_admit_connect().is_some(),
            "should admit after release"
        );
    }

    #[test]
    fn test_admission_control_non_gateway_unlimited() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        // Non-gateway should have unlimited admission — hold guards to keep slots occupied
        let guards: Vec<_> = (0..100)
            .map(|_| {
                cm.try_admit_connect()
                    .expect("non-gateway should always admit")
            })
            .collect();
        assert_eq!(guards.len(), 100);
    }

    #[test]
    fn test_admission_guard_explicit_release() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);

        let guard = cm.try_admit_connect().unwrap();
        assert_eq!(cm.connect_in_flight.load(Ordering::SeqCst), 1);

        // Explicit release
        guard.release();
        assert_eq!(cm.connect_in_flight.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_admission_guard_drop_release() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);

        {
            let _guard = cm.try_admit_connect().unwrap();
            assert_eq!(cm.connect_in_flight.load(Ordering::SeqCst), 1);
        }
        // Guard dropped, slot should be released
        assert_eq!(cm.connect_in_flight.load(Ordering::SeqCst), 0);
    }

    // ============ Phantom location_for_peer cleanup tests (#3088) ============

    /// Regression test for #3088: When open > 0, should_accept inserts into both
    /// pending_reservations and location_for_peer. If the CONNECT times out and only
    /// the reservation is cleaned, a phantom location_for_peer entry remains, causing
    /// has_connection_or_pending to permanently return true for that address.
    #[test]
    fn test_cleanup_removes_orphaned_location_for_peer() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let existing_addr = make_addr(8001);
        let phantom_addr = make_addr(8002);
        let phantom_loc = Location::new(0.5);

        // Establish one real connection so open > 0
        cm.add_connection(
            Location::new(0.1),
            existing_addr,
            keypair.public().clone(),
            false,
        );
        assert_eq!(cm.connection_count(), 1);

        // should_accept with open > 0 calls record_pending_location,
        // creating entries in both pending_reservations and location_for_peer
        assert!(cm.should_accept(phantom_loc, phantom_addr));
        assert!(cm.has_connection_or_pending(phantom_addr));

        // Simulate what happens when a CONNECT times out: the pending reservation
        // expires but location_for_peer is not cleaned (the bug)
        age_reservation(&cm, phantom_addr, Duration::from_secs(120));

        // Before the fix, has_connection_or_pending would still return true
        // because location_for_peer still has the entry
        assert!(
            cm.location_for_peer.read().contains_key(&phantom_addr),
            "phantom entry should exist in location_for_peer"
        );

        // cleanup_stale_reservations should now also clean the orphaned location
        let removed = cm.cleanup_stale_reservations();
        assert_eq!(
            removed, 2,
            "should remove exactly 1 stale reservation + 1 orphaned location"
        );

        assert!(
            !cm.location_for_peer.read().contains_key(&phantom_addr),
            "phantom location_for_peer entry should be removed"
        );
        assert!(
            !cm.has_connection_or_pending(phantom_addr),
            "has_connection_or_pending should return false after cleanup"
        );

        // The established connection should be untouched
        assert!(cm.has_connection_or_pending(existing_addr));
        assert_eq!(cm.connection_count(), 1);
    }

    /// Verify that cleanup_stale_reservations does NOT remove location_for_peer
    /// entries that have a corresponding established connection.
    #[test]
    fn test_cleanup_preserves_established_connection_locations() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let addr = make_addr(8001);
        let loc = Location::new(0.3);

        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert!(cm.location_for_peer.read().contains_key(&addr));

        let removed = cm.cleanup_stale_reservations();
        assert_eq!(removed, 0);
        assert!(cm.location_for_peer.read().contains_key(&addr));
        assert!(cm.has_connection_or_pending(addr));
    }

    /// Verify that a peer can be promoted to ring after its transient entry expires.
    /// This is the key scenario from #3113: transient TTL fires before CONNECT completes,
    /// removing the tracking entry but preserving the transport. When CONNECT succeeds,
    /// the peer must still be added to ring topology.
    #[test]
    fn test_expired_transient_promotion() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        // Step 1: Register transient connection (simulates inbound connection)
        assert!(cm.try_register_transient(addr, Some(loc)));
        assert!(cm.is_transient(addr));
        assert!(!cm.is_in_ring(addr));

        // Step 2: Drop transient (simulates TTL expiry)
        let entry = cm.drop_transient(addr);
        assert!(entry.is_some());
        assert!(!cm.is_transient(addr));
        assert!(!cm.is_in_ring(addr));

        // Step 3: CONNECT succeeds — add_connection should work
        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert!(cm.is_in_ring(addr));
        assert_eq!(cm.connection_count(), 1);
    }

    /// Verify that prune_in_transit_connection clears the phantom entry directly,
    /// as called from handle_aborted_op (Fix 2 of #3088).
    #[test]
    fn test_prune_in_transit_clears_phantom_location() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let existing_addr = make_addr(8001);
        let phantom_addr = make_addr(8002);

        // Establish one real connection so open > 0
        cm.add_connection(
            Location::new(0.1),
            existing_addr,
            keypair.public().clone(),
            false,
        );

        // Accept a new peer (creates location_for_peer + pending_reservations entries)
        assert!(cm.should_accept(Location::new(0.5), phantom_addr));
        assert!(cm.has_connection_or_pending(phantom_addr));

        // prune_in_transit_connection should clear the location_for_peer entry
        cm.prune_in_transit_connection(phantom_addr);

        assert!(
            !cm.location_for_peer.read().contains_key(&phantom_addr),
            "prune_in_transit_connection should remove the location_for_peer entry"
        );
        // prune_connection(addr, false) also removes the pending_reservations entry,
        // so has_connection_or_pending should now return false
        assert!(
            !cm.has_connection_or_pending(phantom_addr),
            "has_connection_or_pending should return false after prune_in_transit_connection"
        );
    }
}
