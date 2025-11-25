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
    pending_reservations: Arc<RwLock<BTreeMap<PeerId, Location>>>,
    pub(super) location_for_peer: Arc<RwLock<BTreeMap<PeerId, Location>>>,
    pub(super) topology_manager: Arc<RwLock<TopologyManager>>,
    connections_by_location: Arc<RwLock<BTreeMap<Location, Vec<Connection>>>>,
    /// Interim connections ongoing handshake or successfully open connections
    /// Is important to keep track of this so no more connections are accepted prematurely.
    own_location: Arc<AtomicU64>,
    peer_key: Arc<Mutex<Option<PeerId>>>,
    is_gateway: bool,
    transient_connections: Arc<DashMap<PeerId, TransientEntry>>,
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
        } else if let Some(peer_key) = &config.peer_id {
            // if the peer id is set, then the location must be set, since it is a gateway
            let location = Location::from_address(&peer_key.addr);
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
                config.peer_id.clone(),
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
        (pub_key, peer_id, own_location): (TransportPublicKey, Option<PeerId>, AtomicU64),
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
            peer_key: Arc::new(Mutex::new(peer_id)),
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
    pub fn should_accept(&self, location: Location, peer_id: &PeerId) -> bool {
        // Don't accept connections from ourselves
        // Primary check: compare full PeerId (address-based equality)
        if let Some(own_id) = self.get_peer_key() {
            if &own_id == peer_id {
                tracing::warn!(%peer_id, "should_accept: rejecting self-connection attempt (address match)");
                return false;
            }
        }

        // Secondary check: compare pub_key directly
        // This catches self-connections even when peer_key is not yet set,
        // which can happen early in initialization. Same pub_key means same node.
        if *self.pub_key == peer_id.pub_key {
            tracing::warn!(
                %peer_id,
                "should_accept: rejecting self-connection attempt (pub_key match)"
            );
            return false;
        }

        tracing::info!("Checking if should accept connection");
        let open = self.connection_count();
        let reserved_before = self.pending_reservations.read().len();

        tracing::info!(
            %peer_id,
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
                %peer_id,
                open,
                reserved_before,
                "Gateway evaluating additional direct connection (post-bootstrap)"
            );
        }

        if self.location_for_peer.read().get(peer_id).is_some() {
            // We've already accepted this peer (pending or active); treat as a no-op acceptance.
            tracing::debug!(%peer_id, "Peer already pending/connected; acknowledging acceptance");
            return true;
        }

        {
            let mut pending = self.pending_reservations.write();
            pending.insert(peer_id.clone(), location);
        }

        let total_conn = match reserved_before
            .checked_add(1)
            .and_then(|val| val.checked_add(open))
        {
            Some(val) => val,
            None => {
                tracing::error!(
                    %peer_id,
                    reserved_before,
                    open,
                    "connection counters would overflow; rejecting connection"
                );
                self.pending_reservations.write().remove(peer_id);
                return false;
            }
        };

        if open == 0 {
            tracing::debug!(%peer_id, "should_accept: first connection -> accepting");
            return true;
        }

        let accepted = if total_conn < self.min_connections {
            tracing::info!(%peer_id, total_conn, "should_accept: accepted (below min connections)");
            true
        } else if total_conn >= self.max_connections {
            tracing::info!(%peer_id, total_conn, "should_accept: rejected (max connections reached)");
            false
        } else {
            let accepted = self
                .topology_manager
                .write()
                .evaluate_new_connection(location, Instant::now())
                .unwrap_or(true);

            tracing::info!(
                %peer_id,
                total_conn,
                accepted,
                "should_accept: topology manager decision"
            );
            accepted
        };
        tracing::info!(
            %peer_id,
            accepted,
            total_conn,
            open_connections = open,
            reserved_connections = self.pending_reservations.read().len(),
            max_connections = self.max_connections,
            min_connections = self.min_connections,
            "should_accept: final decision"
        );
        if !accepted {
            self.pending_reservations.write().remove(peer_id);
        } else {
            tracing::info!(%peer_id, total_conn, "should_accept: accepted (reserving spot)");
            self.record_pending_location(peer_id, location);
        }
        accepted
    }

    /// Record the advertised location for a peer that we have decided to accept.
    ///
    /// This makes the peer discoverable to the routing layer even before the connection
    /// is fully established. The entry is removed automatically if the handshake fails
    /// via `prune_in_transit_connection`.
    pub fn record_pending_location(&self, peer_id: &PeerId, location: Location) {
        let mut locations = self.location_for_peer.write();
        let entry = locations.entry(peer_id.clone());
        match entry {
            Entry::Occupied(_) => {
                tracing::info!(
                    %peer_id,
                    %location,
                    "record_pending_location: location already known"
                );
            }
            Entry::Vacant(v) => {
                tracing::info!(
                    %peer_id,
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

    /// Returns this node location in the ring, if any (must have join the ring already).
    ///
    /// # Panic
    ///
    /// Will panic if the node has no peer id assigned yet.
    pub fn own_location(&self) -> PeerKeyLocation {
        let location = f64::from_le_bytes(
            self.own_location
                .load(std::sync::atomic::Ordering::Acquire)
                .to_le_bytes(),
        );
        let location = if (location - -1f64).abs() < f64::EPSILON {
            None
        } else {
            Some(Location::new(location))
        };
        let peer = self.get_peer_key().expect("peer key not set");
        PeerKeyLocation { peer, location }
    }

    pub fn get_peer_key(&self) -> Option<PeerId> {
        self.peer_key.lock().clone()
    }

    pub fn is_gateway(&self) -> bool {
        self.is_gateway
    }

    /// Attempts to register a transient connection, enforcing the configured budget.
    /// Returns `false` when the budget is exhausted, leaving the map unchanged.
    pub fn try_register_transient(&self, peer: PeerId, location: Option<Location>) -> bool {
        if self.transient_connections.contains_key(&peer) {
            if let Some(mut entry) = self.transient_connections.get_mut(&peer) {
                entry.location = location;
            }
            return true;
        }

        let current = self.transient_in_use.load(Ordering::Acquire);
        if current >= self.transient_budget {
            return false;
        }

        let key = peer.clone();
        self.transient_connections
            .insert(peer, TransientEntry { location });
        let prev = self.transient_in_use.fetch_add(1, Ordering::SeqCst);
        if prev >= self.transient_budget {
            // Undo if we raced past the budget.
            self.transient_connections.remove(&key);
            self.transient_in_use.fetch_sub(1, Ordering::SeqCst);
            return false;
        }

        true
    }

    /// Drops a transient connection and returns its metadata, if it existed.
    /// Also decrements the transient budget counter.
    pub fn drop_transient(&self, peer: &PeerId) -> Option<TransientEntry> {
        let removed = self
            .transient_connections
            .remove(peer)
            .map(|(_, entry)| entry);
        if removed.is_some() {
            self.transient_in_use.fetch_sub(1, Ordering::SeqCst);
        }
        removed
    }

    /// Check whether a peer is currently tracked as transient.
    pub fn is_transient(&self, peer: &PeerId) -> bool {
        self.transient_connections.contains_key(peer)
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

    /// Sets the peer id if is not already set, or returns the current peer id.
    pub fn try_set_peer_key(&self, addr: SocketAddr) -> Option<PeerId> {
        let mut this_peer = self.peer_key.lock();
        if this_peer.is_none() {
            *this_peer = Some(PeerId::new(addr, (*self.pub_key).clone()));
            None
        } else {
            this_peer.clone()
        }
    }

    pub fn prune_alive_connection(&self, peer: &PeerId) -> Option<Location> {
        self.prune_connection(peer, true)
    }

    pub fn prune_in_transit_connection(&self, peer: &PeerId) -> Option<Location> {
        self.prune_connection(peer, false)
    }

    pub fn add_connection(&self, loc: Location, peer: PeerId, was_reserved: bool) {
        tracing::info!(%peer, %loc, %was_reserved, "Adding connection to topology");
        debug_assert!(self.get_peer_key().expect("should be set") != peer);
        if was_reserved {
            self.pending_reservations.write().remove(&peer);
        }
        let mut lop = self.location_for_peer.write();
        let previous_location = lop.insert(peer.clone(), loc);
        drop(lop);

        // Enforce the global cap when adding a new peer (relocations reuse the existing slot).
        if previous_location.is_none() && self.connection_count() >= self.max_connections {
            tracing::warn!(
                %peer,
                %loc,
                max = self.max_connections,
                "add_connection: rejecting new connection to enforce cap"
            );
            // Roll back bookkeeping since we're refusing the connection.
            self.location_for_peer.write().remove(&peer);
            if was_reserved {
                self.pending_reservations.write().remove(&peer);
            }
            return;
        }

        if let Some(prev_loc) = previous_location {
            tracing::info!(
                %peer,
                %prev_loc,
                %loc,
                "add_connection: replacing existing connection for peer"
            );
            let mut cbl = self.connections_by_location.write();
            if let Some(prev_list) = cbl.get_mut(&prev_loc) {
                if let Some(pos) = prev_list.iter().position(|c| c.location.peer == peer) {
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
                location: PeerKeyLocation {
                    peer: peer.clone(),
                    location: Some(loc),
                },
            });
        }
    }

    pub fn update_peer_identity(&self, old_peer: &PeerId, new_peer: PeerId) -> bool {
        if old_peer.addr == new_peer.addr && old_peer.pub_key == new_peer.pub_key {
            tracing::debug!(%old_peer, "update_peer_identity: identical peers; skipping");
            return false;
        }

        let mut loc_for_peer = self.location_for_peer.write();
        let Some(loc) = loc_for_peer.remove(old_peer) else {
            tracing::debug!(
                %old_peer,
                %new_peer,
                "update_peer_identity: old peer entry not found"
            );
            return false;
        };

        tracing::info!(%old_peer, %new_peer, %loc, "Updating peer identity for active connection");
        loc_for_peer.insert(new_peer.clone(), loc);
        drop(loc_for_peer);

        let mut cbl = self.connections_by_location.write();
        let entry = cbl.entry(loc).or_default();
        if let Some(conn) = entry
            .iter_mut()
            .find(|conn| conn.location.peer == *old_peer)
        {
            conn.location.peer = new_peer;
        } else {
            tracing::warn!(
                %old_peer,
                "update_peer_identity: connection entry missing; creating placeholder"
            );
            entry.push(Connection {
                location: PeerKeyLocation {
                    peer: new_peer,
                    location: Some(loc),
                },
            });
        }

        true
    }

    fn prune_connection(&self, peer: &PeerId, is_alive: bool) -> Option<Location> {
        let connection_type = if is_alive { "active" } else { "in transit" };
        tracing::debug!(%peer, "Pruning {} connection", connection_type);

        let mut locations_for_peer = self.location_for_peer.write();

        let Some(loc) = locations_for_peer.remove(peer) else {
            if is_alive {
                tracing::debug!("no location found for peer, skip pruning");
                return None;
            } else {
                let removed = self.pending_reservations.write().remove(peer).is_some();
                if !removed {
                    tracing::warn!(
                        %peer,
                        "prune_connection: no pending reservation to release for in-transit peer"
                    );
                }
            }
            return None;
        };

        let conns = &mut *self.connections_by_location.write();
        if let Some(conns) = conns.get_mut(&loc) {
            if let Some(pos) = conns.iter().position(|c| &c.location.peer == peer) {
                conns.swap_remove(pos);
            }
        }

        if !is_alive {
            self.pending_reservations.write().remove(peer);
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

    pub fn has_connection_or_pending(&self, peer: &PeerId) -> bool {
        self.location_for_peer.read().contains_key(peer)
            || self.pending_reservations.read().contains_key(peer)
    }

    pub(crate) fn get_connections_by_location(&self) -> BTreeMap<Location, Vec<Connection>> {
        self.connections_by_location.read().clone()
    }

    pub(super) fn get_known_locations(&self) -> BTreeMap<PeerId, Location> {
        self.location_for_peer.read().clone()
    }

    /// Route an op to the most optimal target.
    pub fn routing(
        &self,
        target: Location,
        requesting: Option<&PeerId>,
        skip_list: impl Contains<PeerId>,
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
        requesting: Option<&PeerId>,
        skip_list: impl Contains<PeerId>,
    ) -> Vec<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        let candidates: Vec<PeerKeyLocation> = connections
            .values()
            .filter_map(|conns| {
                let conn = conns.choose(&mut rand::rng())?;
                if self.is_transient(&conn.location.peer) {
                    return None;
                }
                if let Some(requester) = requesting {
                    if requester == &conn.location.peer {
                        return None;
                    }
                }
                (!skip_list.has_element(conn.location.peer.clone()))
                    .then_some(conn.location.clone())
            })
            .collect();

        tracing::debug!(
            total_locations = connections.len(),
            candidates = candidates.len(),
            target = %target,
            self_peer = self
                .get_peer_key()
                .as_ref()
                .map(|id| id.to_string())
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
    pub(super) fn connected_peers(&self) -> impl Iterator<Item = PeerId> {
        let read = self.location_for_peer.read();
        read.keys().cloned().collect::<Vec<_>>().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::PeerId;
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
        let own_peer_id = PeerId::new(addr, keypair.public().clone());

        // Create connection manager with this peer id
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                keypair.public().clone(),
                Some(own_peer_id.clone()),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
        );

        // Verify the connection manager has our peer id set
        assert_eq!(cm.get_peer_key(), Some(own_peer_id.clone()));

        // Attempt to accept a connection from ourselves - should be rejected
        let location = Location::new(0.5);
        let accepted = cm.should_accept(location, &own_peer_id);
        assert!(!accepted, "should_accept must reject self-connection");
    }

    #[test]
    fn accepts_connection_from_different_peer() {
        // Create our node's peer id
        let own_keypair = TransportKeypair::new();
        let own_addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let own_peer_id = PeerId::new(own_addr, own_keypair.public().clone());

        // Create connection manager with our peer id
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                own_keypair.public().clone(),
                Some(own_peer_id.clone()),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
        );

        // Create a different peer
        let other_keypair = TransportKeypair::new();
        let other_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let other_peer_id = PeerId::new(other_addr, other_keypair.public().clone());

        // Attempt to accept a connection from a different peer - should succeed
        // (first connection with no other constraints)
        let location = Location::new(0.6);
        let accepted = cm.should_accept(location, &other_peer_id);
        assert!(
            accepted,
            "should_accept must accept connection from different peer"
        );
    }

    #[test]
    fn rejects_self_connection_by_pubkey_when_peer_key_not_set() {
        // Create a keypair for our node
        let keypair = TransportKeypair::new();

        // Create connection manager WITHOUT peer_id set (simulating early initialization)
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                keypair.public().clone(),
                None, // peer_id is None - simulating early initialization
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
        );

        // Verify the connection manager does NOT have peer_key set
        assert_eq!(cm.get_peer_key(), None);

        // Create a PeerId with our pub_key but a different address
        // This simulates a scenario where we receive a connection request
        // that somehow references our own pub_key
        let self_like_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let self_like_peer_id = PeerId::new(self_like_addr, keypair.public().clone());

        // Attempt to accept - should be rejected because pub_key matches
        let location = Location::new(0.5);
        let accepted = cm.should_accept(location, &self_like_peer_id);
        assert!(
            !accepted,
            "should_accept must reject self-connection by pub_key even when peer_key is not set"
        );
    }
}
