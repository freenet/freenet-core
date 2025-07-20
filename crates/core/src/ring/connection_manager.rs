use parking_lot::Mutex;

use crate::topology::{Limits, TopologyManager};

use super::*;

/// State of a peer connection within the ring topology
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Transient gateway connection used for bootstrapping
    Transient,
    /// Full peer connection that's part of the ring topology
    Ring,
}

/// Information about a connected peer
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub location: Location,
    pub state: ConnectionState,
}

#[derive(Clone)]
pub(crate) struct ConnectionManager {
    open_connections: Arc<AtomicUsize>,
    reserved_connections: Arc<AtomicUsize>,
    pub(super) location_for_peer: Arc<RwLock<BTreeMap<PeerId, ConnectionInfo>>>,
    pub(super) topology_manager: Arc<RwLock<TopologyManager>>,
    connections_by_location: Arc<RwLock<BTreeMap<Location, Vec<Connection>>>>,
    /// Interim connections ongoing handshake or successfully open connections
    /// Is important to keep track of this so no more connections are accepted prematurely.
    own_location: Arc<AtomicU64>,
    peer_key: Arc<Mutex<Option<PeerId>>>,
    pub min_connections: usize,
    pub max_connections: usize,
    pub rnd_if_htl_above: usize,
    pub pub_key: Arc<TransportPublicKey>,
}

#[cfg(test)]
impl ConnectionManager {
    pub fn default_with_key(pub_key: TransportPublicKey) -> Self {
        let min_connections = Ring::DEFAULT_MIN_CONNECTIONS;
        let max_connections = Ring::DEFAULT_MAX_CONNECTIONS;
        let max_upstream_bandwidth = Ring::DEFAULT_MAX_UPSTREAM_BANDWIDTH;
        let max_downstream_bandwidth = Ring::DEFAULT_MAX_DOWNSTREAM_BANDWIDTH;
        let rnd_if_htl_above = Ring::DEFAULT_RAND_WALK_ABOVE_HTL;

        Self::init(
            max_upstream_bandwidth,
            max_downstream_bandwidth,
            min_connections,
            max_connections,
            rnd_if_htl_above,
            (
                pub_key,
                None,
                AtomicU64::new(u64::from_le_bytes(
                    Location::random().as_f64().to_le_bytes(),
                )),
            ),
        )
    }
}

impl ConnectionManager {
    pub fn new(config: &NodeConfig) -> Self {
        let min_connections = if let Some(v) = config.min_number_conn {
            v
        } else {
            Ring::DEFAULT_MIN_CONNECTIONS
        };

        let max_connections = if let Some(v) = config.max_number_conn {
            v
        } else {
            Ring::DEFAULT_MAX_CONNECTIONS
        };

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
        )
    }

    fn init(
        max_upstream_bandwidth: Rate,
        max_downstream_bandwidth: Rate,
        min_connections: usize,
        max_connections: usize,
        rnd_if_htl_above: usize,
        (pub_key, peer_id, own_location): (TransportPublicKey, Option<PeerId>, AtomicU64),
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
            open_connections: Arc::new(AtomicUsize::new(0)),
            reserved_connections: Arc::new(AtomicUsize::new(0)),
            topology_manager,
            own_location: own_location.into(),
            peer_key: Arc::new(Mutex::new(peer_id)),
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
        self.should_accept_with_upgrade(location, peer_id, false)
    }

    pub fn should_accept_with_upgrade(
        &self,
        location: Location,
        peer_id: &PeerId,
        allow_upgrade: bool,
    ) -> bool {
        tracing::debug!("Checking if should accept connection");
        let open = self
            .open_connections
            .load(std::sync::atomic::Ordering::SeqCst);
        let total_conn = self
            .reserved_connections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + open;

        if open == 0 {
            // if this is the first connection, then accept it
            return true;
        }

        if let Some(connection_info) = self.location_for_peer.read().get(peer_id) {
            if allow_upgrade && connection_info.state == ConnectionState::Transient {
                // Allow upgrading transient gateway connection to ring connection
                tracing::debug!(%peer_id, "Allowing upgrade from transient to ring connection");
                // Don't decrement reserved_connections here - the upgrade will handle it
                return true;
            } else {
                // avoid connecting more than once to the same peer
                self.reserved_connections
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                tracing::debug!(%peer_id, state=?connection_info.state, "Peer already connected, rejecting");
                return false;
            }
        }

        let accepted = if total_conn < self.min_connections {
            tracing::debug!(%peer_id, "Accepted connection, below min connections");
            true
        } else if total_conn >= self.max_connections {
            tracing::debug!(%peer_id, "Rejected connection, max connections reached");
            false
        } else {
            let accepted = self
                .topology_manager
                .write()
                .evaluate_new_connection(location, Instant::now())
                .unwrap_or(true);

            if accepted {
                tracing::debug!(%peer_id, "Accepted connection, topology manager");
            } else {
                tracing::debug!(%peer_id, "Rejected connection, topology manager");
            }
            accepted
        };
        if !accepted {
            self.reserved_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        } else {
            tracing::debug!(%peer_id, "Accepted connection, reserving spot");
        }
        accepted
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
        self.add_connection_with_state(loc, peer, was_reserved, ConnectionState::Ring);
    }

    pub fn add_connection_with_state(
        &self,
        loc: Location,
        peer: PeerId,
        was_reserved: bool,
        state: ConnectionState,
    ) {
        tracing::debug!(%peer, state=?state, "Adding connection");
        debug_assert!(self.get_peer_key().expect("should be set") != peer);
        if was_reserved {
            let old = self
                .reserved_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            #[cfg(debug_assertions)]
            {
                tracing::debug!(old, "Decremented reserved connections");
                if old == 0 {
                    panic!("Underflow of reserved connections");
                }
            }
            let _ = old;
        }
        let mut lop = self.location_for_peer.write();
        lop.insert(
            peer.clone(),
            ConnectionInfo {
                location: loc,
                state,
            },
        );
        {
            let mut cbl = self.connections_by_location.write();
            cbl.entry(loc).or_default().push(Connection {
                location: PeerKeyLocation {
                    peer: peer.clone(),
                    location: Some(loc),
                },
                open_at: Instant::now(),
            });
        }
        self.open_connections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::mem::drop(lop);
    }

    fn prune_connection(&self, peer: &PeerId, is_alive: bool) -> Option<Location> {
        let connection_type = if is_alive { "active" } else { "in transit" };
        tracing::debug!(%peer, "Pruning {} connection", connection_type);

        let mut locations_for_peer = self.location_for_peer.write();

        let Some(connection_info) = locations_for_peer.remove(peer) else {
            if is_alive {
                tracing::debug!("no location found for peer, skip pruning");
                return None;
            } else {
                self.reserved_connections
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
            return None;
        };

        let conns = &mut *self.connections_by_location.write();
        if let Some(conns) = conns.get_mut(&connection_info.location) {
            if let Some(pos) = conns.iter().position(|c| &c.location.peer == peer) {
                conns.swap_remove(pos);
            }
        }

        if is_alive {
            self.open_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        } else {
            self.reserved_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }

        Some(connection_info.location)
    }

    /// Check if a peer is connected (in any state)
    pub fn is_connected(&self, peer_id: &PeerId) -> bool {
        self.location_for_peer.read().contains_key(peer_id)
    }

    /// Upgrade a transient gateway connection to a full ring connection
    pub fn upgrade_gateway_connection(&self, peer_id: &PeerId) -> bool {
        let mut connections = self.location_for_peer.write();
        if let Some(connection_info) = connections.get_mut(peer_id) {
            if connection_info.state == ConnectionState::Transient {
                tracing::debug!(%peer_id, "Upgrading gateway connection to ring connection");
                connection_info.state = ConnectionState::Ring;
                return true;
            } else {
                tracing::warn!(%peer_id, state=?connection_info.state, "Cannot upgrade connection - not in transient state");
                return false;
            }
        }
        tracing::warn!(%peer_id, "Cannot upgrade connection - peer not found");
        false
    }

    pub(super) fn get_open_connections(&self) -> usize {
        self.open_connections
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn get_connections_by_location(&self) -> BTreeMap<Location, Vec<Connection>> {
        self.connections_by_location.read().clone()
    }

    /// Get a random peer from the known ring connections.
    pub fn random_peer<F>(&self, filter_fn: F) -> Option<PeerKeyLocation>
    where
        F: Fn(&PeerId) -> bool,
    {
        let peers = &*self.location_for_peer.read();
        let amount = peers.len();
        if amount == 0 {
            return None;
        }
        let mut rng = rand::thread_rng();
        let mut attempts = 0;
        loop {
            if attempts >= amount * 2 {
                return None;
            }
            let selected = rng.gen_range(0..amount);
            let (peer, connection_info) = peers.iter().nth(selected).expect("infallible");
            if !filter_fn(peer) {
                attempts += 1;
                continue;
            } else {
                return Some(PeerKeyLocation {
                    peer: peer.clone(),
                    location: Some(connection_info.location),
                });
            }
        }
    }

    /// Route an op to the most optimal target.
    pub fn routing(
        &self,
        target: Location,
        requesting: Option<&PeerId>,
        skip_list: impl Contains<PeerId>,
        router: &Router,
    ) -> Option<PeerKeyLocation> {
        use rand::seq::SliceRandom;
        let connections = self.connections_by_location.read();
        let peers = connections.values().filter_map(|conns| {
            let conn = conns.choose(&mut rand::thread_rng())?;
            if let Some(requester) = requesting {
                if requester == &conn.location.peer {
                    return None;
                }
            }
            (!skip_list.has_element(conn.location.peer.clone())).then_some(&conn.location)
        });
        router.select_peer(peers, target).cloned()
    }

    pub fn num_connections(&self) -> usize {
        self.connections_by_location.read().len()
    }

    pub(super) fn connected_peers(&self) -> impl Iterator<Item = PeerId> {
        let read = self.location_for_peer.read();
        read.iter()
            .filter(|(_, connection_info)| connection_info.state == ConnectionState::Ring)
            .map(|(peer_id, _)| peer_id.clone())
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dev_tool::{Location, PeerId, TransportKeypair};

    fn create_test_connection_manager() -> ConnectionManager {
        let keypair = TransportKeypair::new();
        let cm = ConnectionManager::default_with_key(keypair.public().clone());
        cm.try_set_peer_key("127.0.0.1:8080".parse().unwrap());
        cm
    }

    fn create_test_peer() -> PeerId {
        let keypair = TransportKeypair::new();
        PeerId::new("127.0.0.1:8000".parse().unwrap(), keypair.public().clone())
    }

    #[test]
    fn test_add_transient_connection() {
        let cm = create_test_connection_manager();
        let peer = create_test_peer();
        let location = Location::from_address(&peer.addr);

        // Add as transient connection
        cm.add_connection_with_state(location, peer.clone(), false, ConnectionState::Transient);

        // Should be connected
        assert!(cm.is_connected(&peer));

        // Should not appear in connected_peers (only Ring connections)
        assert_eq!(cm.connected_peers().count(), 0);
    }

    #[test]
    fn test_upgrade_gateway_connection() {
        let cm = create_test_connection_manager();
        let peer = create_test_peer();
        let location = Location::from_address(&peer.addr);

        // Add as transient connection
        cm.add_connection_with_state(location, peer.clone(), false, ConnectionState::Transient);

        // Upgrade to ring connection
        assert!(cm.upgrade_gateway_connection(&peer));

        // Should still be connected
        assert!(cm.is_connected(&peer));

        // Should now appear in connected_peers
        assert_eq!(cm.connected_peers().count(), 1);
    }

    #[test]
    fn test_should_accept_with_upgrade() {
        let cm = create_test_connection_manager();
        let peer = create_test_peer();
        let location = Location::from_address(&peer.addr);

        // First connection should be accepted
        assert!(cm.should_accept(location, &peer));

        // Add as transient connection
        cm.add_connection_with_state(location, peer.clone(), true, ConnectionState::Transient);

        // Normal should_accept should reject
        assert!(!cm.should_accept(location, &peer));

        // should_accept_with_upgrade should allow
        assert!(cm.should_accept_with_upgrade(location, &peer, true));

        // Upgrade the connection
        assert!(cm.upgrade_gateway_connection(&peer));

        // After upgrade, should_accept_with_upgrade should still reject (already Ring)
        assert!(!cm.should_accept_with_upgrade(location, &peer, true));
    }

    #[test]
    fn test_upgrade_nonexistent_connection() {
        let cm = create_test_connection_manager();
        let peer = create_test_peer();

        // Should not be able to upgrade non-existent connection
        assert!(!cm.upgrade_gateway_connection(&peer));
    }

    #[test]
    fn test_upgrade_ring_connection() {
        let cm = create_test_connection_manager();
        let peer = create_test_peer();
        let location = Location::from_address(&peer.addr);

        // Add as ring connection directly
        cm.add_connection(location, peer.clone(), false);

        // Should not be able to upgrade ring connection
        assert!(!cm.upgrade_gateway_connection(&peer));
    }
}
