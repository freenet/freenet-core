use parking_lot::Mutex;

use crate::topology::{Limits, TopologyManager};

use super::*;

#[derive(Clone)]
pub(crate) struct ConnectionManager {
    open_connections: Arc<AtomicUsize>,
    reserved_connections: Arc<AtomicUsize>,
    pub(super) location_for_peer: Arc<RwLock<BTreeMap<PeerId, Location>>>,
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
            pub_key,
            None,
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

        Self::init(
            max_upstream_bandwidth,
            max_downstream_bandwidth,
            min_connections,
            max_connections,
            rnd_if_htl_above,
            config.key_pair.public().clone(),
            config.peer_id.clone(),
        )
    }

    fn init(
        max_upstream_bandwidth: Rate,
        max_downstream_bandwidth: Rate,
        min_connections: usize,
        max_connections: usize,
        rnd_if_htl_above: usize,
        pub_key: TransportPublicKey,
        peerid: Option<PeerId>,
    ) -> Self {
        let own_location = if let Some(peer_key) = &peerid {
            // if the peer id is set, then the location must be set, since it is a gateway
            let location = Location::from_address(&peer_key.addr);
            AtomicU64::new(u64::from_le_bytes(location.0.to_le_bytes()))
        } else {
            // for location here consider -1 == None
            AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()))
        };

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
            peer_key: Arc::new(Mutex::new(peerid)),
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
            self.location_for_peer
                .write()
                .insert(peer_id.clone(), location);
            return true;
        }

        if self.location_for_peer.read().get(peer_id).is_some() {
            // avoid connecting more than once to the same peer
            self.reserved_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            tracing::debug!(%peer_id, "Peer already connected");
            return false;
        }

        let my_location = self
            .own_location()
            .location
            .unwrap_or_else(Location::random);
        let accepted = if location == my_location
            || self.connections_by_location.read().contains_key(&location)
        {
            false
        } else if total_conn < self.min_connections {
            true
        } else if total_conn >= self.max_connections {
            false
        } else {
            self.topology_manager
                .write()
                .evaluate_new_connection(location, Instant::now())
                .unwrap_or(true)
        };
        if !accepted {
            self.reserved_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        } else {
            tracing::debug!(%peer_id, "Accepted connection, reserving spot");
            self.location_for_peer
                .write()
                .insert(peer_id.clone(), location);
        }
        accepted
    }

    /// Update this node location.
    pub fn update_location(&self, loc: Option<Location>) {
        if let Some(loc) = loc {
            self.own_location.store(
                u64::from_le_bytes(loc.0.to_le_bytes()),
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
            Some(Location(location))
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
        tracing::info!(%peer, this = ?self.get_peer_key(), %was_reserved, "Adding connection to peer");
        debug_assert!(
            self.get_peer_key()
                .expect("should be set")
                != peer
        );
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
        self.open_connections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut cbl = self.connections_by_location.write();
        cbl.entry(loc).or_default().push(Connection {
            location: PeerKeyLocation {
                peer: peer.clone(),
                location: Some(loc),
            },
            open_at: Instant::now(),
        });
        self.location_for_peer
            .write()
            .insert(peer.clone(), loc);
        std::mem::drop(cbl);
    }

    fn prune_connection(&self, peer: &PeerId, is_alive: bool) -> Option<Location> {
        let connection_type = if is_alive { "active" } else { "in transit" };
        tracing::debug!(%peer, "Pruning {} connection", connection_type);

        let Some(loc) = self.location_for_peer.write().remove(peer) else {
            if is_alive {
                self.open_connections
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            } else {
                self.reserved_connections
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
            return None;
        };

        let conns = &mut *self.connections_by_location.write();
        if let Some(conns) = conns.get_mut(&loc) {
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

        Some(loc)
    }
    
    pub(super) fn get_open_connections(&self) -> usize {
        self.open_connections.load(std::sync::atomic::Ordering::SeqCst)
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
            let (peer, loc) = peers.iter().nth(selected).expect("infallible");
            if !filter_fn(peer) {
                attempts += 1;
                continue;
            } else {
                return Some(PeerKeyLocation {
                    peer: peer.clone(),
                    location: Some(*loc),
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
            (!skip_list.has_element(&conn.location.peer)).then_some(&conn.location)
        });
        router.select_peer(peers, target).cloned()
    }

    pub fn num_connections(&self) -> usize {
        self.connections_by_location.read().len()
    }

    pub(super) fn connected_peers(&self) -> impl Iterator<Item = PeerId> {
        let read = self.location_for_peer.read();
        read.keys().cloned().collect::<Vec<_>>().into_iter()
    }
}
