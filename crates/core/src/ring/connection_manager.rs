use parking_lot::Mutex;

use crate::topology::{Limits, TopologyManager};

use super::*;

#[derive(Clone)]
pub(crate) struct ConnectionManager {
    pub(super) open_connections: Arc<AtomicUsize>,
    pub(super) location_for_peer: Arc<RwLock<BTreeMap<PeerId, Location>>>,
    pub(super) topology_manager: Arc<RwLock<TopologyManager>>,
    pub(super) connections_by_location: Arc<RwLock<BTreeMap<Location, Vec<Connection>>>>,
    /// Interim connections ongoing handshake or successfully open connections
    /// Is important to keep track of this so no more connections are accepted prematurely.
    pub(super) own_location: Arc<AtomicU64>,
    pub(super) peer_key: Arc<Mutex<Option<PeerId>>>,
    pub min_connections: usize,
    pub max_connections: usize,
    pub max_hops_to_live: usize,
    pub rnd_if_htl_above: usize,
}

#[cfg(test)]
impl Default for ConnectionManager {
    fn default() -> Self {
        let min_connections = Ring::DEFAULT_MIN_CONNECTIONS;
        let max_connections = Ring::DEFAULT_MAX_CONNECTIONS;
        let max_upstream_bandwidth = Ring::DEFAULT_MAX_UPSTREAM_BANDWIDTH;
        let max_downstream_bandwidth = Ring::DEFAULT_MAX_DOWNSTREAM_BANDWIDTH;
        let max_hops_to_live = Ring::DEFAULT_MAX_HOPS_TO_LIVE;
        let rnd_if_htl_above = Ring::DEFAULT_RAND_WALK_ABOVE_HTL;

        Self::init(
            max_upstream_bandwidth,
            max_downstream_bandwidth,
            min_connections,
            max_connections,
            max_hops_to_live,
            rnd_if_htl_above,
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

        let max_hops_to_live = if let Some(v) = config.max_hops_to_live {
            v
        } else {
            Ring::DEFAULT_MAX_HOPS_TO_LIVE
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
            max_hops_to_live,
            rnd_if_htl_above,
        )
    }

    fn init(
        max_upstream_bandwidth: Rate,
        max_downstream_bandwidth: Rate,
        min_connections: usize,
        max_connections: usize,
        max_hops_to_live: usize,
        rnd_if_htl_above: usize,
    ) -> Self {
        // for location here consider -1 == None
        let own_location = AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()));

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
            topology_manager,
            own_location: own_location.into(),
            peer_key: Arc::new(Mutex::new(None)),
            min_connections,
            max_connections,
            max_hops_to_live,
            rnd_if_htl_above,
        }
    }

    /// Whether a node should accept a new node connection or not based
    /// on the relative location and other conditions.
    ///
    /// # Panic
    /// Will panic if the node checking for this condition has no location assigned.
    // FIXME: peer here should not be optional ever
    pub fn should_accept(&self, location: Location, peer: Option<&PeerId>) -> bool {
        let open_conn = self
            .open_connections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        if let Some(peer_id) = peer {
            if self.location_for_peer.read().get(peer_id).is_some() {
                // avoid connecting more than once to the same peer
                self.open_connections
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                tracing::debug!(%peer_id, "Peer already connected");
                return false;
            }
        }

        let my_location = self
            .own_location()
            .location
            .unwrap_or_else(Location::random);
        let accepted = if location == my_location
            || self.connections_by_location.read().contains_key(&location)
        {
            false
        } else if open_conn < self.min_connections {
            true
        } else if open_conn >= self.max_connections {
            false
        } else {
            self.topology_manager
                .write()
                .evaluate_new_connection(location, Instant::now())
                .unwrap_or(true)
        };
        if !accepted {
            self.open_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        } else if let Some(peer_id) = peer {
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
    pub fn set_peer_key(&self, peer_key: PeerId) -> Option<PeerId> {
        let mut this_peer = self.peer_key.lock();
        if this_peer.is_none() {
            *this_peer = Some(peer_key);
            None
        } else {
            this_peer.clone()
        }
    }

    pub fn prune_connection(&self, peer: &PeerId) -> Option<Location> {
        let Some(loc) = self.location_for_peer.write().remove(&peer) else {
            self.open_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return None;
        };
        let conns = &mut *self.connections_by_location.write();
        if let Some(conns) = conns.get_mut(&loc) {
            if let Some(pos) = conns.iter().position(|c| &c.location.peer == peer) {
                conns.swap_remove(pos);
            }
        }
        self.open_connections
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        Some(loc)
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
}
