use parking_lot::Mutex;
use rand::prelude::IndexedRandom;
use std::collections::{btree_map::Entry, BTreeMap, HashMap};

use crate::topology::{Limits, TopologyManager};

use super::*;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct TransientEntry {
    #[allow(dead_code)]
    pub opened_at: Instant,
    pub location: Option<Location>,
}

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
    is_gateway: bool,
    transient_connections: Arc<RwLock<HashMap<PeerId, TransientEntry>>>,
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
            open_connections: Arc::new(AtomicUsize::new(0)),
            reserved_connections: Arc::new(AtomicUsize::new(0)),
            topology_manager,
            own_location: own_location.into(),
            peer_key: Arc::new(Mutex::new(peer_id)),
            is_gateway,
            transient_connections: Arc::new(RwLock::new(HashMap::new())),
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
        tracing::info!("Checking if should accept connection");
        let open = self
            .open_connections
            .load(std::sync::atomic::Ordering::SeqCst);
        let reserved_before = self
            .reserved_connections
            .load(std::sync::atomic::Ordering::SeqCst);

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

        let reserved_before = loop {
            let current = self
                .reserved_connections
                .load(std::sync::atomic::Ordering::SeqCst);
            if current == usize::MAX {
                tracing::error!(
                    %peer_id,
                    "reserved connection counter overflowed; rejecting new connection"
                );
                return false;
            }
            match self.reserved_connections.compare_exchange(
                current,
                current + 1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            ) {
                Ok(_) => break current,
                Err(actual) => {
                    tracing::debug!(
                        %peer_id,
                        expected = current,
                        actual,
                        "reserved connection counter changed concurrently; retrying"
                    );
                }
            }
        };

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
                self.reserved_connections
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return false;
            }
        };

        if open == 0 {
            tracing::debug!(%peer_id, "should_accept: first connection -> accepting");
            return true;
        }

        const GATEWAY_DIRECT_ACCEPT_LIMIT: usize = 2;
        if self.is_gateway {
            let direct_total = open + reserved_before;
            if direct_total >= GATEWAY_DIRECT_ACCEPT_LIMIT {
                tracing::info!(
                    %peer_id,
                    open,
                    reserved_before,
                    limit = GATEWAY_DIRECT_ACCEPT_LIMIT,
                    "Gateway reached direct-accept limit; forwarding join request instead"
                );
                self.reserved_connections
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                tracing::info!(%peer_id, "should_accept: gateway direct-accept limit hit, forwarding instead");
                return false;
            }
        }

        if self.location_for_peer.read().get(peer_id).is_some() {
            // We've already accepted this peer (pending or active); treat as a no-op acceptance.
            tracing::debug!(%peer_id, "Peer already pending/connected; acknowledging acceptance");
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
            reserved_connections = self
                .reserved_connections
                .load(std::sync::atomic::Ordering::SeqCst),
            "should_accept: final decision"
        );
        if !accepted {
            self.reserved_connections
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
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

    pub fn register_transient(&self, peer: PeerId, location: Option<Location>) {
        self.transient_connections.write().insert(
            peer,
            TransientEntry {
                opened_at: Instant::now(),
                location,
            },
        );
    }

    pub fn drop_transient(&self, peer: &PeerId) -> Option<TransientEntry> {
        self.transient_connections.write().remove(peer)
    }

    #[allow(dead_code)]
    pub fn is_transient(&self, peer: &PeerId) -> bool {
        self.transient_connections.read().contains_key(peer)
    }

    pub fn transient_count(&self) -> usize {
        self.transient_connections.read().len()
    }

    pub fn transient_budget(&self) -> usize {
        self.transient_budget
    }

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
        lop.insert(peer.clone(), loc);
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

    pub fn update_peer_identity(&self, old_peer: &PeerId, new_peer: PeerId) -> bool {
        if old_peer == &new_peer {
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
                open_at: Instant::now(),
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
        self.open_connections
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn get_connections_by_location(&self) -> BTreeMap<Location, Vec<Connection>> {
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
        let connections = self.connections_by_location.read();
        let peers = connections.values().filter_map(|conns| {
            let conn = conns.choose(&mut rand::rng())?;
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
