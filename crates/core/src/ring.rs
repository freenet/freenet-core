//! Ring protocol logic and supporting types.
//!
//! Mainly maintains a healthy and optimal pool of connections to other peers in the network
//! and routes requests to the optimal peers.

use std::collections::{BTreeSet, HashSet};
use std::net::SocketAddr;
use std::{
    cmp::Reverse,
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tracing::Instrument;

use dashmap::mapref::one::Ref as DmRef;
use either::Either;
use freenet_stdlib::prelude::ContractKey;
use itertools::Itertools;
use parking_lot::RwLock;
use rand::Rng;

use crate::message::TransactionType;
use crate::topology::rate::Rate;
use crate::topology::TopologyAdjustment;
use crate::tracing::{NetEventLog, NetEventRegister};
use crate::transport::TransportPublicKey;
use crate::util::Contains;
use crate::{
    config::GlobalExecutor,
    message::Transaction,
    node::{self, EventLoopNotificationsSender, NodeConfig, PeerId},
    operations::connect,
    router::Router,
};

mod connection_manager;
pub(crate) use connection_manager::ConnectionManager;
mod connection;
mod live_tx;
mod location;
mod peer_key_location;
mod score;
mod seeding;

use self::score::Score;

pub use self::live_tx::LiveTransactionTracker;
pub use connection::Connection;
pub use location::{Distance, Location};
pub use peer_key_location::PeerKeyLocation;

/// Thread safe and friendly data structure to keep track of the local knowledge
/// of the state of the ring.
///
// Note: For now internally we wrap some of the types internally with locks and/or use
// multithreaded maps. In the future if performance requires it some of this can be moved
// towards a more lock-free multithreading model if necessary.
pub(crate) struct Ring {
    pub max_hops_to_live: usize,
    pub connection_manager: ConnectionManager,
    pub router: Arc<RwLock<Router>>,
    pub live_tx_tracker: LiveTransactionTracker,
    seeding_manager: seeding::SeedingManager,
    event_register: Box<dyn NetEventRegister>,
    /// Whether this peer is a gateway or not. This will affect behavior of the node when acquiring
    /// and dropping connections.
    #[allow(unused)]
    is_gateway: bool,
}

// /// A data type that represents the fact that a peer has been blacklisted
// /// for some action. Has to be coupled with that action
// #[derive(Debug)]
// struct Blacklisted {
//     since: Instant,
//     peer: PeerKey,
// }

impl Ring {
    const DEFAULT_MIN_CONNECTIONS: usize = 25;

    const DEFAULT_MAX_CONNECTIONS: usize = 200;

    const DEFAULT_MAX_UPSTREAM_BANDWIDTH: Rate = Rate::new_per_second(1_000_000.0);

    const DEFAULT_MAX_DOWNSTREAM_BANDWIDTH: Rate = Rate::new_per_second(1_000_000.0);

    /// Above this number of remaining hops, randomize which node a message which be forwarded to.
    const DEFAULT_RAND_WALK_ABOVE_HTL: usize = 7;

    /// Max hops to be performed for certain operations (e.g. propagating connection of a peer in the network).
    pub const DEFAULT_MAX_HOPS_TO_LIVE: usize = 10;

    pub fn new<ER: NetEventRegister + Clone>(
        config: &NodeConfig,
        event_loop_notifier: EventLoopNotificationsSender,
        event_register: ER,
        is_gateway: bool,
        connection_manager: ConnectionManager,
    ) -> anyhow::Result<Arc<Self>> {
        let (live_tx_tracker, missing_candidate_rx) = LiveTransactionTracker::new();

        let max_hops_to_live = if let Some(v) = config.max_hops_to_live {
            v
        } else {
            Self::DEFAULT_MAX_HOPS_TO_LIVE
        };

        let router = Arc::new(RwLock::new(Router::new(&[])));
        GlobalExecutor::spawn(Self::refresh_router(router.clone(), event_register.clone()));

        // Just initialize with a fake location, this will be later updated when the peer has an actual location assigned.
        let ring = Ring {
            max_hops_to_live,
            router,
            connection_manager,
            seeding_manager: seeding::SeedingManager::new(),
            live_tx_tracker: live_tx_tracker.clone(),
            event_register: Box::new(event_register),
            is_gateway,
        };

        if let Some(loc) = config.location {
            if config.peer_id.is_none() {
                return Err(anyhow::anyhow!("PeerId is required for gateways"));
            }
            ring.connection_manager.update_location(Some(loc));
        }

        let ring = Arc::new(ring);
        let current_span = tracing::Span::current();
        let span = if current_span.is_none() {
            tracing::info_span!("connection_maintenance")
        } else {
            tracing::info_span!(parent: current_span, "connection_maintenance")
        };

        GlobalExecutor::spawn(
            ring.clone()
                .connection_maintenance(event_loop_notifier, live_tx_tracker, missing_candidate_rx)
                .instrument(span),
        );

        Ok(ring)
    }

    pub fn is_gateway(&self) -> bool {
        self.is_gateway
    }

    pub fn open_connections(&self) -> usize {
        self.connection_manager.get_open_connections()
    }

    async fn refresh_router<ER: NetEventRegister>(router: Arc<RwLock<Router>>, register: ER) {
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 5));
        interval.tick().await;
        loop {
            interval.tick().await;
            let history = register
                .get_router_events(10_000)
                .await
                .map_err(|error| {
                    tracing::error!(%error, "shutting down refresh router task");
                    error
                })
                .expect("todo: propagate this to main thread");
            if !history.is_empty() {
                let router_ref = &mut *router.write();
                *router_ref = Router::new(&history);
            }
        }
    }

    /// Return if a contract is within appropiate seeding distance.
    pub fn should_seed(&self, key: &ContractKey) -> bool {
        let own_loc = self
            .connection_manager
            .own_location()
            .location
            .expect("should be set");
        self.seeding_manager.should_seed(key, own_loc)
    }

    /// Add a new subscription for this peer.
    pub fn seed_contract(&self, key: ContractKey) -> (Option<ContractKey>, Vec<PeerKeyLocation>) {
        let own_loc = self
            .connection_manager
            .own_location()
            .location
            .expect("should be set");
        self.seeding_manager.seed_contract(key, own_loc)
    }

    /// Whether this node already is seeding to this contract or not.
    #[inline]
    pub fn is_seeding_contract(&self, key: &ContractKey) -> bool {
        self.seeding_manager.is_seeding_contract(key)
    }

    pub fn record_request(
        &self,
        recipient: PeerKeyLocation,
        target: Location,
        request_type: TransactionType,
    ) {
        self.connection_manager
            .topology_manager
            .write()
            .record_request(recipient, target, request_type);
    }

    pub async fn add_connection(&self, loc: Location, peer: PeerId, was_reserved: bool) {
        tracing::info!(%peer, this = ?self.connection_manager.get_peer_key(), %was_reserved, "Adding connection to peer");
        self.connection_manager
            .add_connection(loc, peer.clone(), was_reserved);
        self.event_register
            .register_events(Either::Left(NetEventLog::connected(self, peer, loc)))
            .await;
        self.refresh_density_request_cache()
    }

    fn refresh_density_request_cache(&self) {
        let cbl = self.connection_manager.get_connections_by_location();
        let topology_manager = &mut self.connection_manager.topology_manager.write();
        let _ = topology_manager.refresh_cache(&cbl);
    }

    /// Returns a filtered iterator for peers that are not connected to this node already.
    pub fn is_not_connected<'a>(
        &self,
        peers: impl Iterator<Item = &'a PeerKeyLocation>,
    ) -> impl Iterator<Item = &'a PeerKeyLocation> + Send {
        let locs = &*self.connection_manager.location_for_peer.read();
        let mut filtered = Vec::new();
        for peer in peers {
            if !locs.contains_key(&peer.peer) {
                filtered.push(peer);
            }
        }
        filtered.into_iter()
    }

    /// Return the most optimal peer caching a given contract.
    #[inline]
    pub fn closest_potentially_caching(
        &self,
        contract_key: &ContractKey,
        skip_list: impl Contains<PeerId>,
    ) -> Option<PeerKeyLocation> {
        let router = self.router.read();
        self.connection_manager
            .routing(Location::from(contract_key), None, skip_list, &router)
    }

    pub fn routing_finished(&self, event: crate::router::RouteEvent) {
        self.connection_manager
            .topology_manager
            .write()
            .report_outbound_request(event.peer.clone(), event.contract_location);
        self.router.write().add_event(event);
    }

    /// Will return an error in case the max number of subscribers has been added.
    pub fn add_subscriber(
        &self,
        contract: &ContractKey,
        subscriber: PeerKeyLocation,
    ) -> Result<(), ()> {
        self.seeding_manager.add_subscriber(contract, subscriber)
    }

    pub fn subscribers_of(
        &self,
        contract: &ContractKey,
    ) -> Option<DmRef<ContractKey, Vec<PeerKeyLocation>>> {
        self.seeding_manager.subscribers_of(contract)
    }

    pub async fn prune_connection(&self, peer: PeerId) {
        tracing::debug!(%peer, "Removing connection");
        self.live_tx_tracker.prune_transactions_from_peer(&peer);
        // This case would be when a connection is being open, so peer location hasn't been recorded yet and we can ignore everything below
        let Some(loc) = self.connection_manager.prune_alive_connection(&peer) else {
            return;
        };
        {
            self.seeding_manager.prune_subscriber(loc);
        }
        self.event_register
            .register_events(Either::Left(NetEventLog::disconnected(self, &peer)))
            .await;
    }

    pub fn closest_to_location(
        &self,
        location: Location,
        skip_list: HashSet<PeerId>,
    ) -> Option<PeerKeyLocation> {
        use rand::seq::SliceRandom;
        self.connection_manager
            .get_connections_by_location()
            .iter()
            .sorted_by(|(loc_a, _), (loc_b, _)| {
                loc_a.distance(location).cmp(&loc_b.distance(location))
            })
            .find_map(|(_, conns)| {
                for _ in 0..conns.len() {
                    let conn = conns.choose(&mut rand::thread_rng()).unwrap();
                    let selected =
                        (!skip_list.contains(&conn.location.peer)).then_some(conn.location.clone());
                    if selected.is_some() {
                        return selected;
                    }
                }
                None
            })
    }

    async fn connection_maintenance(
        self: Arc<Self>,
        notifier: EventLoopNotificationsSender,
        live_tx_tracker: LiveTransactionTracker,
        mut missing_candidates: mpsc::Receiver<PeerId>,
    ) -> anyhow::Result<()> {
        tracing::debug!("Initializing connection maintenance task");
        #[cfg(not(test))]
        const CONNECTION_AGE_THRESOLD: Duration = Duration::from_secs(60 * 5);
        #[cfg(test)]
        const CONNECTION_AGE_THRESOLD: Duration = Duration::from_secs(5);
        const CHECK_TICK_DURATION: Duration = Duration::from_secs(60);
        const REGENERATE_DENSITY_MAP_INTERVAL: Duration = Duration::from_secs(60);

        let mut check_interval = tokio::time::interval(CHECK_TICK_DURATION);
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh_density_map = tokio::time::interval(REGENERATE_DENSITY_MAP_INTERVAL);
        refresh_density_map.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut missing = BTreeMap::new();

        #[cfg(not(test))]
        let retry_peers_missing_candidates_interval = Duration::from_secs(60 * 5) * 2;
        #[cfg(test)]
        let retry_peers_missing_candidates_interval = Duration::from_secs(5);

        // if the peer is just starting wait a bit before
        // we even attempt acquiring more connections
        tokio::time::sleep(Duration::from_secs(2)).await;

        let mut live_tx = None;
        let mut pending_conn_adds = BTreeSet::new();
        loop {
            if self.connection_manager.get_peer_key().is_none() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            loop {
                match missing_candidates.try_recv() {
                    Ok(missing_candidate) => {
                        missing.insert(Reverse(Instant::now()), missing_candidate);
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        tracing::debug!("Shutting down connection maintenance");
                        anyhow::bail!("finished");
                    }
                }
            }

            // eventually peers which failed to return candidates should be retried when enough time has passed
            let retry_missing_candidates_until =
                Instant::now() - retry_peers_missing_candidates_interval;

            // remove all missing candidates which have been retried
            missing.split_off(&Reverse(retry_missing_candidates_until));

            // avoid connecting to the same peer multiple times
            let mut skip_list: HashSet<_> = missing.values().collect();
            let this_peer = self.connection_manager.get_peer_key().unwrap();
            skip_list.insert(&this_peer);

            // if there are no open connections, we need to acquire more
            if let Some(tx) = &live_tx {
                if !live_tx_tracker.still_alive(tx) {
                    let _ = live_tx.take();
                }
            }

            if let Some(ideal_location) = pending_conn_adds.pop_first() {
                if live_tx.is_none() {
                    live_tx = self
                        .acquire_new(ideal_location, &skip_list, &notifier, &live_tx_tracker)
                        .await
                        .map_err(|error| {
                            tracing::debug!(?error, "Shutting down connection maintenance task");
                            error
                        })?;
                } else {
                    pending_conn_adds.insert(ideal_location);
                }
            }

            let neighbor_locations = {
                let peers = self.connection_manager.get_connections_by_location();
                peers
                    .iter()
                    .map(|(loc, conns)| {
                        let conns: Vec<_> = conns
                            .iter()
                            .filter(|conn| {
                                conn.open_at.elapsed() > CONNECTION_AGE_THRESOLD
                                    && !live_tx_tracker.has_live_connection(&conn.location.peer)
                            })
                            .cloned()
                            .collect();
                        (*loc, conns)
                    })
                    .filter(|(_, conns)| !conns.is_empty())
                    .collect()
            };

            let adjustment = self
                .connection_manager
                .topology_manager
                .write()
                .adjust_topology(
                    &neighbor_locations,
                    &self.connection_manager.own_location().location,
                    Instant::now(),
                );
            match adjustment {
                TopologyAdjustment::AddConnections(target_locs) => {
                    pending_conn_adds.extend(target_locs);
                }
                TopologyAdjustment::RemoveConnections(mut should_disconnect_peers) => {
                    for peer in should_disconnect_peers.drain(..) {
                        notifier
                            .send(Either::Right(crate::message::NodeEvent::DropConnection(
                                peer.peer,
                            )))
                            .await
                            .map_err(|error| {
                                tracing::debug!(
                                    ?error,
                                    "Shutting down connection maintenance task"
                                );
                                error
                            })?;
                    }
                }
                TopologyAdjustment::NoChange => {}
            }

            tokio::select! {
              _ = refresh_density_map.tick() => {
                self.refresh_density_request_cache();
              }
              _ = check_interval.tick() => {}
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self, notifier, live_tx_tracker), fields(peer = %self.connection_manager.pub_key))]
    async fn acquire_new(
        &self,
        ideal_location: Location,
        skip_list: &HashSet<&PeerId>,
        notifier: &EventLoopNotificationsSender,
        live_tx_tracker: &LiveTransactionTracker,
    ) -> anyhow::Result<Option<Transaction>> {
        // First find a query target using just the input skip list
        let query_target = {
            let router = self.router.read();
            if let Some(t) = self.connection_manager.routing(
                ideal_location,
                None,
                skip_list, // Use just the input skip list for finding who to query
                &router,
            ) {
                t
            } else {
                return Ok(None);
            }
        };

        // Now create the complete skip list for the connect request
        let new_skip_list = skip_list
            .iter()
            .copied()
            .cloned()
            .chain(self.connection_manager.connected_peers())
            .collect();

        let joiner = self.connection_manager.own_location();
        tracing::debug!(
            this_peer = %joiner,
            %query_target,
            %ideal_location,
            skip_list = ?new_skip_list,
            "Adding new connections"
        );
        let missing_connections = self.connection_manager.max_connections - self.open_connections();
        let id = Transaction::new::<connect::ConnectMsg>();
        live_tx_tracker.add_transaction(query_target.peer.clone(), id);
        let msg = connect::ConnectMsg::Request {
            id,
            target: query_target.clone(),
            msg: connect::ConnectRequest::FindOptimalPeer {
                query_target,
                ideal_location,
                joiner,
                max_hops_to_live: missing_connections,
                skip_list: new_skip_list,
            },
        };
        notifier.send(Either::Left(msg.into())).await?;
        Ok(Some(id))
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RingError {
    #[error(transparent)]
    ConnError(#[from] Box<node::ConnectionError>),
    #[error("No ring connections found")]
    EmptyRing,
    #[error("Ran out of, or haven't found any, caching peers for contract {0}")]
    NoCachingPeers(ContractKey),
    #[error("No location assigned to this peer")]
    NoLocation,
}
