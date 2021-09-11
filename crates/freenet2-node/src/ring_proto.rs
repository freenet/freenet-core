#![allow(unused)] // FIXME: remove this attr

use std::{
    collections::{BTreeMap, HashSet},
    convert::TryFrom,
    fmt::Display,
    hash::Hasher,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::{
    conn_manager::{self, ConnectionManager, ListenerHandle, PeerKey, PeerKeyLocation, Transport},
    message::{Message, MsgType, Transaction},
    ring_proto::messages::{JoinRequest, JoinResponse},
    StdResult,
};

type Result<T> = StdResult<T, RingProtoError>;

struct RingProtocol<CM> {
    conn_manager: Arc<CM>,
    peer_key: PeerKey,
    /// A location gets assigned once a node joins the network via a gateway,
    /// until then it has no location unless the node is a gateway.
    location: RwLock<Option<Location>>,
    gateways: RwLock<HashSet<PeerKeyLocation>>,
    max_hops_to_live: usize,
    rnd_if_htl_above: usize,
    ring: Ring,
}

impl<CM, T> RingProtocol<CM>
where
    T: Transport + 'static,
    CM: ConnectionManager<Transport = T> + 'static,
{
    fn new(
        conn_manager: CM,
        peer_key: PeerKey,
        max_hops_to_live: usize,
        rnd_if_htl_above: usize,
    ) -> Arc<Self> {
        Arc::new(RingProtocol {
            conn_manager: Arc::new(conn_manager),
            peer_key,
            location: RwLock::new(None),
            gateways: RwLock::new(HashSet::new()),
            max_hops_to_live,
            rnd_if_htl_above,
            ring: Ring::new(),
        })
    }

    pub fn with_location(self: Arc<Self>, loc: Location) -> Arc<Self> {
        *self.location.write() = Some(loc);
        self
    }

    fn listen_for_close_conn(&self) {
        todo!()
    }

    fn listen_for_join_req(self: &Arc<Self>) -> ListenerHandle {
        let self_cp = self.clone();
        let process_join_req = move |sender: PeerKey, msg: Message| -> conn_manager::Result<()> {
            let (tx, join_req) = if let Message::JoinRequest(id, join_req) = msg {
                (id, join_req)
            } else {
                return Err(conn_manager::ConnError::UnexpectedResponseMessage(msg));
            };

            enum ReqType {
                Initial,
                Proxy,
            }

            let peer_key_loc;
            let req_type;
            let jr_hpt = match join_req {
                messages::JoinRequest::Initial { key, hops_to_live } => {
                    peer_key_loc = PeerKeyLocation {
                        peer: key,
                        location: Location::random(),
                    };
                    req_type = ReqType::Initial;
                    hops_to_live
                }
                messages::JoinRequest::Proxy {
                    joiner,
                    hops_to_live,
                } => {
                    peer_key_loc = joiner;
                    req_type = ReqType::Proxy;
                    hops_to_live
                }
            };
            log::debug!("JoinRequest received by {} with HTL {}", sender, jr_hpt);

            let your_location = self_cp
                .location
                .read()
                .ok_or(conn_manager::ConnError::LocationUnknown)?;
            let accepted_by = if self_cp
                .ring
                .should_accept(&your_location, &peer_key_loc.location)
            {
                log::debug!(
                    "Accepting connections to {:?}, establising connection @ {}",
                    peer_key_loc,
                    self_cp.peer_key
                );
                self_cp.establish_conn(peer_key_loc, tx);
                vec![PeerKeyLocation {
                    peer: self_cp.peer_key,
                    location: your_location,
                }]
            } else {
                log::debug!("Not accepting new connection sender {:?}", peer_key_loc);
                Vec::new()
            };

            log::debug!(
                "Sending JoinResponse to {} accepting {} connections",
                sender,
                accepted_by.len()
            );
            let join_response = match req_type {
                ReqType::Initial => Message::from((
                    tx,
                    JoinResponse::Initial {
                        accepted_by: accepted_by.clone(),
                        your_location: peer_key_loc.location,
                        your_peer_id: peer_key_loc.peer,
                    },
                )),
                ReqType::Proxy => Message::from((
                    tx,
                    JoinResponse::Proxy {
                        accepted_by: accepted_by.clone(),
                    },
                )),
            };
            self_cp.conn_manager.send(peer_key_loc, tx, join_response)?;

            if jr_hpt > 0 && !self_cp.ring.connections_by_location.read().is_empty() {
                let forward_to = if jr_hpt >= self_cp.rnd_if_htl_above {
                    log::debug!(
                        "Randomly selecting peer to forward JoinRequest sender {}",
                        sender
                    );
                    self_cp.ring.random_peer(|p| p.peer != sender)
                } else {
                    log::debug!("Selecting close peer to forward request sender {}", sender);
                    self_cp
                        .ring
                        .connections_by_location
                        .read()
                        .get(&peer_key_loc.location)
                        .filter(|it| it.peer != sender)
                        .copied()
                };

                if let Some(forward_to) = forward_to {
                    let forwarded = Message::from((
                        tx,
                        JoinRequest::Proxy {
                            joiner: peer_key_loc,
                            hops_to_live: jr_hpt.min(self_cp.max_hops_to_live) - 1,
                        },
                    ));

                    let forwarded_acceptors =
                        Arc::new(Mutex::new(accepted_by.into_iter().collect::<HashSet<_>>()));

                    log::debug!(
                        "Forwarding JoinRequest sender {} to {}",
                        sender,
                        forward_to.peer
                    );
                    let self_cp2 = self_cp.clone();
                    let register_acceptors =
                        move |jr_sender: PeerKeyLocation, join_resp| -> conn_manager::Result<()> {
                            if let Message::JoinResponse(tx, resp) = join_resp {
                                let new_acceptors = match resp {
                                    JoinResponse::Initial { accepted_by, .. } => accepted_by,
                                    JoinResponse::Proxy { accepted_by, .. } => accepted_by,
                                };
                                let fa = &mut *forwarded_acceptors.lock();
                                new_acceptors.iter().for_each(|p| {
                                    if !fa.contains(p) {
                                        fa.insert(*p);
                                    }
                                });
                                let msg = Message::from((
                                    tx,
                                    JoinResponse::Proxy {
                                        accepted_by: new_acceptors,
                                    },
                                ));
                                self_cp2.conn_manager.send(jr_sender, tx, msg)?;
                            };
                            Ok(())
                        };
                    self_cp.conn_manager.send_with_callback(
                        forward_to,
                        tx,
                        forwarded,
                        register_acceptors,
                    )?;
                }
            }
            Ok(())
        };
        self.conn_manager
            .listen(<JoinRequest as MsgType>::msg_type_id(), process_join_req)
    }

    fn join_ring(self: &Arc<Self>) -> Result<()> {
        if self.conn_manager.transport().is_open() && self.gateways.read().is_empty() {
            match *self.location.read() {
                Some(loc) => {
                    log::info!(
                        "No gateways to join through, listening for connections at loc: {}",
                        loc
                    );
                    return Ok(());
                }
                None => return Err(RingProtoError::Join),
            }
        }

        // FIXME: this iteration should be shuffled, must write an extension iterator shuffle items "in place"
        // the idea here is to limit the amount of gateways being contacted that's why shuffling is required
        for gateway in self.gateways.read().iter() {
            log::info!("Joining ring via {} at {}", gateway.peer, gateway.location);
            self.conn_manager.add_connection(*gateway, true);
            let tx = Transaction::new(<JoinRequest as MsgType>::msg_type_id());
            let join_req = messages::JoinRequest::Initial {
                key: self.peer_key,
                hops_to_live: self.max_hops_to_live,
            };
            log::debug!("Sending {:?} to {}", join_req, gateway.peer);

            let ring_proto = self.clone();
            let join_response_cb =
                move |sender: PeerKeyLocation, join_res: Message| -> conn_manager::Result<()> {
                    let (accepted_by, tx) = if let Message::JoinResponse(
                        incoming_tx,
                        messages::JoinResponse::Initial {
                            accepted_by,
                            your_location,
                            ..
                        },
                    ) = join_res
                    {
                        log::debug!("JoinResponse received from {}", sender.peer,);
                        if incoming_tx != tx {
                            return Err(conn_manager::ConnError::UnexpectedTx(tx, incoming_tx));
                        }
                        let loc = &mut *ring_proto.location.write();
                        *loc = Some(your_location);
                        (accepted_by, incoming_tx)
                    } else {
                        return Err(conn_manager::ConnError::UnexpectedResponseMessage(join_res));
                    };

                    let self_location = &*ring_proto.location.read();
                    let self_location =
                        &self_location.ok_or(conn_manager::ConnError::LocationUnknown)?;
                    for new_peer_key in accepted_by {
                        if ring_proto
                            .ring
                            .should_accept(self_location, &new_peer_key.location)
                        {
                            log::info!("Establishing connection to {}", new_peer_key.peer);
                            ring_proto.establish_conn(new_peer_key, tx);
                        } else {
                            log::debug!("Not accepting connection to {}", new_peer_key.peer);
                        }
                    }

                    Ok(())
                };
            log::debug!("Initiating JoinRequest transaction: {}", tx);
            let msg: Message = (tx, join_req).into();
            self.conn_manager
                .send_with_callback(*gateway, tx, msg, join_response_cb)?;
        }

        Ok(())
    }

    fn establish_conn(self: &Arc<Self>, new_peer: PeerKeyLocation, tx: Transaction) {
        self.conn_manager.add_connection(new_peer, false);
        let self_cp = self.clone();
        let state = Arc::new(RwLock::new(messages::OpenConnection::Connecting));

        let state_cp = state.clone();
        let ack_peer = move |peer: PeerKeyLocation, msg: Message| -> conn_manager::Result<()> {
            let (tx, oc) = match msg {
                Message::OpenConnection(tx, oc) => (tx, oc),
                msg => return Err(conn_manager::ConnError::UnexpectedResponseMessage(msg)),
            };
            let mut current_state = state_cp.write();
            current_state.transition(oc);
            if !current_state.is_connected() {
                let open_conn: Message = (tx, *current_state).into();
                log::debug!("Acknowledging OC");
                self_cp
                    .conn_manager
                    .send(peer, *open_conn.id(), open_conn)?;
            } else {
                log::info!(
                    "{} connected to {}, adding to ring",
                    self_cp.peer_key,
                    new_peer.peer
                );
                self_cp.conn_manager.send(
                    peer,
                    tx,
                    Message::from((tx, messages::OpenConnection::Connected)),
                )?;
                self_cp
                    .ring
                    .connections_by_location
                    .write()
                    .insert(new_peer.location, new_peer);
            }
            Ok(())
        };
        self.conn_manager.listen_to_replies(tx, ack_peer);

        let conn_manager = self.conn_manager.clone();
        tokio::spawn(async move {
            let curr_time = Instant::now();
            let mut attempts = 0;
            while !state.read().is_connected() && curr_time.elapsed() <= Duration::from_secs(30) {
                log::debug!(
                    "Sending {} to {}, number of messages sent: {}",
                    *state.read(),
                    new_peer.peer,
                    attempts
                );
                conn_manager.send(new_peer, tx, Message::OpenConnection(tx, *state.read()))?;
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(200)).await
            }
            if curr_time.elapsed() > Duration::from_secs(30) {
                log::error!("Timed out trying to connect to {}", new_peer.peer);
                Err(conn_manager::ConnError::NegotationFailed)
            } else {
                conn_manager.remove_listener(tx);
                log::info!("Success negotiating connection to {}", new_peer.peer);
                Ok(())
            }
        });
    }
}

#[derive(Debug)]
struct Ring {
    connections_by_location: RwLock<BTreeMap<Location, PeerKeyLocation>>,
}

impl Ring {
    const MIN_CONNECTIONS: usize = 10;
    const MAX_CONNECTIONS: usize = 20;

    fn new() -> Self {
        Ring {
            connections_by_location: RwLock::new(BTreeMap::new()),
        }
    }

    fn should_accept(&self, my_location: &Location, location: &Location) -> bool {
        let cbl = &*self.connections_by_location.read();
        if location == my_location || cbl.contains_key(location) {
            false
        } else if cbl.len() < Self::MIN_CONNECTIONS {
            true
        } else if cbl.len() >= Self::MAX_CONNECTIONS {
            false
        } else {
            my_location.distance(location) < self.median_distance_to(my_location)
        }
    }

    fn median_distance_to(&self, location: &Location) -> Distance {
        let mut conn_by_dist = self.connections_by_distance(location);
        conn_by_dist.sort_unstable();
        let idx = self.connections_by_location.read().len() / 2;
        conn_by_dist[idx]
    }

    fn connections_by_distance(&self, to: &Location) -> Vec<Distance> {
        self.connections_by_location
            .read()
            .keys()
            .map(|key| key.distance(to))
            .collect()
    }

    fn random_peer<F>(&self, filter_fn: F) -> Option<PeerKeyLocation>
    where
        F: FnMut(&&PeerKeyLocation) -> bool,
    {
        // FIXME: should be optimized
        self.connections_by_location
            .read()
            .values()
            .find(filter_fn)
            .copied()
    }
}

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct Location(f64);

type Distance = Location;

impl Location {
    /// Returns a new random location.
    pub fn random() -> Self {
        use rand::prelude::*;
        let mut rng = rand::thread_rng();
        Location(rng.gen_range(0.0..=1.0))
    }

    /// Compute the distance between two locations.
    pub fn distance(&self, other: &Location) -> Distance {
        let d = (self.0 - other.0).abs();
        if d < 0.5 {
            Location(d)
        } else {
            Location(1.0 - d)
        }
    }
}

impl Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.to_string().as_str())?;
        Ok(())
    }
}

impl PartialEq for Location {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

/// Since we don't allow NaN values in the construction of Location
/// we can safely assume that an equivalence relation holds.  
impl Eq for Location {}

impl Ord for Location {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .expect("always should return a cmp value")
    }
}

impl PartialOrd for Location {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl std::hash::Hash for Location {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let bits = self.0.to_bits();
        state.write_u64(bits);
        state.finish();
    }
}

impl TryFrom<f64> for Location {
    type Error = ();

    fn try_from(value: f64) -> StdResult<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            Err(())
        } else {
            Ok(Location(value))
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RingProtoError {
    #[error("failed while attempting to join a ring")]
    Join,
    #[error(transparent)]
    ConnError(#[from] conn_manager::ConnError),
}

pub(crate) mod messages {
    use super::*;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum JoinRequest {
        Initial {
            key: PeerKey,
            hops_to_live: usize,
        },
        Proxy {
            joiner: PeerKeyLocation,
            hops_to_live: usize,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum JoinResponse {
        Initial {
            accepted_by: Vec<PeerKeyLocation>,
            your_location: Location,
            your_peer_id: PeerKey,
        },
        Proxy {
            accepted_by: Vec<PeerKeyLocation>,
        },
    }

    /// A stateful connection attempt.
    #[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum OpenConnection {
        OCReceived,
        Connecting,
        Connected,
    }

    impl OpenConnection {
        pub fn is_initiated(&self) -> bool {
            matches!(self, OpenConnection::Connecting)
        }

        pub fn is_connected(&self) -> bool {
            matches!(self, OpenConnection::Connected)
        }

        pub(super) fn transition(&mut self, other_host_state: Self) {
            match (*self, other_host_state) {
                (Self::Connected, _) => {}
                (_, Self::Connecting) => *self = Self::OCReceived,
                (_, Self::OCReceived | Self::Connected) => *self = Self::Connected,
            }
        }
    }

    impl Display for OpenConnection {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "OpenConnection::{:?}", self)
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use libp2p::identity;
    use rand::Rng;

    use super::{messages::OpenConnection, *};
    use crate::{
        config::tracing::Logger, conn_manager::in_memory::MemoryConnManager, message::ProbeRequest,
        probe_proto::ProbeProtocol,
    };

    #[test]
    fn open_connection_state_transition() {
        let mut oc0 = OpenConnection::Connecting;
        let oc1 = OpenConnection::Connecting;
        oc0.transition(oc1);
        assert_eq!(oc0, OpenConnection::OCReceived);

        let mut oc0 = OpenConnection::Connecting;
        let oc1 = OpenConnection::OCReceived;
        oc0.transition(oc1);
        assert!(oc0.is_connected());

        let mut oc0 = OpenConnection::Connecting;
        let oc1 = OpenConnection::Connected;
        oc0.transition(oc1);
        assert!(oc0.is_connected());

        let mut oc0 = OpenConnection::Connecting;
        let oc1 = OpenConnection::OCReceived;
        oc0.transition(oc1);
        assert!(oc0.is_connected());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node0_to_gateway_conn() -> StdResult<(), Box<dyn std::error::Error>> {
        //! Given a network of one node and one gateway test that both are connected.
        Logger::init_logger();

        let ring_protocols = sim_network_builder(1, 1, 0);
        tokio::time::sleep(Duration::from_secs(3)).await;

        assert_eq!(
            ring_protocols["node-0"]
                .ring_protocol
                .ring
                .connections_by_location
                .read()
                .len(),
            1
        );

        assert_eq!(
            ring_protocols["gateway"]
                .ring_protocol
                .ring
                .connections_by_location
                .read()
                .len(),
            1
        );

        Ok(())
    }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn all_nodes_should_connect() -> StdResult<(), Box<dyn std::error::Error>> {
        //! Given a network of 1000 peers all nodes should have connections.
        Logger::init_logger();

        let mut sim_nodes = sim_network_builder(200, 10, 7);

        // let _hist: Vec<_> = _ring_distribution(sim_nodes.values()).collect();

        for probe_idx in 0..10 {
            let target = Location::random();
            let idx: usize = rand::thread_rng().gen_range(0..sim_nodes.len());
            let rnd_node = sim_nodes
                .get_mut(&format!("node-{}", idx))
                .ok_or("node not found")?;
            let probe_response = rnd_node.probe_protocol.probe(ProbeRequest);
            log::trace!("Probe #{}, target: {}", probe_idx, target);
            log::trace!("hop\tlocation\tlatency");
            for visit in &probe_response.visits {
                log::trace!(
                    "{}\t{}\t{}",
                    visit.hop,
                    visit.location,
                    visit.latency.as_secs()
                );
            }
        }

        let any_empties = sim_nodes
            .values()
            .map(|node| {
                node.ring_protocol
                    .ring
                    .connections_by_location
                    .read()
                    .is_empty()
            })
            .any(|is_empty| is_empty);
        assert!(!any_empties);

        Ok(())
    }

    struct SimulatedNode {
        ring_protocol: Arc<RingProtocol<MemoryConnManager>>,
        probe_protocol: ProbeProtocol<MemoryConnManager>,
    }

    fn sim_network_builder(
        network_size: usize,
        ring_max_htl: usize,
        rnd_if_htl_above: usize,
        // _per_node_delay: usize,
    ) -> HashMap<String, SimulatedNode> {
        let mut nodes = HashMap::new();

        // build gateway node
        let keypair = identity::Keypair::generate_ed25519();
        let gw_key = keypair.public().into();
        let loc = Location::random();
        let conn_manager = MemoryConnManager::new(true, gw_key, Some(loc));
        let ring_protocol =
            RingProtocol::new(conn_manager.clone(), gw_key, ring_max_htl, rnd_if_htl_above)
                .with_location(loc);
        ring_protocol.listen_for_join_req();
        let probe_protocol = ProbeProtocol::new(conn_manager);
        nodes.insert(
            "gateway".to_owned(),
            SimulatedNode {
                ring_protocol,
                probe_protocol,
            },
        );

        // add other nodes to the simulation
        for node_no in 0..network_size {
            let label = format!("node-{}", node_no);
            let keypair = identity::Keypair::generate_ed25519();
            let peer_key = keypair.public().into();
            let conn_manager = MemoryConnManager::new(false, peer_key, None);
            let ring_protocol = RingProtocol::new(
                conn_manager.clone(),
                peer_key,
                ring_max_htl,
                rnd_if_htl_above,
            );
            ring_protocol.gateways.write().insert(PeerKeyLocation {
                peer: gw_key,
                location: loc,
            });
            ring_protocol.join_ring().unwrap();
            let probe_protocol = ProbeProtocol::new(conn_manager);

            nodes.insert(
                label,
                SimulatedNode {
                    ring_protocol,
                    probe_protocol,
                },
            );
        }
        nodes
    }

    /// Builds an histogram of the distribution in the ring of each node relative to each other.
    fn _ring_distribution<'a>(
        nodes: impl Iterator<Item = &'a SimulatedNode> + 'a,
    ) -> impl Iterator<Item = Distance> + 'a {
        // TODO: groupby  certain intervals
        // e.g. grouping func: (it * 200.0).roundToInt().toDouble() / 200.0
        nodes
            .map(|node| {
                let node_ring = &node.ring_protocol.ring;
                let self_loc = node.ring_protocol.location.read().unwrap();
                node_ring
                    .connections_by_location
                    .read()
                    .keys()
                    .map(|d| self_loc.distance(d))
                    .collect::<Vec<_>>()
            })
            .flatten()
    }
}
