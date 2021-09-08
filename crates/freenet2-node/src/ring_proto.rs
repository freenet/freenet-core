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
    conn_manager::{self, ConnectionManager, PeerKey, PeerKeyLocation, Transport},
    message::{Message, MsgType, TransactionId},
    ring_proto::messages::{JoinRequest, JoinResponse},
    StdResult,
};

use self::messages::OpenConnection;

type Result<T> = std::result::Result<T, RingProtoError>;

struct RingProtocol<CM> {
    conn_manager: Arc<CM>,
    peer_key: PeerKey,
    /// A location gets assigned once a node joins the network via a gateway,
    /// until then it has no location unless the node is a gateway.
    location: RwLock<Option<Location>>,
    gateways: HashSet<PeerKeyLocation>,
    max_hops_to_live: usize,
    rnd_if_htl_above: usize,
    ring: Ring,
    // notify_state_transition: Receiver<>,
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
            gateways: HashSet::new(),
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

    fn listen_for_join_req(self: &Arc<Self>) {
        let self_c = self.clone();
        let listening_to =
            move |sender: PeerKeyLocation, msg: Message| -> conn_manager::Result<()> {
                let (tx_id, join_req) = if let Message::JoinRequest(id, join_req) = msg {
                    (id, join_req)
                } else {
                    return Err(conn_manager::ConnError::UnexpectedResponseMessage(msg));
                };
                log::debug!(
                    "JoinRequest received by {} with HTL {:?}",
                    sender.peer,
                    join_req
                );

                let peer_key_loc;
                let req_type;

                enum ReqType {
                    Initial,
                    Proxy,
                }

                let join_req_hpt = match join_req {
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

                let your_location = self_c
                    .location
                    .read()
                    .ok_or(conn_manager::ConnError::LocationUnknown)?;
                let accepted_by = if self_c
                    .ring
                    .should_accept(&your_location, &peer_key_loc.location)
                {
                    log::debug!(
                        "Accepting connections to {:?}, establising connection",
                        peer_key_loc
                    );
                    self_c.establish_conn(peer_key_loc);
                    vec![peer_key_loc]
                } else {
                    log::debug!("Not accepting new connection sender {:?}", peer_key_loc);
                    Vec::new()
                };

                log::debug!(
                    "Sending JoinResponse to {} accepting {} connections",
                    sender.peer,
                    accepted_by.len()
                );
                let join_response = match req_type {
                    ReqType::Initial => Message::from((
                        tx_id,
                        JoinResponse::Initial {
                            accepted_by: accepted_by.clone(),
                            your_location: peer_key_loc.location,
                            your_peer_id: peer_key_loc.peer,
                        },
                    )),
                    ReqType::Proxy => Message::from((
                        tx_id,
                        JoinResponse::Proxy {
                            accepted_by: accepted_by.clone(),
                        },
                    )),
                };
                self_c.conn_manager.send(sender.peer, tx_id, join_response);

                if join_req_hpt > 0 && !self_c.ring.connections_by_location.is_empty() {
                    let forward_to = if join_req_hpt >= self_c.rnd_if_htl_above {
                        log::info!(
                            "Randomly selecting peer to forward JoinRequest sender {}",
                            sender.peer
                        );
                        self_c.ring.random_peer(|p| *p != &sender)
                    } else {
                        log::info!(
                            "Selecting close peer to forward request sender {}",
                            sender.peer
                        );
                        self_c
                            .ring
                            .connections_by_location
                            .get(&peer_key_loc.location)
                            .filter(|it| it.peer != sender.peer)
                            .copied()
                    }
                    .map(|p| p.peer);

                    if let Some(forward_to) = forward_to {
                        let forwarded = Message::from((
                            tx_id,
                            JoinRequest::Proxy {
                                joiner: peer_key_loc,
                                hops_to_live: join_req_hpt.min(self_c.max_hops_to_live) - 1,
                            },
                        ));

                        let forwarded_acceptors =
                            Arc::new(Mutex::new(accepted_by.into_iter().collect::<HashSet<_>>()));

                        log::info!(
                            "Forwarding JoinRequest sender {} to {}",
                            sender.peer,
                            forward_to
                        );
                        let self_c2 = self_c.clone();
                        let callback = move |jr_sender, join_resp| -> conn_manager::Result<()> {
                            if let Message::JoinResponse(tx_id, resp) = join_resp {
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
                                    tx_id,
                                    JoinResponse::Proxy {
                                        accepted_by: new_acceptors,
                                    },
                                ));
                                self_c2.conn_manager.send(jr_sender, tx_id, msg);
                            };
                            Ok(())
                        };
                        self_c
                            .conn_manager
                            .send_with_callback(forward_to, tx_id, forwarded, callback);
                    }
                }

                Ok(())
            };
        self.conn_manager
            .listen(<JoinRequest as MsgType>::msg_type_id(), listening_to);
    }

    fn join_ring(self: &Arc<Self>) -> Result<()> {
        if self.conn_manager.transport().is_open() && self.gateways.is_empty() {
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
        for gateway in self.gateways.iter() {
            log::info!("Joining ring via {}", gateway.location);
            self.conn_manager.add_connection(*gateway, true);
            let tx_id = TransactionId::new(<JoinRequest as MsgType>::msg_type_id());
            let join_req = messages::JoinRequest::Initial {
                key: self.peer_key,
                hops_to_live: self.max_hops_to_live,
            };
            log::debug!(
                "Sending {req:?} to {gateway}",
                req = join_req,
                gateway = gateway.peer
            );

            let ring_proto = self.clone();
            let join_response_cb = move |sender, join_res: Message| -> conn_manager::Result<()> {
                log::debug!("JoinResponse received from {} of type {}", sender, join_res);
                let accepted_by = if let Message::JoinResponse(
                    _tx_id,
                    messages::JoinResponse::Initial {
                        accepted_by,
                        your_location,
                        ..
                    },
                ) = join_res
                {
                    if _tx_id != tx_id {
                        return Err(conn_manager::ConnError::UnexpectedTx(tx_id, _tx_id));
                    }
                    let loc = &mut *ring_proto.location.write();
                    *loc = Some(your_location);
                    accepted_by
                } else {
                    return Err(conn_manager::ConnError::UnexpectedResponseMessage(join_res));
                };

                let self_location = &*ring_proto.location.read();
                let self_location =
                    &self_location.ok_or(conn_manager::ConnError::LocationUnknown)?;
                for new_peer_key in accepted_by {
                    if ring_proto.ring.should_accept(self_location, self_location) {
                        log::info!("Establishing connection to {}", new_peer_key.peer);
                        ring_proto.establish_conn(new_peer_key);
                    } else {
                        log::debug!("Not accepting connection to {}", new_peer_key.peer);
                    }
                }

                Ok(())
            };
            let msg: Message = (tx_id, join_req).into();
            self.conn_manager
                .send_with_callback(gateway.peer, tx_id, msg, join_response_cb);
        }

        Ok(())
    }

    fn establish_conn(&self, new_peer: PeerKeyLocation) {
        self.conn_manager.add_connection(new_peer, false);
        let conn_manager = self.conn_manager.clone();
        let state = Arc::new(RwLock::new(messages::OpenConnection::Connecting));
        let state_copy = state.clone();
        let callback = Box::new(
            move |peer: PeerKeyLocation, msg: Message| -> conn_manager::Result<()> {
                let state = state_copy;
                let (tx_id, oc) = match msg {
                    Message::OpenConnection(tx_id, oc) => (tx_id, oc),
                    msg => return Err(conn_manager::ConnError::UnexpectedResponseMessage(msg)),
                };
                let mut current_state = state.write();
                current_state.transition(oc);
                if !current_state.is_connected() {
                    let open_conn: Message = (tx_id, *current_state).into();
                    log::debug!("Acknowledging OC");
                    conn_manager.send(peer.peer, *open_conn.id(), open_conn);
                }
                Ok(())
            },
        );
        let msg_id = TransactionId::new(<OpenConnection as MsgType>::msg_type_id());
        let _handler = self.conn_manager.listen_to_replies(msg_id, callback);

        let conn_manager = self.conn_manager.clone();
        tokio::spawn(async move {
            let curr_time = Instant::now();
            let state = *state.read();
            while !state.is_connected() && curr_time.elapsed() <= Duration::from_secs(30) {
                log::debug!("Sending {:?} to {:?}", state, new_peer);
                conn_manager.send(
                    new_peer.peer,
                    msg_id,
                    Message::OpenConnection(msg_id, state),
                );
                tokio::time::sleep(Duration::from_millis(200)).await
            }
        });
    }
}

#[derive(Debug)]
struct Ring {
    connections_by_location: BTreeMap<Location, PeerKeyLocation>,
}

impl Ring {
    const MIN_CONNECTIONS: usize = 10;
    const MAX_CONNECTIONS: usize = 20;

    fn new() -> Self {
        Ring {
            connections_by_location: BTreeMap::new(),
        }
    }

    fn should_accept(&self, my_location: &Location, location: &Location) -> bool {
        if location == my_location || self.connections_by_location.contains_key(location) {
            false
        } else if self.connections_by_location.len() < Self::MIN_CONNECTIONS {
            true
        } else if self.connections_by_location.len() >= Self::MAX_CONNECTIONS {
            false
        } else {
            my_location.distance(location) < self.median_distance_to(my_location)
        }
    }

    fn median_distance_to(&self, location: &Location) -> Distance {
        let mut conn_by_dist = self.connections_by_distance(location);
        conn_by_dist.sort_unstable();
        let idx = self.connections_by_location.len() / 2;
        conn_by_dist[idx]
    }

    fn connections_by_distance(&self, to: &Location) -> Vec<Distance> {
        self.connections_by_location
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
            .values()
            .filter(filter_fn)
            .next()
            .copied()
    }
}

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
pub(crate) struct Location(f64);

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
}

pub(crate) mod messages {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
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

    #[derive(Debug, Serialize, Deserialize)]
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
    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
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
            match other_host_state {
                Self::Connecting => *self = Self::OCReceived,
                Self::OCReceived | Self::Connected => *self = Self::Connected,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use libp2p::identity;
    use rand::Rng;

    use super::*;
    use crate::{
        message::ProbeRequest, probe_proto::ProbeProtocol, tests::TestingConnectionManager,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn node0_to_gateway_conn() -> StdResult<(), Box<dyn std::error::Error>> {
        //! Given a network of one node and one gateway test that both are connected.

        let ring_protocols = sim_network_builder(1, 1, 0);

        // TODO: in order for this test to pass connection should be established between both nodes.

        assert_eq!(
            ring_protocols["gateway"]
                .ring_protocol
                .ring
                .connections_by_location
                .len(),
            1
        );

        assert_eq!(
            ring_protocols["node-0"]
                .ring_protocol
                .ring
                .connections_by_location
                .len(),
            1
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn all_nodes_should_connect() -> StdResult<(), Box<dyn std::error::Error>> {
        //! Given a network of 1000 peers all nodes should have connections.

        let mut sim_nodes = sim_network_builder(200, 10, 7);

        // let _hist: Vec<_> = _ring_distribution(sim_nodes.values()).collect();

        for probe_idx in 0..10 {
            let target = Location::random();
            let idx: usize = rand::thread_rng().gen_range(0..sim_nodes.len());
            let rnd_node = sim_nodes
                .get_mut(&format!("node-{}", idx))
                .ok_or("node not found")?;
            let probe_response = rnd_node.probe_protocol.probe(ProbeRequest);
            println!("Probe #{}, target: {}", probe_idx, target);
            println!("hop\tlocation\tlatency");
            for visit in &probe_response.visits {
                println!(
                    "{}\t{}\t{}",
                    visit.hop,
                    visit.location,
                    visit.latency.as_secs()
                );
            }
        }

        let any_empties = sim_nodes
            .values()
            .map(|node| node.ring_protocol.ring.connections_by_location.is_empty())
            .any(|is_empty| is_empty);
        assert!(!any_empties);

        Ok(())
    }

    struct SimulatedNode {
        ring_protocol: Arc<RingProtocol<TestingConnectionManager>>,
        probe_protocol: ProbeProtocol<TestingConnectionManager>,
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
        let conn_manager = TestingConnectionManager::new(true);
        let ring_protocol = RingProtocol::new(
            conn_manager.clone(),
            keypair.public().into(),
            ring_max_htl,
            rnd_if_htl_above,
        )
        .with_location(Location::random());
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
            let conn_manager = TestingConnectionManager::new(false);
            let ring_protocol = RingProtocol::new(
                conn_manager.clone(),
                keypair.public().into(),
                ring_max_htl,
                rnd_if_htl_above,
            );
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
                    .keys()
                    .map(|d| self_loc.distance(d))
                    .collect::<Vec<_>>()
            })
            .flatten()
    }
}
