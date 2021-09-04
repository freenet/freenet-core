use std::{
    collections::{BTreeMap, HashSet},
    convert::TryFrom,
    fmt::Display,
    hash::Hasher,
    sync::Arc,
};

// use crossbeam::channel::{unbounded, Receiver, Sender};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{
    conn_manager::{self, ConnectionManager, PeerKey, PeerKeyLocation, Transport},
    message::{Message, MessageType},
    StdResult,
};

use self::messages::OpenConnection;

type Result<T> = std::result::Result<T, RingProtoError>;

struct RingProtocol<CM> {
    conn_manager: CM,
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
    ) -> Self {
        RingProtocol {
            conn_manager,
            peer_key,
            location: RwLock::new(None),
            gateways: HashSet::new(),
            max_hops_to_live,
            rnd_if_htl_above,
            ring: Ring::new(Location::random()),
        }
    }

    fn listen_for_close_conn() {
        todo!()
    }

    fn listen_for_join_req() {
        todo!()
    }

    fn join_ring(self: Arc<Self>) -> Result<()> {
        if self.conn_manager.transport().is_open() && self.gateways.is_empty() {
            return Err(RingProtoError::Join);
        }

        // FIXME: this iteration should be shuffled, must write an extension
        // iterator shuffle items "in place"
        for gateway in self.gateways.iter() {
            log::info!("Joining ring via {gateway}", gateway = gateway.location);
            self.conn_manager.add_connection(*gateway, true);
            let join_req = messages::JoinRequest::Initial { key: self.peer_key };
            log::debug!(
                "Sending {req:?} to {gateway}",
                req = join_req,
                gateway = gateway.location
            );

            let ring_proto = self.clone();
            let join_response = move |sender, join_res: Message| -> conn_manager::Result<()> {
                log::debug!(
                    "JoinResponse received from {} of type: {}",
                    sender,
                    join_res
                );

                let accepted = if let Message::JoinResponse(messages::JoinResponse::Initial {
                    accepted,
                    your_location,
                    ..
                }) = join_res
                {
                    let loc = &mut *ring_proto.location.write();
                    *loc = Some(your_location);
                    accepted
                } else {
                    return Err(conn_manager::ConnError::UnexpectedResponseMessage(join_res));
                };

                let self_location = &*ring_proto.location.read();
                let self_location =
                    &self_location.ok_or(conn_manager::ConnError::LocationUnknown)?;
                for new_peer_key in accepted {
                    if ring_proto.ring.should_accept(self_location) {
                        log::info!("Establishing connection to {}", new_peer_key.peer);
                        ring_proto.establish_conn(new_peer_key);
                    } else {
                        log::debug!("Not accepting connection to {}", new_peer_key.peer);
                    }
                }

                Ok(())
            };
            self.conn_manager.send(&gateway.peer, join_response);
        }

        Ok(())
    }

    // TODO: should be async since it performs I/O
    fn establish_conn(&self, new_peer: PeerKeyLocation) {
        self.conn_manager.add_connection(new_peer, false);

        let state = messages::OpenConnection::Connecting;

        let callback = Box::new(move |s, k| -> conn_manager::Result<()> {
            //;
            Ok(())
        });

        self.conn_manager
            .listen(Message::from(state).msg_type_id(), callback);
        // self.conn_manager.send(to, call_back)
    }
}

#[derive(Debug)]
struct Ring {
    connections_by_location: BTreeMap<Location, PeerKeyLocation>,
    location: Location,
}

impl Ring {
    fn new(location: Location) -> Self {
        Ring {
            connections_by_location: BTreeMap::new(),
            location,
        }
    }

    fn should_accept(&self, location: &Location) -> bool {
        todo!()
    }
}

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy)]
pub(crate) struct Location(f64);

impl Location {
    /// Returns a new random location.
    pub fn random() -> Self {
        use rand::prelude::*;
        let mut rng = rand::thread_rng();
        Location(rng.gen_range(0.0..=1.0))
    }

    /// Compute the distance between two locations.
    pub fn distance(&self, other: &Location) -> f64 {
        let d = (self.0 - other.0).abs();
        if d < 0.5 {
            d
        } else {
            1.0 - d
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
    use crate::message::MessageId;

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum JoinRequest {
        Initial { key: PeerKey },
        Proxy { joiner: PeerKeyLocation },
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum JoinResponse {
        Initial {
            accepted: Vec<PeerKeyLocation>,
            reply_to: MessageId,
            your_location: Location,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub(crate) enum OpenConnection {
        OCReceived,
        Connecting,
        Connected,
    }

    impl OpenConnection {
        pub fn is_initiate(&self) -> bool {
            match self {
                OpenConnection::Connecting => true,
                _ => false,
            }
        }

        pub(super) fn transition(&mut self, other_state: Self) {
            match other_state {
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
        message::ProbeRequest,
        probe_proto::ProbeProtocol,
        tests::{InMemoryTransport, TestingConnectionManager},
    };

    #[test]
    fn node0_to_gateway_conn() -> StdResult<(), Box<dyn std::error::Error>> {
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

    #[test]
    fn all_nodes_should_connect() -> StdResult<(), Box<dyn std::error::Error>> {
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
        ring_protocol: RingProtocol<TestingConnectionManager>,
        probe_protocol: ProbeProtocol<TestingConnectionManager>,
    }

    fn sim_network_builder(
        network_size: usize,
        ring_max_htl: usize,
        rnd_if_htl_above: usize,
        // _per_node_delay: usize,
    ) -> HashMap<String, SimulatedNode> {
        let mut nodes = HashMap::new();

        let keypair = identity::Keypair::generate_ed25519();
        let conn_manager = TestingConnectionManager::new();

        // build gateway node
        let ring_protocol = RingProtocol::new(
            conn_manager.clone(),
            keypair.public().into(),
            ring_max_htl,
            rnd_if_htl_above,
        );
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
            let conn_manager = TestingConnectionManager::new();
            let ring_protocol = RingProtocol::new(
                conn_manager.clone(),
                keypair.public().into(),
                ring_max_htl,
                rnd_if_htl_above,
            );
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
    ) -> impl Iterator<Item = f64> + 'a {
        // TODO: groupby  certain intervals
        // e.g. grouping func: (it * 200.0).roundToInt().toDouble() / 200.0
        nodes
            .map(|node| {
                let node_ring = &node.ring_protocol.ring;
                node_ring
                    .connections_by_location
                    .keys()
                    .map(|d| node_ring.location.distance(d))
                    .collect::<Vec<_>>()
            })
            .flatten()
    }
}
