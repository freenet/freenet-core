use std::{
    collections::{BTreeMap, HashSet},
    convert::TryFrom,
    fmt::Display,
    hash::Hasher,
};

use crate::conn_manager::{ConnectionManager, PeerKey, PeerKeyLocation};

struct RingProtocol {
    conn_manager: Box<dyn ConnectionManager>,
    peer_key: PeerKey,
    location: Location,
    gateways: HashSet<PeerKey>,
    max_hops_to_live: usize,
    rnd_if_htl_above: usize,
    ring: Ring,
}

impl RingProtocol {
    fn listen_for_close_conn() {
        todo!()
    }

    fn listen_for_join_req() {
        todo!()
    }

    fn join_ring() {
        todo!()
    }

    fn establish_conn(&mut self, new_peer: PeerKeyLocation) {
        todo!()
    }
}

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
}

/// An abstract location on the 1D ring, represented by a real number on the interal [0, 1]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
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

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            Err(())
        } else {
            Ok(Location(value))
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use rand::Rng;

    use super::*;
    use crate::{
        conn_manager::{ListenHandler, ListenerReaction, RemoveConnHandle},
        message::{Message, ProbeRequest},
        probe_proto::ProbeProtocol,
    };

    struct SimulatedNode {
        ring_protocol: RingProtocol,
        probe_protocol: ProbeProtocol,
    }

    #[derive(Clone)]
    struct ConnManagerMock;

    impl ConnectionManager for ConnManagerMock {
        fn on_remove_conn(&mut self, _func: RemoveConnHandle) {}
        fn listen(&mut self, reaction: ListenerReaction) -> ListenHandler {
            ListenHandler
        }
    }

    fn sim_network_builder(
        network_size: usize,
        ring_max_htl: usize,
        rnd_if_htl_above: usize,
        // _per_node_delay: usize,
    ) -> HashMap<String, SimulatedNode> {
        let mut nodes = HashMap::new();

        let conn_manager = Box::new(ConnManagerMock);
        // gateway node
        let ring_protocol = RingProtocol {
            conn_manager: conn_manager.clone(),
            peer_key: PeerKey,
            location: Location::random(),
            gateways: HashSet::new(),
            max_hops_to_live: ring_max_htl,
            rnd_if_htl_above,
            ring: Ring::new(Location::random()),
        };
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
            let conn_manager = Box::new(ConnManagerMock);
            let ring_protocol = RingProtocol {
                conn_manager: conn_manager.clone(),
                peer_key: PeerKey,
                location: Location::random(),
                gateways: HashSet::new(),
                max_hops_to_live: ring_max_htl,
                rnd_if_htl_above,
                ring: Ring::new(Location::random()),
            };
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

    #[test]
    fn node0_to_gateway_conn() {
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

    #[test]
    fn all_nodes_should_connect() -> Result<(), ()> {
        let mut sim_nodes = sim_network_builder(200, 10, 7);

        // let _hist: Vec<_> = _ring_distribution(sim_nodes.values()).collect();

        for probe_idx in 0..10 {
            let target = Location::random();
            let idx: usize = rand::thread_rng().gen_range(0..sim_nodes.len());
            let rnd_node = sim_nodes.get_mut(&format!("node-{}", idx)).ok_or(())?;
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

        Ok(())
    }
}
