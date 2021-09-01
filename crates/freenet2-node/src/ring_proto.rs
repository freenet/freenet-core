use std::collections::{BTreeMap, HashSet};

use crate::conn_manager::{ConnectionManager, Location, PeerKey, PeerKeyLocation};

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
    // FIXME: key should be a `Location` type but f64 cannot impl Eq + Ord
    connections_by_location: BTreeMap<usize, PeerKeyLocation>,
}

impl Ring {
    fn new() -> Self {
        Ring {
            connections_by_location: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;

    struct SimulatedNode {
        ring_protocol: RingProtocol,
    }

    struct ConnManagerMock;

    impl ConnectionManager for ConnManagerMock {
        fn on_remove_conn(&mut self, _func: crate::conn_manager::RemoveConnHandle) {}
    }

    fn simulated_network_builder(
        network_size: usize,
        ring_max_htl: usize,
        rnd_if_htl_above: usize,
        // per_node_delay: usize,
    ) -> HashMap<String, SimulatedNode> {
        let mut nodes = HashMap::new();

        // gateway node
        let ring_protocol = RingProtocol {
            conn_manager: Box::new(ConnManagerMock),
            peer_key: PeerKey,
            location: Location::new(),
            gateways: HashSet::new(),
            max_hops_to_live: ring_max_htl,
            rnd_if_htl_above,
            ring: Ring::new(),
        };
        nodes.insert("gateway".to_owned(), SimulatedNode { ring_protocol });

        // add other nodes to the simulation
        for node_no in 0..network_size {
            let label = format!("node-{}", node_no);
            let ring_protocol = RingProtocol {
                conn_manager: Box::new(ConnManagerMock),
                peer_key: PeerKey,
                location: Location::new(),
                gateways: HashSet::new(),
                max_hops_to_live: ring_max_htl,
                rnd_if_htl_above,
                ring: Ring::new(),
            };
            nodes.insert(label, SimulatedNode { ring_protocol });
        }
        nodes
    }

    /// Given a network of one node and one gateway test that both are connected.
    #[test]
    fn node0_to_gateway_conn() {
        let ring_protocols = simulated_network_builder(1, 1, 0);

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
}
