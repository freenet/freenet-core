use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
};

use libp2p::identity;
use rand::Rng;

use crate::{
    conn_manager::PeerKey,
    contract::MemoryContractHandler,
    node::{event_listener::TestEventListener, InitPeerNode, NodeInMemory},
    ring::{Distance, Location},
    user_events::test_utils::MemoryEventsGen,
    NodeConfig,
};

use super::SimStorageError;

pub fn get_free_port() -> Result<u16, ()> {
    let mut port;
    for _ in 0..100 {
        port = get_dynamic_port();
        let bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        if let Ok(conn) = TcpListener::bind(bind_addr) {
            std::mem::drop(conn);
            return Ok(port);
        }
    }
    Err(())
}

pub fn get_dynamic_port() -> u16 {
    const FIRST_DYNAMIC_PORT: u16 = 49152;
    const LAST_DYNAMIC_PORT: u16 = 65535;
    rand::thread_rng().gen_range(FIRST_DYNAMIC_PORT..LAST_DYNAMIC_PORT)
}

/// A simulated in-memory network topology.
pub(crate) struct SimNetwork {
    event_listener: TestEventListener,
    labels: HashMap<String, PeerKey>,
}

impl SimNetwork {
    fn new() -> Self {
        Self {
            event_listener: TestEventListener::new(),
            labels: HashMap::new(),
        }
    }

    pub fn build(network_size: usize, ring_max_htl: usize, rnd_if_htl_above: usize) -> SimNetwork {
        let mut sim = SimNetwork::new();

        // build gateway node
        // let probe_protocol = Some(ProbeProtocol::new(ring_protocol.clone(), loc));
        const GW_LABEL: &str = "gateway";
        let gateway_pair = identity::Keypair::generate_ed25519();
        let gateway_peer_id = gateway_pair.public().into_peer_id();
        let gateway_port = get_free_port().unwrap();
        let gateway_loc = Location::random();
        let config = NodeConfig::new()
            .with_ip(Ipv6Addr::LOCALHOST)
            .with_port(gateway_port)
            .with_key(gateway_pair)
            .with_location(gateway_loc)
            .max_hops_to_live(ring_max_htl)
            .rnd_if_htl_above(rnd_if_htl_above);
        sim.event_listener
            .add_node(GW_LABEL.to_string(), PeerKey::from(gateway_peer_id));
        let gateway = NodeInMemory::<SimStorageError>::build::<MemoryContractHandler>(
            config,
            Some(Box::new(sim.event_listener.clone())),
        )
        .unwrap();
        sim.initialize_peer(gateway, GW_LABEL.to_string());

        // add other nodes to the simulation
        for node_no in 0..(network_size - 1) {
            let label = format!("node-{}", node_no);
            let id = identity::Keypair::generate_ed25519()
                .public()
                .into_peer_id();
            sim.event_listener
                .add_node(label.clone(), PeerKey::from(id));

            let config = NodeConfig::new()
                .add_provider(
                    InitPeerNode::new()
                        .listening_ip(Ipv6Addr::LOCALHOST)
                        .listening_port(gateway_port)
                        .with_identifier(id),
                )
                .max_hops_to_live(ring_max_htl)
                .rnd_if_htl_above(rnd_if_htl_above);
            sim.initialize_peer(
                NodeInMemory::<SimStorageError>::build::<MemoryContractHandler>(
                    config,
                    Some(Box::new(sim.event_listener.clone())),
                )
                .unwrap(),
                label,
            );
        }
        sim
    }

    fn initialize_peer(&mut self, mut peer: NodeInMemory<SimStorageError>, label: String) {
        let user_events = MemoryEventsGen::new();
        self.labels.insert(label, peer.peer_key);
        tokio::spawn(async move { peer.listen_on(user_events).await });
    }

    pub fn connected(&mut self, peer: &str) -> bool {
        if let Some(key) = self.labels.get(peer) {
            self.event_listener.registered_connection(*key)
        } else {
            false
        }
    }

    /// Query the network for connectivity stats.
    pub async fn ring_distribution(&self) {}
}

/// Builds an histogram of the distribution in the ring of each node relative to each other.
pub(crate) fn ring_distribution<'a>(
    nodes: impl Iterator<Item = &'a NodeInMemory<SimStorageError>> + 'a,
) -> impl Iterator<Item = Distance> + 'a {
    // TODO: groupby certain intervals
    // e.g. grouping func: (it * 200.0).roundToInt().toDouble() / 200.0
    nodes
        .map(|node| {
            let node_ring = &node.op_storage.ring;
            let self_loc = node_ring.own_location().location.unwrap();
            node_ring
                .connections_by_location
                .read()
                .keys()
                .map(|d| self_loc.distance(d))
                .collect::<Vec<_>>()
        })
        .flatten()
}
