use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
};

use libp2p::{identity, PeerId};
use rand::Rng;
use tokio::sync::watch::{channel, Receiver, Sender};

use crate::{
    conn_manager::PeerKey,
    contract::MemoryContractHandler,
    node::{event_listener::TestEventListener, InitPeerNode, NodeInMemory},
    ring::Location,
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
    usr_ev_controller: Sender<PeerKey>,
    receiver_ch: Receiver<PeerKey>,
    gateways: Vec<(NodeInMemory<SimStorageError>, GatewayConfig)>,
    nodes: Vec<(NodeInMemory<SimStorageError>, String)>,
    ring_max_htl: usize,
    rnd_if_htl_above: usize,
    max_connections: usize,
}

#[derive(Clone)]
struct GatewayConfig {
    label: String,
    port: u16,
    id: PeerId,
    location: Location,
}

impl SimNetwork {
    pub fn new(
        gateways: usize,
        nodes: usize,
        ring_max_htl: usize,
        rnd_if_htl_above: usize,
        max_connections: usize,
    ) -> Self {
        assert!(gateways > 0 && nodes > 0);
        let (usr_ev_controller, _rcv_copy) = channel(PeerKey::random());
        let mut net = Self {
            event_listener: TestEventListener::new(),
            labels: HashMap::new(),
            usr_ev_controller,
            receiver_ch: _rcv_copy,
            gateways: Vec::new(),
            nodes: Vec::new(),
            ring_max_htl,
            rnd_if_htl_above,
            max_connections,
        };
        net.build_gateways(gateways);
        net.build_nodes(nodes);
        net
    }

    fn build_gateways(&mut self, num: usize) {
        for node_no in 0..num {
            let label = format!("gateway-{}", node_no);
            let pair = identity::Keypair::generate_ed25519();
            let id = pair.public().into_peer_id();
            let port = get_free_port().unwrap();
            let location = Location::random();

            let mut config = NodeConfig::new();
            config
                .with_ip(Ipv6Addr::LOCALHOST)
                .with_port(port)
                .with_key(pair)
                .with_location(location)
                .max_hops_to_live(self.ring_max_htl)
                .max_number_of_connections(self.max_connections)
                .rnd_if_htl_above(self.rnd_if_htl_above);

            self.event_listener
                .add_node(label.clone(), PeerKey::from(id));

            let gateway = NodeInMemory::<SimStorageError>::build::<MemoryContractHandler>(
                config,
                Some(Box::new(self.event_listener.clone())),
            )
            .unwrap();
            self.gateways.push((
                gateway,
                GatewayConfig {
                    label,
                    port,
                    location,
                    id,
                },
            ));
        }
    }

    fn build_nodes(&mut self, num: usize) {
        let gateways: Vec<_> = self
            .gateways
            .iter()
            .map(|(_node, config)| config)
            .cloned()
            .collect();

        for node_no in 0..num {
            let label = format!("node-{}", node_no);
            let pair = identity::Keypair::generate_ed25519();
            let id = pair.public().into_peer_id();

            let mut config = NodeConfig::new();
            for GatewayConfig {
                port, id, location, ..
            } in &gateways
            {
                config.add_gateway(
                    InitPeerNode::new(*id, *location)
                        .listening_ip(Ipv6Addr::LOCALHOST)
                        .listening_port(*port),
                );
            }
            config
                .max_hops_to_live(self.ring_max_htl)
                .rnd_if_htl_above(self.rnd_if_htl_above)
                .max_number_of_connections(self.max_connections)
                .with_key(pair);

            self.event_listener
                .add_node(label.clone(), PeerKey::from(id));

            let node = NodeInMemory::<SimStorageError>::build::<MemoryContractHandler>(
                config,
                Some(Box::new(self.event_listener.clone())),
            )
            .unwrap();
            self.nodes.push((node, label));
        }
    }

    pub fn build(&mut self) {
        for (node, label) in self
            .nodes
            .drain(..)
            .chain(self.gateways.drain(..).map(|(n, c)| (n, c.label)))
            .collect::<Vec<_>>()
        {
            self.initialize_peer(node, label);
        }
    }

    fn initialize_peer(&mut self, mut peer: NodeInMemory<SimStorageError>, label: String) {
        let user_events = MemoryEventsGen::new(self.receiver_ch.clone(), peer.peer_key);
        self.labels.insert(label, peer.peer_key);
        tokio::spawn(async move { peer.listen_on(user_events).await });
    }

    pub fn connected(&self, peer: &str) -> bool {
        if let Some(key) = self.labels.get(peer) {
            self.event_listener.registered_connection(*key)
        } else {
            false
        }
    }

    /// Query the network for connectivity stats.
    pub async fn ring_distribution(&self) {}
}

// /// Builds an histogram of the distribution in the ring of each node relative to each other.
// pub(crate) fn ring_distribution<'a>(
//     nodes: impl Iterator<Item = &'a NodeInMemory<SimStorageError>> + 'a,
// ) -> impl Iterator<Item = Distance> + 'a {
//     // TODO: groupby certain intervals
//     // e.g. grouping func: (it * 200.0).roundToInt().toDouble() / 200.0
//     nodes
//         .map(|node| {
//             let node_ring = &node.op_storage.ring;
//             let self_loc = node_ring.own_location().location.unwrap();
//             node_ring
//                 .connections_by_location
//                 .read()
//                 .keys()
//                 .map(|d| self_loc.distance(d))
//                 .collect::<Vec<_>>()
//         })
//         .flatten()
// }
