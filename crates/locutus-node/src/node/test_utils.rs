use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
};

use itertools::Itertools;
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
            gateways: Vec::with_capacity(gateways),
            nodes: Vec::with_capacity(gateways),
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

    /// Builds an histogram of the distribution in the ring of each node relative to each other.
    pub fn ring_distribution(&self) -> impl Iterator<Item = (f64, usize)> {
        let mut all_dists = Vec::with_capacity(self.labels.len());
        for (.., key) in &self.labels {
            all_dists.push(self.event_listener.connections(*key).into_iter());
        }
        group_locations_in_buckets(all_dists.into_iter().flatten().map(|(_, l)| l), 1)
    }
}

fn group_locations_in_buckets(
    locs: impl IntoIterator<Item = Location>,
    scale: i32,
) -> impl Iterator<Item = (f64, usize)> {
    let mut distances = HashMap::new();
    for (bucket, group) in &locs
        .into_iter()
        .group_by(|l| (l.0 * (10.0f64).powi(scale)).floor() as u32)
    {
        let count = group.count();
        distances
            .entry(bucket)
            .and_modify(|c| *c += count)
            .or_insert(count);
    }
    distances
        .into_iter()
        .map(move |(k, v)| ((k as f64 / (10.0f64).powi(scale)) as f64, v))
}

#[test]
fn group_locations_test() -> Result<(), anyhow::Error> {
    let locations = vec![
        Location::try_from(0.5356)?,
        Location::try_from(0.5435)?,
        Location::try_from(0.5468)?,
        Location::try_from(0.5597)?,
        Location::try_from(0.6745)?,
        Location::try_from(0.7309)?,
        Location::try_from(0.7412)?,
    ];

    let mut grouped: Vec<_> =
        group_locations_in_buckets(locations.clone().into_iter(), 1).collect();
    grouped.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    assert_eq!(grouped, vec![(0.5, 4), (0.6, 1), (0.7, 2)]);

    let mut grouped: Vec<_> = group_locations_in_buckets(locations, 1).collect();
    grouped.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    assert_eq!(
        grouped,
        vec![
            (0.53, 1),
            (0.54, 2),
            (0.55, 1),
            (0.67, 1),
            (0.73, 1),
            (0.74, 1)
        ]
    );

    Ok(())
}
