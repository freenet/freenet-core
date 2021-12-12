use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
    time::{Duration, Instant},
};

use itertools::Itertools;
use libp2p::{identity, PeerId};
use rand::Rng;
use tokio::sync::watch::{channel, Receiver, Sender};

use crate::contract::{Contract, ContractKey, ContractValue};
use crate::user_events::UserEvent;
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
    pub labels: HashMap<String, PeerKey>,
    usr_ev_controller: Sender<(EventId, PeerKey)>,
    receiver_ch: Receiver<(EventId, PeerKey)>,
    gateways: Vec<(NodeInMemory<SimStorageError>, GatewayConfig)>,
    nodes: Vec<(NodeInMemory<SimStorageError>, String)>,
    ring_max_htl: usize,
    rnd_if_htl_above: usize,
    max_connections: usize,
    min_connections: usize,
}

pub(crate) type EventId = usize;

#[derive(Clone)]
pub(crate) struct NodeSpecification {
    /// Pair of contract and the initial value
    pub owned_contracts: Vec<(Contract, ContractValue)>,
    pub non_owned_contracts: Vec<ContractKey>,
    pub events_to_generate: HashMap<EventId, UserEvent>,
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
        min_connections: usize,
    ) -> Self {
        assert!(gateways > 0 && nodes > 0);
        let (usr_ev_controller, _rcv_copy) = channel((0, PeerKey::random()));
        let mut net = Self {
            event_listener: TestEventListener::new(),
            labels: HashMap::new(),
            usr_ev_controller,
            receiver_ch: _rcv_copy,
            gateways: Vec::with_capacity(gateways),
            nodes: Vec::with_capacity(nodes),
            ring_max_htl,
            rnd_if_htl_above,
            max_connections,
            min_connections,
        };
        net.build_gateways(gateways);
        net.build_nodes(nodes);
        net
    }

    fn build_gateways(&mut self, num: usize) {
        let mut configs = Vec::with_capacity(num);
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
                .min_number_of_connections(self.min_connections)
                .rnd_if_htl_above(self.rnd_if_htl_above);

            self.event_listener
                .add_node(label.clone(), PeerKey::from(id));
            configs.push((
                config,
                GatewayConfig {
                    label,
                    id,
                    port,
                    location,
                },
            ));
        }

        for (mut this_node, this_config) in configs.clone() {
            for GatewayConfig {
                port, id, location, ..
            } in configs.iter().filter_map(|(_, config)| {
                if this_config.label != config.label {
                    Some(config)
                } else {
                    None
                }
            }) {
                this_node.add_gateway(
                    InitPeerNode::new(*id, *location)
                        .listening_ip(Ipv6Addr::LOCALHOST)
                        .listening_port(*port),
                );
            }

            let gateway = NodeInMemory::<SimStorageError>::build::<MemoryContractHandler>(
                this_node,
                Some(Box::new(self.event_listener.clone())),
            )
            .unwrap();
            self.gateways.push((gateway, this_config));
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
        self.build_with_specs(HashMap::new())
    }

    pub fn build_with_specs(&mut self, mut specs: HashMap<String, NodeSpecification>) {
        for (node, label) in self
            .nodes
            .drain(..)
            .chain(self.gateways.drain(..).map(|(n, c)| (n, c.label)))
            .collect::<Vec<_>>()
        {
            let node_spec = specs.remove(&label);
            self.initialize_peer(node, label, node_spec);
        }
    }

    fn initialize_peer(
        &mut self,
        mut peer: NodeInMemory<SimStorageError>,
        label: String,
        node_specs: Option<NodeSpecification>,
    ) {
        let mut user_events = MemoryEventsGen::new(self.receiver_ch.clone(), peer.peer_key);
        if let Some(specs) = node_specs.clone() {
            user_events.has_contract(specs.owned_contracts);
            user_events.request_contracts(specs.non_owned_contracts);
            user_events.generate_events(specs.events_to_generate);
        }
        self.labels.insert(label, peer.peer_key);
        tokio::spawn(async move {
            if let Some(specs) = node_specs {
                peer.append_contracts(specs.owned_contracts)
                    .await
                    .map_err(|_| anyhow::anyhow!("failed inserting test owned contracts"))?;
            }
            peer.listen_on(user_events).await
        });
    }

    pub fn connected(&self, peer: &str) -> bool {
        if let Some(key) = self.labels.get(peer) {
            self.event_listener.is_connected(*key)
        } else {
            false
        }
    }

    /// Builds an histogram of the distribution in the ring of each node relative to each other.
    pub fn ring_distribution(&self, scale: i32) -> impl Iterator<Item = (f64, usize)> {
        let mut all_dists = Vec::with_capacity(self.labels.len());
        for (.., key) in &self.labels {
            all_dists.push(self.event_listener.connections(*key).into_iter());
        }
        group_locations_in_buckets(all_dists.into_iter().flatten().map(|(_, l)| l), scale)
    }

    /// Returns the connectivity in the network per peer (that is all the connections
    /// this peers has registered).
    pub fn node_connectivity(&self) -> HashMap<String, HashMap<PeerKey, Distance>> {
        let mut peers_connections = HashMap::with_capacity(self.labels.len());
        for (label, key) in &self.labels {
            peers_connections.insert(
                label.clone(),
                self.event_listener
                    .connections(*key)
                    .collect::<HashMap<_, _>>(),
            );
        }
        peers_connections
    }

    pub fn trigger_event(&self, label: &str, event_id: EventId) -> Result<(), anyhow::Error> {
        let peer = self
            .labels
            .get(label)
            .ok_or_else(|| anyhow::anyhow!("node not found"))?;
        self.usr_ev_controller
            .send((event_id, *peer))
            .expect("node listeners disconnected");
        Ok(())
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

pub(crate) async fn check_connectivity(
    sim_nodes: &SimNetwork,
    num_nodes: usize,
    wait_time: Duration,
) -> Result<(), anyhow::Error> {
    let mut connected = HashSet::new();
    let elapsed = Instant::now();
    while elapsed.elapsed() < wait_time && connected.len() < num_nodes {
        for node in 0..num_nodes {
            if !connected.contains(&node) && sim_nodes.connected(&format!("node-{}", node)) {
                connected.insert(node);
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let expected = HashSet::from_iter(0..num_nodes);
    let missing: Vec<_> = expected
        .difference(&connected)
        .map(|n| {
            let label = format!("node-{}", n);
            let key = sim_nodes.labels[&label];
            (label, key)
        })
        .collect();
    if !missing.is_empty() {
        log::error!("Nodes without connection: {:?}", missing);
        log::error!("Total nodes without connection: {:?}", missing.len());
    }
    assert!(missing.is_empty());
    log::info!(
        "Required time for connecting all peers: {} secs",
        elapsed.elapsed().as_secs()
    );

    let hist: Vec<_> = sim_nodes.ring_distribution(1).collect();
    log::info!("Ring distribution: {:?}", hist);

    let node_connectivity = sim_nodes.node_connectivity();
    let mut connections_per_peer: Vec<_> = node_connectivity
        .iter()
        .map(|(k, v)| (k, v.len()))
        .filter_map(|(k, v)| {
            if !k.starts_with("gateway") {
                Some(v)
            } else {
                None
            }
        })
        .collect();

    // ensure at least some normal nodes have more than one connection
    connections_per_peer.sort_unstable_by_key(|num_conn| *num_conn);
    assert!(connections_per_peer.iter().last().unwrap() > &1);

    // ensure the average number of connections per peer is above N
    let avg_connections: usize = connections_per_peer.iter().sum::<usize>() / num_nodes;
    log::info!("Average connections: {}", avg_connections);
    assert!(avg_connections > 1);
    Ok(())
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

    let mut grouped: Vec<_> = group_locations_in_buckets(locations, 2).collect();
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
