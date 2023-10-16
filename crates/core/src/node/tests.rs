use std::{
    collections::{HashMap, HashSet},
    fmt::Write,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
    sync::Arc,
    time::{Duration, Instant},
};

use freenet_stdlib::client_api::ClientRequest;
use freenet_stdlib::prelude::*;
use itertools::Itertools;
use libp2p::{identity, PeerId};
use rand::Rng;
use tokio::sync::watch::{channel, Receiver, Sender};
use tracing::{info, instrument};

use crate::{
    client_events::test::MemoryEventsGen,
    config::GlobalExecutor,
    node::{event_log::TestEventListener, InitPeerNode, NodeBuilder, NodeInMemory},
    ring::{Distance, Location, PeerKeyLocation},
};

use super::PeerKey;

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

pub(crate) type EventId = usize;

#[derive(PartialEq, Eq, Hash, Clone)]
pub(crate) struct NodeLabel(Arc<str>);

impl NodeLabel {
    fn gateway(id: usize) -> Self {
        Self(format!("gateway-{id}").into())
    }

    fn node(id: usize) -> Self {
        Self(format!("node-{id}").into())
    }

    fn is_gateway(&self) -> bool {
        self.0.starts_with("gateway")
    }
}

impl std::fmt::Display for NodeLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Deref for NodeLabel {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a> From<&'a str> for NodeLabel {
    fn from(value: &'a str) -> Self {
        assert!(value.starts_with("gateway-") || value.starts_with("node-"));
        let mut parts = value.split('-');
        assert!(parts.next().is_some());
        assert!(parts
            .next()
            .map(|s| s.parse::<u16>())
            .transpose()
            .expect("should be an u16")
            .is_some());
        assert!(parts.next().is_none());
        Self(value.to_string().into())
    }
}

#[derive(Clone)]
pub(crate) struct NodeSpecification {
    /// Pair of contract and the initial value
    pub owned_contracts: Vec<(ContractContainer, WrappedState)>,
    pub non_owned_contracts: Vec<ContractKey>,
    pub events_to_generate: HashMap<EventId, ClientRequest<'static>>,
    pub contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
}

#[derive(Clone)]
struct GatewayConfig {
    label: NodeLabel,
    port: u16,
    id: PeerId,
    location: Location,
}

/// A simulated in-memory network topology.
pub(crate) struct SimNetwork {
    name: String,
    debug: bool,
    pub labels: HashMap<NodeLabel, PeerKey>,
    pub event_listener: TestEventListener,
    user_ev_controller: Sender<(EventId, PeerKey)>,
    receiver_ch: Receiver<(EventId, PeerKey)>,
    gateways: Vec<(NodeInMemory, GatewayConfig)>,
    nodes: Vec<(NodeInMemory, NodeLabel)>,
    ring_max_htl: usize,
    rnd_if_htl_above: usize,
    max_connections: usize,
    min_connections: usize,
}

impl SimNetwork {
    pub async fn new(
        name: &str,
        gateways: usize,
        nodes: usize,
        ring_max_htl: usize,
        rnd_if_htl_above: usize,
        max_connections: usize,
        min_connections: usize,
    ) -> Self {
        assert!(gateways > 0 && nodes > 0);
        let (user_ev_controller, receiver_ch) = channel((0, PeerKey::random()));
        let mut net = Self {
            name: name.into(),
            debug: false,
            event_listener: TestEventListener::new(),
            labels: HashMap::new(),
            user_ev_controller,
            receiver_ch,
            gateways: Vec::with_capacity(gateways),
            nodes: Vec::with_capacity(nodes),
            ring_max_htl,
            rnd_if_htl_above,
            max_connections,
            min_connections,
        };
        net.build_gateways(gateways).await;
        net.build_nodes(nodes).await;
        net
    }

    #[allow(unused)]
    pub fn debug(&mut self) {
        self.debug = true;
    }

    #[instrument(skip(self))]
    async fn build_gateways(&mut self, num: usize) {
        info!("Building {} gateways", num);
        let mut configs = Vec::with_capacity(num);
        for node_no in 0..num {
            let label = NodeLabel::gateway(node_no);
            let pair = identity::Keypair::generate_ed25519();
            let id = pair.public().to_peer_id();
            let port = get_free_port().unwrap();
            let location = Location::random();

            let mut config = NodeBuilder::new([Box::new(MemoryEventsGen::new(
                self.receiver_ch.clone(),
                PeerKey::from(id),
            ))]);
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

        let gateways: Vec<_> = configs.iter().map(|(_, gw)| gw.clone()).collect();
        for (mut this_node, this_config) in configs {
            for GatewayConfig {
                port, id, location, ..
            } in gateways
                .iter()
                .filter(|config| this_config.label != config.label)
            {
                this_node.add_gateway(
                    InitPeerNode::new(*id, *location)
                        .listening_ip(Ipv6Addr::LOCALHOST)
                        .listening_port(*port),
                );
            }
            let gateway = NodeInMemory::build(
                this_node,
                self.event_listener.clone(),
                format!("{}-{label}", self.name, label = this_config.label),
            )
            .await
            .unwrap();
            self.gateways.push((gateway, this_config));
        }
    }

    #[instrument(skip(self))]
    async fn build_nodes(&mut self, num: usize) {
        info!("Building {} regular nodes", num);
        let gateways: Vec<_> = self
            .gateways
            .iter()
            .map(|(_node, config)| config)
            .cloned()
            .collect();

        for node_no in 0..num {
            let label = NodeLabel::node(node_no);
            let pair = identity::Keypair::generate_ed25519();
            let id = pair.public().to_peer_id();

            let mut config = NodeBuilder::new([Box::new(MemoryEventsGen::new(
                self.receiver_ch.clone(),
                PeerKey::from(id),
            ))]);
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

            let node = NodeInMemory::build(
                config,
                self.event_listener.clone(),
                format!("{}-{label}", self.name),
            )
            .await
            .unwrap();
            self.nodes.push((node, label));
        }
    }

    pub async fn build(&mut self) {
        self.build_with_specs(HashMap::new()).await
    }

    pub async fn build_with_specs(&mut self, mut specs: HashMap<NodeLabel, NodeSpecification>) {
        let mut gw_not_init = self.gateways.len();
        let gw = self.gateways.drain(..).map(|(n, c)| (n, c.label));
        for (node, label) in gw.chain(self.nodes.drain(..)).collect::<Vec<_>>() {
            let node_spec = specs.remove(&label);
            self.initialize_peer(node, label, node_spec);
            if gw_not_init != 0 {
                gw_not_init -= 1;
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    fn initialize_peer(
        &mut self,
        mut peer: NodeInMemory,
        label: NodeLabel,
        node_specs: Option<NodeSpecification>,
    ) {
        let mut user_events = MemoryEventsGen::new(self.receiver_ch.clone(), peer.peer_key);
        if let Some(specs) = node_specs.clone() {
            user_events.has_contract(specs.owned_contracts);
            user_events.request_contracts(specs.non_owned_contracts);
            user_events.generate_events(specs.events_to_generate);
        }
        self.labels.insert(label, peer.peer_key);
        GlobalExecutor::spawn(async move {
            if let Some(specs) = node_specs {
                peer.append_contracts(specs.owned_contracts, specs.contract_subscribers)
                    .await
                    .map_err(|_| anyhow::anyhow!("failed inserting test owned contracts"))?;
            }
            peer.run_node(user_events).await
        });
    }

    pub fn get_locations_by_node(&self) -> HashMap<NodeLabel, PeerKeyLocation> {
        let mut locations_by_node: HashMap<NodeLabel, PeerKeyLocation> = HashMap::new();

        // Get node and gateways location by label
        for (node, label) in &self.nodes {
            locations_by_node.insert(label.clone(), node.op_storage.ring.own_location());
        }
        for (node, config) in &self.gateways {
            locations_by_node.insert(config.label.clone(), node.op_storage.ring.own_location());
        }
        locations_by_node
    }

    pub fn connected(&self, peer: &NodeLabel) -> bool {
        if let Some(key) = self.labels.get(peer) {
            self.event_listener.is_connected(key)
        } else {
            panic!("peer not found");
        }
    }

    pub fn has_put_contract(
        &self,
        peer: &NodeLabel,
        key: &ContractKey,
        value: &WrappedState,
    ) -> bool {
        if let Some(pk) = self.labels.get(peer) {
            self.event_listener.has_put_contract(pk, key, value)
        } else {
            panic!("peer not found");
        }
    }

    pub fn has_got_contract(&self, peer: &NodeLabel, key: &ContractKey) -> bool {
        if let Some(pk) = self.labels.get(peer) {
            self.event_listener.has_got_contract(pk, key)
        } else {
            panic!("peer not found");
        }
    }

    /// Builds an histogram of the distribution in the ring of each node relative to each other.
    pub fn ring_distribution(&self, scale: i32) -> impl Iterator<Item = (f64, usize)> {
        let mut all_dists = Vec::with_capacity(self.labels.len());
        for (.., key) in &self.labels {
            all_dists.push(self.event_listener.connections(*key));
        }
        group_locations_in_buckets(
            all_dists.into_iter().flatten().map(|(_, l)| l.as_f64()),
            scale,
        )
    }

    /// Returns the connectivity in the network per peer (that is all the connections
    /// this peers has registered).
    pub fn node_connectivity(&self) -> HashMap<NodeLabel, HashMap<NodeLabel, Distance>> {
        let mut peers_connections = HashMap::with_capacity(self.labels.len());
        let key_to_label: HashMap<_, _> = self.labels.iter().map(|(k, v)| (v, k)).collect();
        for (label, key) in &self.labels {
            peers_connections.insert(
                label.clone(),
                self.event_listener
                    .connections(*key)
                    .map(|(k, d)| (key_to_label[&k].clone(), d))
                    .collect::<HashMap<_, _>>(),
            );
        }
        peers_connections
    }

    pub async fn trigger_event(
        &self,
        label: &NodeLabel,
        event_id: EventId,
        await_for: Option<Duration>,
    ) -> Result<(), anyhow::Error> {
        let peer = self
            .labels
            .get(label)
            .ok_or_else(|| anyhow::anyhow!("node not found"))?;
        self.user_ev_controller
            .send((event_id, *peer))
            .expect("node listeners disconnected");
        if let Some(sleep_time) = await_for {
            tokio::time::sleep(sleep_time).await;
        }
        Ok(())
    }
}

impl Drop for SimNetwork {
    fn drop(&mut self) {
        if !self.debug {
            for label in self.labels.keys() {
                let p = std::env::temp_dir()
                    .join(format!("freenet-executor-{sim}-{label}", sim = self.name));
                let _ = std::fs::remove_dir_all(p);
            }
        }
    }
}

fn group_locations_in_buckets(
    locs: impl IntoIterator<Item = f64>,
    scale: i32,
) -> impl Iterator<Item = (f64, usize)> {
    let mut distances = HashMap::new();
    for (bucket, group) in &locs
        .into_iter()
        .group_by(|l| (l * (10.0f64).powi(scale)).floor() as u32)
    {
        let count = group.count();
        distances
            .entry(bucket)
            .and_modify(|c| *c += count)
            .or_insert(count);
    }
    distances
        .into_iter()
        .map(move |(k, v)| ((k as f64 / (10.0f64).powi(scale)), v))
}

pub(crate) async fn check_connectivity(
    sim_nodes: &SimNetwork,
    num_nodes: usize,
    time_out: Duration,
) -> Result<(), anyhow::Error> {
    let mut connected = HashSet::new();
    let elapsed = Instant::now();
    while elapsed.elapsed() < time_out && connected.len() < num_nodes {
        for node in 0..num_nodes {
            if !connected.contains(&node) && sim_nodes.connected(&NodeLabel::node(node)) {
                connected.insert(node);
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let expected = HashSet::from_iter(0..num_nodes);
    let mut missing: Vec<_> = expected
        .difference(&connected)
        .map(|n| format!("node-{}", n))
        .collect();

    let node_connectivity = sim_nodes.node_connectivity();
    let connections = pretty_print_connections(&node_connectivity);
    tracing::info!("{connections}");

    if !missing.is_empty() {
        missing.sort();
        tracing::error!("Nodes without connection: {:?}", missing);
        tracing::error!("Total nodes without connection: {:?}", missing.len());
        anyhow::bail!("found disconnected nodes");
    }

    tracing::info!(
        "Required time for connecting all peers: {} secs",
        elapsed.elapsed().as_secs()
    );

    let hist: Vec<_> = sim_nodes.ring_distribution(1).collect();
    tracing::info!("Ring distribution: {:?}", hist);

    let mut connections_per_peer: Vec<_> = node_connectivity
        .iter()
        .map(|(k, v)| (k, v.len()))
        .filter_map(|(k, v)| if !k.is_gateway() { Some(v) } else { None })
        .collect();

    // ensure at least some normal nodes have more than one connection
    connections_per_peer.sort_unstable_by_key(|num_conn| *num_conn);
    if *connections_per_peer.iter().last().unwrap() < 1 {
        anyhow::bail!("low connectivy; nodes didn't connect beyond the gateway");
    }

    // ensure the average number of connections per peer is above N
    let avg_connections: usize = connections_per_peer.iter().sum::<usize>() / num_nodes;
    tracing::info!("Average connections: {}", avg_connections);
    if avg_connections < 1 {
        anyhow::bail!("average number of connections is low");
    }
    Ok(())
}

fn pretty_print_connections(conns: &HashMap<NodeLabel, HashMap<NodeLabel, Distance>>) -> String {
    let mut connections = String::from("Node connections:\n");
    let mut conns = conns.iter().collect::<Vec<_>>();
    conns.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (peer, conns) in conns {
        if peer.is_gateway() {
            continue;
        }
        writeln!(&mut connections, "{peer}:").unwrap();
        for (conn, dist) in conns {
            let dist = dist.as_f64();
            writeln!(&mut connections, "    {conn} (dist: {dist:.3})").unwrap();
        }
    }
    connections
}

#[ignore]
#[test]
fn group_locations_test() -> Result<(), anyhow::Error> {
    let locations = vec![0.5356, 0.5435, 0.5468, 0.5597, 0.6745, 0.7309, 0.7412];

    let mut grouped: Vec<_> = group_locations_in_buckets(locations.clone(), 1).collect();
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
