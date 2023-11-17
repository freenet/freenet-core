use std::{
    collections::{HashMap, HashSet},
    fmt::Write,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
    sync::Arc,
    time::{Duration, Instant},
};

use either::Either;
use freenet_stdlib::prelude::*;
use futures::future::BoxFuture;
use itertools::Itertools;
use libp2p::{identity, PeerId as Libp2pPeerId};
use rand::{seq::SliceRandom, Rng};
use tokio::sync::watch::{channel, Receiver, Sender};
use tracing::{info, Instrument};

#[cfg(feature = "trace-ot")]
use crate::node::network_event_log::CombinedRegister;
use crate::{
    client_events::test::{MemoryEventsGen, RandomEventGenerator},
    config::GlobalExecutor,
    contract,
    message::{NetMessage, NodeEvent},
    node::{network_event_log::TestEventListener, InitPeerNode, NetEventRegister, NodeConfig},
    ring::{Distance, Location, PeerKeyLocation},
};

mod in_memory;
mod inter_process;

use self::inter_process::SimPeer;

use super::{network_bridge::EventLoopNotifications, ConnectionError, NetworkBridge, PeerId};

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

#[derive(PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub struct NodeLabel(Arc<str>);

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

    pub fn is_node(&self) -> bool {
        self.0.starts_with("node")
    }

    pub fn number(&self) -> usize {
        let mut parts = self.0.split('-');
        assert!(parts.next().is_some());
        parts
            .next()
            .map(|s| s.parse::<usize>())
            .transpose()
            .expect("should be an usize")
            .expect("should have an other part")
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

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct NodeSpecification {
    pub owned_contracts: Vec<(ContractContainer, WrappedState)>,
    pub events_to_generate: HashMap<EventId, freenet_stdlib::client_api::ClientRequest<'static>>,
    pub contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
}

#[derive(Clone)]
struct GatewayConfig {
    label: NodeLabel,
    port: u16,
    id: Libp2pPeerId,
    location: Location,
}

pub struct EventChain {
    network: SimNetwork,
    total_events: usize,
    count: usize,
    rng: rand::rngs::SmallRng,
}

impl Iterator for EventChain {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        (self.count < self.total_events).then(|| {
            let (_, id) = self
                .network
                .labels
                .choose(&mut self.rng)
                .expect("not empty");
            self.network
                .user_ev_controller
                .send((self.count, *id))
                .expect("peer controller should be alive");
            self.count += 1;
            self.count
        })
    }
}

#[cfg(feature = "trace-ot")]
type DefaultRegistry = CombinedRegister<2>;

#[cfg(not(feature = "trace-ot"))]
type DefaultRegistry = TestEventListener;

pub(super) struct Builder<ER> {
    pub(super) peer_key: PeerId,
    config: NodeConfig,
    ch_builder: String,
    add_noise: bool,
    event_register: ER,
    contracts: Vec<(ContractContainer, WrappedState)>,
    contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
}

impl<ER: NetEventRegister> Builder<ER> {
    /// Buils an in-memory node. Does nothing upon construction,
    pub fn build(
        builder: NodeConfig,
        event_register: ER,
        ch_builder: String,
        add_noise: bool,
    ) -> Builder<ER> {
        let peer_key = builder.peer_id;
        Builder {
            peer_key,
            config: builder,
            ch_builder,
            add_noise,
            event_register,
            contracts: Vec::new(),
            contract_subscribers: HashMap::new(),
        }
    }
}

/// A simulated in-memory network topology.
pub struct SimNetwork {
    name: String,
    debug: bool,
    labels: Vec<(NodeLabel, PeerId)>,
    pub(crate) event_listener: TestEventListener,
    user_ev_controller: Sender<(EventId, PeerId)>,
    receiver_ch: Receiver<(EventId, PeerId)>,
    number_of_gateways: usize,
    gateways: Vec<(Builder<DefaultRegistry>, GatewayConfig)>,
    number_of_nodes: usize,
    nodes: Vec<(Builder<DefaultRegistry>, NodeLabel)>,
    ring_max_htl: usize,
    rnd_if_htl_above: usize,
    max_connections: usize,
    min_connections: usize,
    init_backoff: Duration,
    add_noise: bool,
}

#[cfg(any(debug_assertions, test))]
impl std::fmt::Debug for SimNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimNetwork")
            .field("name", &self.name)
            .field("labels", &self.labels)
            .field("number_of_gateways", &self.number_of_gateways)
            .field("number_of_nodes", &self.number_of_nodes)
            .field("ring_max_htl", &self.ring_max_htl)
            .field("rnd_if_htl_above", &self.rnd_if_htl_above)
            .field("max_connections", &self.max_connections)
            .field("min_connections", &self.min_connections)
            .field("init_backoff", &self.init_backoff)
            .field("add_noise", &self.add_noise)
            .finish()
    }
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
        let (user_ev_controller, receiver_ch) = channel((0, PeerId::random()));
        let mut net = Self {
            name: name.into(),
            debug: false,
            event_listener: TestEventListener::new(),
            labels: Vec::with_capacity(nodes + gateways),
            user_ev_controller,
            receiver_ch,
            number_of_gateways: gateways,
            gateways: Vec::with_capacity(gateways),
            number_of_nodes: nodes,
            nodes: Vec::with_capacity(nodes),
            ring_max_htl,
            rnd_if_htl_above,
            max_connections,
            min_connections,
            init_backoff: Duration::from_millis(1),
            add_noise: false,
        };
        net.build_gateways(gateways).await;
        net.build_nodes(nodes).await;
        net
    }
}

impl SimNetwork {
    pub fn with_start_backoff(&mut self, value: Duration) {
        self.init_backoff = value;
    }

    /// Simulates network random behaviour, like messages arriving delayed or out of order, throttling etc.
    #[allow(unused)]
    pub fn with_noise(&mut self) {
        self.add_noise = true;
    }

    #[allow(unused)]
    pub fn debug(&mut self) {
        self.debug = true;
    }

    async fn build_gateways(&mut self, num: usize) {
        info!("Building {} gateways", num);
        let mut configs = Vec::with_capacity(num);
        for node_no in 0..num {
            let label = NodeLabel::gateway(node_no);
            let pair = identity::Keypair::generate_ed25519();
            let id = pair.public().to_peer_id();
            let port = get_free_port().unwrap();
            let location = Location::random();

            let mut config = NodeConfig::new();
            config
                .with_ip(Ipv6Addr::LOCALHOST)
                .with_port(port)
                .with_key(pair.public().into())
                .with_location(location)
                .max_hops_to_live(self.ring_max_htl)
                .max_number_of_connections(self.max_connections)
                .min_number_of_connections(self.min_connections)
                .rnd_if_htl_above(self.rnd_if_htl_above);

            self.event_listener
                .add_node(label.clone(), PeerId::from(id));
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
            let event_listener = {
                #[cfg(feature = "trace-ot")]
                {
                    use super::network_event_log::OTEventRegister;
                    CombinedRegister::new([
                        self.event_listener.trait_clone(),
                        Box::new(OTEventRegister::new()),
                    ])
                }
                #[cfg(not(feature = "trace-ot"))]
                {
                    self.event_listener.clone()
                }
            };
            let gateway = Builder::build(
                this_node,
                event_listener,
                format!("{}-{label}", self.name, label = this_config.label),
                self.add_noise,
            );
            self.gateways.push((gateway, this_config));
        }
    }

    async fn build_nodes(&mut self, num: usize) {
        info!("Building {} regular nodes", num);
        let gateways: Vec<_> = self
            .gateways
            .iter()
            .map(|(_node, config)| config)
            .cloned()
            .collect();

        for node_no in self.number_of_gateways..num + self.number_of_gateways {
            let label = NodeLabel::node(node_no);
            let pair = identity::Keypair::generate_ed25519();
            let id = pair.public().to_peer_id();

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
                .with_key(pair.public().into());

            let peer = PeerId::from(id);
            self.event_listener.add_node(label.clone(), peer);

            let event_listener = {
                #[cfg(feature = "trace-ot")]
                {
                    use super::network_event_log::OTEventRegister;
                    CombinedRegister::new([
                        self.event_listener.trait_clone(),
                        Box::new(OTEventRegister::new()),
                    ])
                }
                #[cfg(not(feature = "trace-ot"))]
                {
                    self.event_listener.clone()
                }
            };
            let node = Builder::build(
                config,
                event_listener,
                format!("{}-{label}", self.name),
                self.add_noise,
            );
            self.nodes.push((node, label));
        }
    }

    #[cfg(test)]
    pub(crate) async fn start(&mut self) {
        self.start_with_spec(HashMap::new()).await
    }

    #[cfg(test)]
    pub(crate) async fn start_with_spec(
        &mut self,
        mut specs: HashMap<NodeLabel, NodeSpecification>,
    ) {
        let gw = self.gateways.drain(..).map(|(n, c)| (n, c.label));
        for (mut node, label) in gw.chain(self.nodes.drain(..)).collect::<Vec<_>>() {
            tracing::debug!(peer = %label, "initializing");
            let node_spec = specs.remove(&label);
            let mut user_events = MemoryEventsGen::new(self.receiver_ch.clone(), node.peer_key);
            if let Some(specs) = node_spec.clone() {
                user_events.generate_events(specs.events_to_generate);
            }
            let span = if label.is_gateway() {
                tracing::info_span!("in_mem_gateway", %node.peer_key)
            } else {
                tracing::info_span!("in_mem_node", %node.peer_key)
            };
            if let Some(specs) = node_spec {
                node.append_contracts(specs.owned_contracts, specs.contract_subscribers);
            }
            self.labels.push((label, node.peer_key));

            let node_task = async move { node.run_node(user_events, span).await };
            GlobalExecutor::spawn(node_task);

            tokio::time::sleep(self.init_backoff).await;
        }
        self.labels.sort_by(|(a, _), (b, _)| a.cmp(b));
    }

    pub async fn start_with_rand_gen<R>(
        &mut self,
        seed: u64,
        max_contract_num: usize,
        iterations: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>>
    where
        R: RandomEventGenerator + Send + 'static,
    {
        let total_peer_num = self.gateways.len() + self.nodes.len();
        let gw = self.gateways.drain(..).map(|(n, c)| (n, c.label));
        let mut peers = vec![];
        for (node, label) in gw.chain(self.nodes.drain(..)).collect::<Vec<_>>() {
            tracing::debug!(peer = %label, "initializing");
            let mut user_events =
                MemoryEventsGen::<R>::new_with_seed(self.receiver_ch.clone(), node.peer_key, seed);
            user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);
            let span = if label.is_gateway() {
                tracing::info_span!("in_mem_gateway", %node.peer_key)
            } else {
                tracing::info_span!("in_mem_node", %node.peer_key)
            };
            self.labels.push((label, node.peer_key));

            let node_task = async move { node.run_node(user_events, span).await };
            let handle = GlobalExecutor::spawn(node_task);
            peers.push(handle);

            tokio::time::sleep(self.init_backoff).await;
        }
        self.labels.sort_by(|(a, _), (b, _)| a.cmp(b));
        peers
    }

    /// Builds peer nodes and returns the controller to trigger events.
    pub fn build_peers(&mut self) -> Vec<(NodeLabel, SimPeer)> {
        let gw = self.gateways.drain(..).map(|(n, c)| (n, c.label));
        let mut peers = vec![];
        for (builder, label) in gw.chain(self.nodes.drain(..)).collect::<Vec<_>>() {
            self.labels.push((label.clone(), builder.peer_key));
            peers.push((
                label,
                SimPeer {
                    config: builder.config,
                },
            ));
        }
        self.labels.sort_by(|(a, _), (b, _)| a.cmp(b));
        peers.sort_by(|(a, _), (b, _)| a.cmp(b));
        peers
    }

    pub fn get_locations_by_node(&self) -> HashMap<NodeLabel, PeerKeyLocation> {
        let mut locations_by_node: HashMap<NodeLabel, PeerKeyLocation> = HashMap::new();

        // Get node and gateways location by label
        for (node, label) in &self.nodes {
            locations_by_node.insert(
                label.clone(),
                PeerKeyLocation {
                    peer: node.peer_key,
                    location: None,
                },
            );
        }
        for (node, config) in &self.gateways {
            locations_by_node.insert(
                config.label.clone(),
                PeerKeyLocation {
                    peer: node.peer_key,
                    location: config.location.into(),
                },
            );
        }
        locations_by_node
    }

    pub fn connected(&self, peer: &NodeLabel) -> bool {
        let pos = self
            .labels
            .binary_search_by(|(label, _)| label.cmp(peer))
            .expect("peer not found");
        self.event_listener.is_connected(&self.labels[pos].1)
    }

    pub fn has_put_contract(
        &self,
        peer: impl Into<NodeLabel>,
        key: &ContractKey,
        value: &WrappedState,
    ) -> bool {
        let peer = peer.into();
        let pos = self
            .labels
            .binary_search_by(|(label, _)| label.cmp(&peer))
            .expect("peer not found");
        self.event_listener
            .has_put_contract(&self.labels[pos].1, key, value)
    }

    pub fn has_got_contract(&self, peer: impl Into<NodeLabel>, key: &ContractKey) -> bool {
        let peer = peer.into();
        let pos = self
            .labels
            .binary_search_by(|(label, _)| label.cmp(&peer))
            .expect("peer not found");
        self.event_listener
            .has_got_contract(&self.labels[pos].1, key)
    }

    pub fn is_subscribed_to_contract(&self, peer: impl Into<NodeLabel>, key: &ContractKey) -> bool {
        let peer = peer.into();
        let pos = self
            .labels
            .binary_search_by(|(label, _)| label.cmp(&peer))
            .expect("peer not found");
        self.event_listener
            .is_subscribed_to_contract(&self.labels[pos].1, key)
    }

    /// Builds an histogram of the distribution in the ring of each node relative to each other.
    pub fn ring_distribution(&self, scale: i32) -> Vec<(f64, usize)> {
        let mut all_dists = Vec::with_capacity(self.labels.len());
        for (.., key) in &self.labels {
            all_dists.push(self.event_listener.connections(*key));
        }
        let mut dist_buckets = group_locations_in_buckets(
            all_dists.into_iter().flatten().map(|(_, l)| l.as_f64()),
            scale,
        )
        .collect::<Vec<_>>();
        dist_buckets
            .sort_by(|(d0, _), (d1, _)| d0.partial_cmp(d1).unwrap_or(std::cmp::Ordering::Equal));
        dist_buckets
    }

    /// Returns the connectivity in the network per peer (that is all the connections
    /// this peers has registered).
    pub fn node_connectivity(&self) -> HashMap<NodeLabel, (PeerId, HashMap<NodeLabel, Distance>)> {
        let mut peers_connections = HashMap::with_capacity(self.labels.len());
        let key_to_label: HashMap<_, _> = self.labels.iter().map(|(k, v)| (v, k)).collect();
        for (label, key) in &self.labels {
            let conns = self
                .event_listener
                .connections(*key)
                .map(|(k, d)| (key_to_label[&k].clone(), d))
                .collect::<HashMap<_, _>>();
            peers_connections.insert(label.clone(), (*key, conns));
        }
        peers_connections
    }

    /// # Arguments
    ///
    /// - label: node for which to trigger the
    /// - event_id: which event to trigger
    /// - await_for: if set, wait for the duration before returning
    #[cfg(test)]
    pub(crate) async fn trigger_event(
        &self,
        label: impl Into<NodeLabel>,
        event_id: EventId,
        await_for: Option<Duration>,
    ) -> Result<(), anyhow::Error> {
        let label = label.into();
        let pos = self
            .labels
            .binary_search_by(|(other, _)| other.cmp(&label))
            .map_err(|_| anyhow::anyhow!("peer not found"))?;
        let (_, peer) = &self.labels[pos];
        self.user_ev_controller
            .send((event_id, *peer))
            .expect("node listeners disconnected");
        if let Some(sleep_time) = await_for {
            tokio::time::sleep(sleep_time).await;
        }
        Ok(())
    }

    /// Start an event chain for this simulation. Allows passing a different controller for the peers.
    ///
    /// If done make sure you set the proper receiving side for the controller. For example in the
    /// nodes built through the [`build_peers`](`Self::build_peers`) method.
    pub fn event_chain(
        mut self,
        total_events: usize,
        controller: Option<Sender<(EventId, PeerId)>>,
    ) -> EventChain {
        const SEED: u64 = 0xdeadbeef;
        if let Some(controller) = controller {
            self.user_ev_controller = controller;
        }
        EventChain {
            network: self,
            total_events,
            count: 0,
            rng: rand::rngs::SmallRng::seed_from_u64(SEED),
        }
    }

    /// Checks that all peers in the network have acquired at least one connection to any
    /// other peers.
    pub fn check_connectivity(&self, time_out: Duration) -> Result<(), anyhow::Error> {
        self.connectivity(time_out, 1.0)
    }

    /// Checks that a percentage (given as a float between 0 and 1) of the nodes has at least
    /// one connection to any other peers.
    pub fn check_partial_connectivity(
        &self,
        time_out: Duration,
        percent: f64,
    ) -> Result<(), anyhow::Error> {
        self.connectivity(time_out, percent)
    }

    fn connectivity(&self, time_out: Duration, percent: f64) -> Result<(), anyhow::Error> {
        let num_nodes = self.number_of_nodes;
        let mut connected = HashSet::new();
        let elapsed = Instant::now();
        while elapsed.elapsed() < time_out && (connected.len() as f64 / num_nodes as f64) < percent
        {
            for node in self.number_of_gateways..num_nodes + self.number_of_gateways {
                if !connected.contains(&node) && self.connected(&NodeLabel::node(node)) {
                    connected.insert(node);
                }
            }
        }

        let expected =
            HashSet::from_iter(self.number_of_gateways..num_nodes + self.number_of_gateways);
        let mut missing: Vec<_> = expected
            .difference(&connected)
            .map(|n| format!("node-{}", n))
            .collect();

        tracing::info!("Number of simulated nodes: {num_nodes}");

        let missing_percent = (num_nodes - missing.len()) as f64 / num_nodes as f64;
        if missing_percent > (percent + 0.01/* 1% error tolerance */) {
            missing.sort();
            let show_max = missing.len().min(100);
            tracing::error!("Nodes without connection: {:?}(..)", &missing[..show_max],);
            tracing::error!(
                "Total nodes without connection: {:?},  ({}% > {}%)",
                missing.len(),
                missing_percent * 100.0,
                percent * 100.0
            );
            anyhow::bail!("found disconnected nodes");
        }

        tracing::info!(
            "Required time for connecting all peers: {} secs",
            elapsed.elapsed().as_secs()
        );

        Ok(())
    }

    pub fn print_network_connections(&self) {
        let node_connectivity = self.node_connectivity();
        let connections = pretty_print_connections(&node_connectivity);
        tracing::info!("{connections}");
    }

    pub fn print_ring_distribution(&self) {
        let hist = self.ring_distribution(1);
        tracing::info!("Ring distribution: {:?}", hist);
    }

    /// Recommended to calling after `check_connectivity` to ensure enough time
    /// elapsed for all peers to become connected.
    ///
    /// Checks that there is a good connectivity over the simulated network,
    /// meaning that:
    ///
    /// - at least 50% of the peers have more than the minimum connections
    /// -
    pub fn network_connectivity_quality(&self) -> Result<(), anyhow::Error> {
        const HIGHER_THAN_MIN_THRESHOLD: f64 = 0.5;
        let num_nodes = self.number_of_nodes;
        let min_connections_threshold = (num_nodes as f64 * HIGHER_THAN_MIN_THRESHOLD) as usize;
        let node_connectivity = self.node_connectivity();

        let mut connections_per_peer: Vec<_> = node_connectivity
            .iter()
            .map(|(k, v)| (k, v.1.len()))
            .filter(|&(k, _)| !k.is_gateway())
            .map(|(_, v)| v)
            .collect();

        // ensure at least "most" normal nodes have more than one connection
        connections_per_peer.sort_unstable_by_key(|num_conn| *num_conn);
        if connections_per_peer[min_connections_threshold] < self.min_connections {
            tracing::error!(
                "Low connectivity; more than {:.0}% of the nodes don't have more than minimum connections",
                HIGHER_THAN_MIN_THRESHOLD * 100.0
            );
            anyhow::bail!("low connectivity");
        } else {
            let idx = connections_per_peer[min_connections_threshold..]
                .iter()
                .position(|num_conn| *num_conn < self.min_connections)
                .unwrap_or_else(|| connections_per_peer[min_connections_threshold..].len() - 1)
                + (min_connections_threshold - 1);
            let percentile = idx as f64 / connections_per_peer.len() as f64 * 100.0;
            tracing::info!("{percentile:.0}% nodes have higher than required minimum connections");
        }

        // ensure the average number of connections per peer is above the mean between max and min connections
        let expected_avg_connections =
            ((self.max_connections - self.min_connections) / 2) + self.min_connections;
        let avg_connections: usize = connections_per_peer.iter().sum::<usize>() / num_nodes;
        if avg_connections < expected_avg_connections {
            tracing::warn!("Average number of connections ({avg_connections}) is low (< {expected_avg_connections})");
        }
        Ok(())
    }
}

impl Drop for SimNetwork {
    fn drop(&mut self) {
        if !self.debug {
            for (label, _) in &self.labels {
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

fn pretty_print_connections(
    conns: &HashMap<NodeLabel, (PeerId, HashMap<NodeLabel, Distance>)>,
) -> String {
    let mut connections = String::from("Node connections:\n");
    let mut conns = conns.iter().collect::<Vec<_>>();
    conns.sort_by(|(a, _), (b, _)| a.cmp(b));
    for (peer, (key, conns)) in conns {
        writeln!(&mut connections, "{peer} ({key}):").unwrap();
        for (conn, dist) in conns {
            let dist = dist.as_f64();
            writeln!(&mut connections, "    {conn} (dist: {dist:.3})").unwrap();
        }
    }
    connections
}

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

use super::op_state_manager::OpManager;
use crate::client_events::ClientEventsProxy;

pub(super) trait NetworkBridgeExt: Clone + 'static {
    fn recv(&mut self) -> BoxFuture<Result<NetMessage, ConnectionError>>;
}

struct RunnerConfig<NB, UsrEv>
where
    NB: NetworkBridge,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    peer_key: PeerId,
    is_gateway: bool,
    gateways: Vec<PeerKeyLocation>,
    parent_span: Option<tracing::Span>,
    op_storage: Arc<OpManager>,
    conn_manager: NB,
    /// Set on creation, taken on run
    user_events: Option<UsrEv>,
    notification_channel: EventLoopNotifications,
    event_register: Box<dyn NetEventRegister>,
}

async fn run_node<NB, UsrEv>(mut config: RunnerConfig<NB, UsrEv>) -> Result<(), anyhow::Error>
where
    NB: NetworkBridge + NetworkBridgeExt,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    use crate::util::IterExt;
    if !config.is_gateway {
        if let Some(gateway) = config.gateways.iter().shuffle().take(1).next() {
            super::join_ring_request(
                None,
                config.peer_key,
                gateway,
                &config.op_storage,
                &mut config.conn_manager,
            )
            .await?;
        } else {
            anyhow::bail!("requires at least one gateway");
        }
    }
    let (client_responses, cli_response_sender) = contract::ClientResponses::channel();
    let span = {
        config
            .parent_span
            .clone()
            .map(|parent_span| {
                tracing::info_span!(
                    parent: parent_span,
                    "client_event_handling",
                    peer = %config.peer_key
                )
            })
            .unwrap_or_else(
                || tracing::info_span!("client_event_handling", peer = %config.peer_key),
            )
    };
    let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
    GlobalExecutor::spawn(
        super::client_event_handling(
            config.op_storage.clone(),
            config.user_events.take().expect("should be set"),
            client_responses,
            node_controller_tx,
        )
        .instrument(span),
    );
    let parent_span: tracing::Span = config
        .parent_span
        .clone()
        .unwrap_or_else(|| tracing::info_span!("event_listener", peer = %config.peer_key));
    run_event_listener(cli_response_sender, node_controller_rx, config)
        .instrument(parent_span)
        .await
}

/// Starts listening to incoming events. Will attempt to join the ring if any gateways have been provided.
async fn run_event_listener<NB, UsrEv>(
    _client_responses: contract::ClientResponsesSender,
    mut node_controller_rx: tokio::sync::mpsc::Receiver<NodeEvent>,
    RunnerConfig {
        peer_key,
        is_gateway,
        gateways,
        parent_span,
        op_storage,
        mut conn_manager,
        mut notification_channel,
        mut event_register,
        ..
    }: RunnerConfig<NB, UsrEv>,
) -> Result<(), anyhow::Error>
where
    NB: NetworkBridge + NetworkBridgeExt,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    use crate::util::IterExt;
    loop {
        let msg = tokio::select! {
            msg = conn_manager.recv() => { msg.map(Either::Left) }
            msg = notification_channel.recv() => {
                if let Some(msg) = msg {
                    Ok(msg.map_left(|(msg, _cli_id)| msg))
                } else {
                    anyhow::bail!("notification channel shutdown, fatal error");
                }
            }
            msg = node_controller_rx.recv() => {
                if let Some(msg) = msg {
                    Ok(Either::Right(msg))
                } else {
                    anyhow::bail!("node controller channel shutdown, fatal error");
                }
            }
        };

        if let Ok(Either::Left(NetMessage::Aborted(tx))) = msg {
            let tx_type = tx.transaction_type();
            let res =
                super::handle_cancelled_op(tx, peer_key, &op_storage, &mut conn_manager).await;
            match res {
                Err(crate::operations::OpError::MaxRetriesExceeded(_, _))
                    if tx_type == crate::message::TransactionType::Connect && !is_gateway =>
                {
                    tracing::warn!("Retrying joining the ring with an other peer");
                    if let Some(gateway) = gateways.iter().shuffle().next() {
                        super::join_ring_request(
                            None,
                            peer_key,
                            gateway,
                            &op_storage,
                            &mut conn_manager,
                        )
                        .await?
                    } else {
                        anyhow::bail!("requires at least one gateway");
                    }
                }
                Err(err) => return Err(anyhow::anyhow!(err)),
                Ok(_) => {}
            }
            continue;
        }

        let msg = match msg {
            Ok(Either::Left(msg)) => msg,
            Ok(Either::Right(action)) => match action {
                NodeEvent::ShutdownNode => break Ok(()),
                NodeEvent::DropConnection(peer) => {
                    tracing::info!("Dropping connection to {peer}");
                    event_register.register_events(Either::Left(
                        crate::node::network_event_log::NetEventLog::disconnected(&peer),
                    ));
                    op_storage.ring.prune_connection(peer);
                    continue;
                }
                NodeEvent::Disconnect { cause: Some(cause) } => {
                    tracing::info!(peer = %peer_key, "Shutting down node, reason: {cause}");
                    return Ok(());
                }
                NodeEvent::Disconnect { cause: None } => {
                    tracing::info!(peer = %peer_key, "Shutting down node");
                    return Ok(());
                }
                other => {
                    unreachable!("event {other:?}, shouldn't happen in the in-memory impl")
                }
            },
            Err(err) => {
                super::report_result(
                    None,
                    Err(err.into()),
                    &op_storage,
                    None,
                    None,
                    &mut *event_register as &mut _,
                )
                .await;
                continue;
            }
        };

        let op_storage = op_storage.clone();
        let conn_manager = conn_manager.clone();
        let event_listener = event_register.trait_clone();

        let span = {
            parent_span
                .clone()
                .map(|parent_span| {
                    tracing::info_span!(
                        parent: parent_span.clone(),
                        "process_network_message",
                        peer = %peer_key, transaction = %msg.id(),
                        tx_type = %msg.id().transaction_type()
                    )
                })
                .unwrap_or_else(|| {
                    tracing::info_span!(
                        "process_network_message",
                        peer = %peer_key, transaction = %msg.id(),
                        tx_type = %msg.id().transaction_type()
                    )
                })
        };

        let msg = super::process_message(
            msg,
            op_storage,
            conn_manager,
            event_listener,
            None,
            None,
            None,
        )
        .instrument(span);
        GlobalExecutor::spawn(msg);
    }
}
