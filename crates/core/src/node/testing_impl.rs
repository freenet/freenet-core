use std::{
    collections::{HashMap, HashSet},
    fmt::Write,
    net::Ipv6Addr,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use either::Either;
use freenet_stdlib::prelude::*;
use futures::Future;
use itertools::Itertools;
use rand::seq::SliceRandom;
use tokio::sync::{mpsc, watch};
use tracing::{info, Instrument};

#[cfg(feature = "trace-ot")]
use crate::tracing::CombinedRegister;
use crate::{
    client_events::test::{MemoryEventsGen, RandomEventGenerator},
    config::{ConfigArgs, GlobalExecutor},
    contract::{
        self, ContractHandlerChannel, ExecutorToEventLoopChannel, NetworkEventListenerHalve,
        WaitingResolution,
    },
    message::{MessageStats, NetMessage, NetMessageV1, NodeEvent, Transaction},
    node::{InitPeerNode, NetEventRegister, NodeConfig},
    operations::connect,
    ring::{Distance, Location, PeerKeyLocation},
    tracing::TestEventListener,
};

mod in_memory;
mod network;

pub use self::network::{NetworkPeer, PeerMessage, PeerStatus};

use super::{
    network_bridge::EventLoopNotificationsReceiver, ConnectionError, NetworkBridge, PeerId,
};

pub(crate) type EventId = u32;

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
    pub owned_contracts: Vec<(ContractContainer, WrappedState, bool)>,
    pub events_to_generate: HashMap<EventId, freenet_stdlib::client_api::ClientRequest<'static>>,
    pub contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
}

#[derive(Clone)]
struct GatewayConfig {
    label: NodeLabel,
    id: PeerId,
    location: Location,
}

pub struct EventChain<S = watch::Sender<(EventId, PeerId)>> {
    labels: Vec<(NodeLabel, PeerId)>,
    // user_ev_controller: Sender<(EventId, PeerId)>,
    user_ev_controller: S,
    total_events: u32,
    count: u32,
    rng: rand::rngs::SmallRng,
    clean_up_tmp_dirs: bool,
    choice: Option<PeerId>,
}

impl<S> EventChain<S> {
    pub fn new(
        labels: Vec<(NodeLabel, PeerId)>,
        user_ev_controller: S,
        total_events: u32,
        clean_up_tmp_dirs: bool,
    ) -> Self {
        const SEED: u64 = 0xdeadbeef;
        EventChain {
            labels,
            user_ev_controller,
            total_events,
            count: 0,
            rng: rand::rngs::SmallRng::seed_from_u64(SEED),
            clean_up_tmp_dirs,
            choice: None,
        }
    }

    fn increment_count(self: Pin<&mut Self>) {
        unsafe {
            // This is safe because we're not moving the EventChain, just modifying a field
            let this = self.get_unchecked_mut();
            this.count += 1;
        }
    }

    fn choose_peer(self: Pin<&mut Self>) -> PeerId {
        let this = unsafe {
            // This is safe because we're not moving the EventChain, just copying one inner valur
            self.get_unchecked_mut()
        };
        if let Some(id) = this.choice.take() {
            return id;
        }
        let rng = &mut this.rng;
        let labels = &mut this.labels;
        let (_, id) = labels.choose(rng).expect("not empty");
        id.clone()
    }

    fn set_choice(self: Pin<&mut Self>, id: PeerId) {
        let this = unsafe {
            // This is safe because we're not moving the EventChain, just copying one inner valur
            self.get_unchecked_mut()
        };
        this.choice = Some(id);
    }
}

trait EventSender {
    fn send(
        &self,
        cx: &mut std::task::Context<'_>,
        value: (EventId, PeerId),
    ) -> std::task::Poll<Result<(), ()>>;
}

impl EventSender for mpsc::Sender<(EventId, PeerId)> {
    fn send(
        &self,
        cx: &mut std::task::Context<'_>,
        value: (EventId, PeerId),
    ) -> std::task::Poll<Result<(), ()>> {
        let f = self.send(value);
        futures::pin_mut!(f);
        f.poll(cx).map(|r| r.map_err(|_| ()))
    }
}

impl EventSender for watch::Sender<(EventId, PeerId)> {
    fn send(
        &self,
        _cx: &mut std::task::Context<'_>,
        value: (EventId, PeerId),
    ) -> std::task::Poll<Result<(), ()>> {
        match self.send(value) {
            Ok(_) => std::task::Poll::Ready(Ok(())),
            Err(_) => std::task::Poll::Ready(Err(())),
        }
    }
}

impl<S: EventSender> futures::stream::Stream for EventChain<S> {
    type Item = EventId;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.count < self.total_events {
            let id = self.as_mut().choose_peer();
            match self
                .user_ev_controller
                .send(cx, (self.count, id.clone()))
                .map_err(|_| {
                    tracing::error!("peer controller should be alive, finishing event chain")
                }) {
                std::task::Poll::Ready(_) => {}
                std::task::Poll::Pending => {
                    self.as_mut().set_choice(id);
                    return std::task::Poll::Pending;
                }
            }
            self.as_mut().increment_count();
            std::task::Poll::Ready(Some(self.count))
        } else {
            std::task::Poll::Ready(None)
        }
    }
}

impl<S> Drop for EventChain<S> {
    fn drop(&mut self) {
        if self.clean_up_tmp_dirs {
            clean_up_tmp_dirs(&self.labels)
        }
    }
}

#[cfg(feature = "trace-ot")]
type DefaultRegistry = CombinedRegister<2>;

#[cfg(not(feature = "trace-ot"))]
type DefaultRegistry = TestEventListener;

pub(super) struct Builder<ER> {
    pub(super) peer_key: PeerId,
    config: NodeConfig,
    contract_handler_name: String,
    add_noise: bool,
    event_register: ER,
    contracts: Vec<(ContractContainer, WrappedState, bool)>,
    contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
}

impl<ER: NetEventRegister> Builder<ER> {
    /// Buils an in-memory node. Does nothing upon construction,
    pub fn build(
        builder: NodeConfig,
        event_register: ER,
        contract_handler_name: String,
        add_noise: bool,
    ) -> Builder<ER> {
        let peer_key = builder.get_peer_id().unwrap();
        Builder {
            peer_key,
            config: builder.clone(),
            contract_handler_name,
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
    clean_up_tmp_dirs: bool,
    labels: Vec<(NodeLabel, PeerId)>,
    pub(crate) event_listener: TestEventListener,
    user_ev_controller: Option<watch::Sender<(EventId, PeerId)>>,
    receiver_ch: watch::Receiver<(EventId, PeerId)>,
    number_of_gateways: usize,
    gateways: Vec<(Builder<DefaultRegistry>, GatewayConfig)>,
    number_of_nodes: usize,
    nodes: Vec<(Builder<DefaultRegistry>, NodeLabel)>,
    ring_max_htl: usize,
    rnd_if_htl_above: usize,
    max_connections: usize,
    min_connections: usize,
    start_backoff: Duration,
    add_noise: bool,
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
        assert!(nodes > 0);
        let (user_ev_controller, mut receiver_ch) = watch::channel((0, PeerId::random()));
        receiver_ch.borrow_and_update();
        let mut net = Self {
            name: name.into(),
            clean_up_tmp_dirs: true,
            event_listener: TestEventListener::new().await,
            labels: Vec::with_capacity(nodes + gateways),
            user_ev_controller: Some(user_ev_controller),
            receiver_ch,
            number_of_gateways: gateways,
            gateways: Vec::with_capacity(gateways),
            number_of_nodes: nodes,
            nodes: Vec::with_capacity(nodes),
            ring_max_htl,
            rnd_if_htl_above,
            max_connections,
            min_connections,
            start_backoff: Duration::from_millis(1),
            add_noise: false,
        };
        net.config_gateways(
            gateways
                .try_into()
                .expect("should have at least one gateway"),
        )
        .await;
        net.config_nodes(nodes).await;
        net
    }
}

impl SimNetwork {
    pub fn with_start_backoff(&mut self, value: Duration) {
        self.start_backoff = value;
    }

    /// Simulates network random behaviour, like messages arriving delayed or out of order, throttling etc.
    #[allow(unused)]
    pub fn with_noise(&mut self) {
        self.add_noise = true;
    }

    #[allow(unused)]
    pub fn debug(&mut self) {
        self.clean_up_tmp_dirs = false;
    }

    async fn config_gateways(&mut self, num: NonZeroUsize) {
        info!("Building {} gateways", num);
        let mut configs = Vec::with_capacity(num.into());
        for node_no in 0..num.into() {
            let label = NodeLabel::gateway(node_no);
            let port = crate::util::get_free_port().unwrap();
            let keypair = crate::transport::TransportKeypair::new();
            let id = PeerId::new((Ipv6Addr::LOCALHOST, port).into(), keypair.public().clone());
            let location = Location::random();

            let mut config = NodeConfig::new(ConfigArgs::default().build().unwrap());
            config
                .with_ip(Ipv6Addr::LOCALHOST)
                .with_port(port)
                .with_location(location)
                .max_hops_to_live(self.ring_max_htl)
                .max_number_of_connections(self.max_connections)
                .min_number_of_connections(self.min_connections)
                .is_gateway()
                .rnd_if_htl_above(self.rnd_if_htl_above);

            self.event_listener.add_node(label.clone(), id.clone());
            configs.push((
                config,
                GatewayConfig {
                    label,
                    id,
                    location,
                },
            ));
        }
        configs[0].0.should_connect = false;

        let gateways: Vec<_> = configs.iter().map(|(_, gw)| gw.clone()).collect();
        for (mut this_node, this_config) in configs {
            for GatewayConfig { id, location, .. } in gateways
                .iter()
                .filter(|config| this_config.label != config.label)
            {
                this_node.add_gateway(InitPeerNode::new(id.clone(), *location));
            }
            let event_listener = {
                #[cfg(feature = "trace-ot")]
                {
                    use crate::tracing::OTEventRegister;
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

    async fn config_nodes(&mut self, num: usize) {
        info!("Building {} regular nodes", num);
        let gateways: Vec<_> = self
            .gateways
            .iter()
            .map(|(_node, config)| config)
            .cloned()
            .collect();

        for node_no in self.number_of_gateways..num + self.number_of_gateways {
            let label = NodeLabel::node(node_no);
            let peer = PeerId::random();

            let mut config = NodeConfig::new(ConfigArgs::default().build().unwrap());
            for GatewayConfig { id, location, .. } in &gateways {
                config.add_gateway(InitPeerNode::new(id.clone(), *location));
            }
            config
                .max_hops_to_live(self.ring_max_htl)
                .rnd_if_htl_above(self.rnd_if_htl_above)
                .max_number_of_connections(self.max_connections)
                .with_ip(Ipv6Addr::LOCALHOST)
                .with_port(crate::util::get_free_port().unwrap());

            self.event_listener.add_node(label.clone(), peer);

            let event_listener = {
                #[cfg(feature = "trace-ot")]
                {
                    use crate::tracing::OTEventRegister;
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
            let mut user_events =
                MemoryEventsGen::new(self.receiver_ch.clone(), node.peer_key.clone());
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
            self.labels.push((label, node.peer_key.clone()));

            let node_task = async move { node.run_node(user_events, span).await };
            GlobalExecutor::spawn(node_task);

            tokio::time::sleep(self.start_backoff).await;
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
            let mut user_events = MemoryEventsGen::<R>::new_with_seed(
                self.receiver_ch.clone(),
                node.peer_key.clone(),
                seed,
            );
            user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);
            let span = if label.is_gateway() {
                tracing::info_span!("in_mem_gateway", %node.peer_key)
            } else {
                tracing::info_span!("in_mem_node", %node.peer_key)
            };
            self.labels.push((label, node.peer_key.clone()));

            let node_task = async move { node.run_node(user_events, span).await };
            let handle = GlobalExecutor::spawn(node_task);
            peers.push(handle);

            tokio::time::sleep(self.start_backoff).await;
        }
        self.labels.sort_by(|(a, _), (b, _)| a.cmp(b));
        peers
    }

    /// Builds peer nodes and returns the controller to trigger events.
    pub fn build_peers(&mut self) -> Vec<(NodeLabel, NodeConfig)> {
        let gw = self.gateways.drain(..).map(|(n, c)| (n, c.label));
        let mut peers = vec![];
        for (builder, label) in gw.chain(self.nodes.drain(..)).collect::<Vec<_>>() {
            self.labels.push((label.clone(), builder.peer_key));
            peers.push((label, builder.config));
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
                    peer: node.peer_key.clone(),
                    location: None,
                },
            );
        }
        for (node, config) in &self.gateways {
            locations_by_node.insert(
                config.label.clone(),
                PeerKeyLocation {
                    peer: node.peer_key.clone(),
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

    pub fn has_put_contract(&self, peer: impl Into<NodeLabel>, key: &ContractKey) -> bool {
        let peer = peer.into();
        let pos = self
            .labels
            .binary_search_by(|(label, _)| label.cmp(&peer))
            .expect("peer not found");
        self.event_listener
            .has_put_contract(&self.labels[pos].1, key)
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
            all_dists.push(self.event_listener.connections(key.clone()));
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
                .connections(key.clone())
                .map(|(k, d)| (key_to_label[&k].clone(), d))
                .collect::<HashMap<_, _>>();
            peers_connections.insert(label.clone(), (key.clone(), conns));
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
            .as_ref()
            .expect("should be set")
            .send((event_id, peer.clone()))
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
        total_events: u32,
        controller: Option<watch::Sender<(EventId, PeerId)>>,
    ) -> EventChain {
        let user_ev_controller = controller.unwrap_or_else(|| {
            self.user_ev_controller
                .take()
                .expect("controller should be ser")
        });
        let labels = std::mem::take(&mut self.labels);
        let debug_val = self.clean_up_tmp_dirs;
        self.clean_up_tmp_dirs = false; // set to false to avoid cleaning up the tmp dirs
        EventChain::new(labels, user_ev_controller, total_events, debug_val)
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

        let missing_percent = 1.0 - ((num_nodes - missing.len()) as f64 / num_nodes as f64);
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
    /// - the average number of connections per peer is above the mean between max and min connections
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
            .field("init_backoff", &self.start_backoff)
            .field("add_noise", &self.add_noise)
            .finish()
    }
}

impl Drop for SimNetwork {
    fn drop(&mut self) {
        if self.clean_up_tmp_dirs {
            clean_up_tmp_dirs(&self.labels);
        }
    }
}

fn clean_up_tmp_dirs(labels: &[(NodeLabel, PeerId)]) {
    for (label, _) in labels {
        let p = std::env::temp_dir().join(format!(
            "freenet-executor-{sim}-{label}",
            sim = "sim",
            label = label
        ));
        let _ = std::fs::remove_dir_all(p);
    }
}

fn group_locations_in_buckets(
    locs: impl IntoIterator<Item = f64>,
    scale: i32,
) -> impl Iterator<Item = (f64, usize)> {
    let mut distances = HashMap::new();
    for (bucket, group) in &locs
        .into_iter()
        .chunk_by(|l| (l * (10.0f64).powi(scale)).floor() as u32)
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
    fn recv(&mut self) -> impl Future<Output = Result<NetMessage, ConnectionError>> + Send;
}

struct RunnerConfig<NB, UsrEv>
where
    NB: NetworkBridge,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    peer_key: PeerId,
    parent_span: Option<tracing::Span>,
    op_manager: Arc<OpManager>,
    conn_manager: NB,
    /// Set on creation, taken on run
    user_events: Option<UsrEv>,
    notification_channel: EventLoopNotificationsReceiver,
    event_register: Box<dyn NetEventRegister>,
    gateways: Vec<PeerKeyLocation>,
    executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
}

async fn run_node<NB, UsrEv>(mut config: RunnerConfig<NB, UsrEv>) -> Result<(), anyhow::Error>
where
    NB: NetworkBridge + NetworkBridgeExt,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    connect::initial_join_procedure(
        config.op_manager.clone(),
        config.conn_manager.clone(),
        config.peer_key.pub_key.clone(),
        &config.gateways,
    )
    .await?;
    let (client_responses, cli_response_sender) = contract::client_responses_channel();
    let span = {
        config
            .parent_span
            .clone()
            .map(|parent_span| {
                tracing::info_span!(
                    parent: parent_span,
                    "client_event_handling",
                    peer = %config.peer_key.clone()
                )
            })
            .unwrap_or_else(
                || tracing::info_span!("client_event_handling", peer = %config.peer_key),
            )
    };
    let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
    GlobalExecutor::spawn(
        super::client_event_handling(
            config.op_manager.clone(),
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
    cli_response_sender: contract::ClientResponsesSender,
    mut node_controller_rx: tokio::sync::mpsc::Receiver<NodeEvent>,
    RunnerConfig {
        peer_key,
        gateways,
        parent_span,
        op_manager,
        mut conn_manager,
        mut notification_channel,
        mut event_register,
        mut executor_listener,
        client_wait_for_transaction: mut wait_for_event,
        ..
    }: RunnerConfig<NB, UsrEv>,
) -> Result<(), anyhow::Error>
where
    NB: NetworkBridge + NetworkBridgeExt,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    // todo: this two containers need to be clean up on transaction time-out
    let mut pending_from_executor = HashSet::new();
    let mut tx_to_client: HashMap<Transaction, crate::client_events::ClientId> = HashMap::new();
    loop {
        let msg = tokio::select! {
            msg = conn_manager.recv() => { msg.map(Either::Left) }
            msg = notification_channel.recv() => {
                if let Some(msg) = msg {
                    Ok(msg)
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
            event_id = wait_for_event.relay_transaction_result_to_client() => {
                if let Ok((client_id, transaction)) = event_id {
                   tx_to_client.insert(transaction, client_id);
                }
                continue;
            }
            id = executor_listener.transaction_from_executor() => {
                if let Ok(res) = id {
                    pending_from_executor.insert(res);
                }
                continue;
            }
        };

        if let Ok(Either::Left(NetMessage::V1(NetMessageV1::Aborted(tx)))) = msg {
            super::handle_aborted_op(
                tx,
                peer_key.pub_key.clone(),
                &op_manager,
                &mut conn_manager,
                &gateways,
            )
            .await?;
        }

        let msg = match msg {
            Ok(Either::Left(msg)) => msg,
            Ok(Either::Right(action)) => match action {
                NodeEvent::ShutdownNode => break Ok(()),
                NodeEvent::DropConnection(peer) => {
                    tracing::info!("Dropping connection to {peer}");
                    event_register
                        .register_events(Either::Left(crate::tracing::NetEventLog::disconnected(
                            &op_manager.ring,
                            &peer,
                        )))
                        .await;
                    op_manager.ring.prune_connection(peer).await;
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
            },
            Err(err) => {
                super::report_result(
                    None,
                    Err(err.into()),
                    &op_manager,
                    None,
                    None,
                    &mut *event_register as &mut _,
                )
                .await;
                continue;
            }
        };

        let op_manager = op_manager.clone();
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

        let executor_callback = pending_from_executor
            .remove(msg.id())
            .then(|| executor_listener.callback());
        let pending_client_req = tx_to_client.get(msg.id()).copied();
        let client_req_handler_callback = if pending_client_req.is_some() {
            Some(cli_response_sender.clone())
        } else {
            None
        };

        let msg = super::process_message(
            msg,
            op_manager,
            conn_manager.clone(),
            event_listener,
            executor_callback,
            client_req_handler_callback,
            pending_client_req,
        )
        .instrument(span);
        GlobalExecutor::spawn(msg);
    }
}
