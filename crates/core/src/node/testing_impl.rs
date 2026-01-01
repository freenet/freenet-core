use either::Either;
use freenet_stdlib::prelude::*;
use futures::Future;
use rand::prelude::IndexedRandom;
use std::{
    collections::{HashMap, HashSet},
    net::{Ipv6Addr, SocketAddr},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{info, Instrument};

#[cfg(feature = "trace-ot")]
use crate::tracing::CombinedRegister;
use crate::{
    client_events::{
        client_event_handling,
        test::{MemoryEventsGen, RandomEventGenerator},
    },
    config::{ConfigArgs, GlobalExecutor},
    contract::{
        self, ContractHandlerChannel, ExecutorToEventLoopChannel, NetworkEventListenerHalve,
        WaitingResolution,
    },
    dev_tool::TransportKeypair,
    message::{MessageStats, NetMessage, NetMessageV1, NodeEvent, Transaction},
    node::{InitPeerNode, NetEventRegister, NodeConfig},
    operations::connect,
    ring::{Distance, Location, PeerKeyLocation},
    tracing::TestEventListener,
    transport::TransportPublicKey,
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
    fn gateway(network_name: &str, id: usize) -> Self {
        Self(format!("{network_name}-gateway-{id}").into())
    }

    fn node(network_name: &str, id: usize) -> Self {
        Self(format!("{network_name}-node-{id}").into())
    }

    fn is_gateway(&self) -> bool {
        self.0.contains("-gateway-")
    }

    pub fn is_node(&self) -> bool {
        self.0.contains("-node-")
    }

    pub fn number(&self) -> usize {
        // Label format is "{network_name}-{gateway|node}-{id}"
        // The number is always the last part after the final '-'
        self.0
            .rsplit('-')
            .next()
            .expect("should have a number part")
            .parse::<usize>()
            .expect("last part should be a number")
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
struct GatewayConfig {
    label: NodeLabel,
    peer_key_location: PeerKeyLocation,
    location: Location,
}

/// Summary of a network event for deterministic comparison.
///
/// Excludes timestamps which vary between runs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventSummary {
    pub tx: crate::message::Transaction,
    pub peer_addr: std::net::SocketAddr,
    /// String representation of the event kind for sorting
    pub event_kind_name: String,
    /// Contract key if this event involves a contract operation
    pub contract_key: Option<String>,
    /// State hash if this event includes state (Put/Update success/broadcast)
    pub state_hash: Option<String>,
    /// Full debug representation of the event (for backwards compatibility)
    pub event_detail: String,
}

pub struct EventChain<S = watch::Sender<(EventId, TransportPublicKey)>> {
    labels: Vec<(NodeLabel, TransportPublicKey)>,
    user_ev_controller: S,
    total_events: u32,
    count: u32,
    rng: rand::rngs::SmallRng,
    clean_up_tmp_dirs: bool,
    choice: Option<TransportPublicKey>,
}

impl<S> EventChain<S> {
    pub fn new(
        labels: Vec<(NodeLabel, TransportPublicKey)>,
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

    fn choose_peer(self: Pin<&mut Self>) -> TransportPublicKey {
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

    fn set_choice(self: Pin<&mut Self>, id: TransportPublicKey) {
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
        value: (EventId, TransportPublicKey),
    ) -> std::task::Poll<Result<(), ()>>;
}

impl EventSender for mpsc::Sender<(EventId, TransportPublicKey)> {
    fn send(
        &self,
        cx: &mut std::task::Context<'_>,
        value: (EventId, TransportPublicKey),
    ) -> std::task::Poll<Result<(), ()>> {
        let f = self.send(value);
        futures::pin_mut!(f);
        f.poll(cx).map(|r| r.map_err(|_| ()))
    }
}

impl EventSender for watch::Sender<(EventId, TransportPublicKey)> {
    fn send(
        &self,
        _cx: &mut std::task::Context<'_>,
        value: (EventId, TransportPublicKey),
    ) -> std::task::Poll<Result<(), ()>> {
        match self.send(value) {
            Ok(_) => std::task::Poll::Ready(Ok(())),
            Err(_) => std::task::Poll::Ready(Err(())),
        }
    }
}

impl EventSender for broadcast::Sender<(EventId, TransportPublicKey)> {
    fn send(
        &self,
        _cx: &mut std::task::Context<'_>,
        value: (EventId, TransportPublicKey),
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
            clean_up_tmp_dirs(self.labels.iter().map(|(l, _)| l));
        }
    }
}

#[cfg(feature = "trace-ot")]
type DefaultRegistry = CombinedRegister<2>;

#[cfg(not(feature = "trace-ot"))]
type DefaultRegistry = TestEventListener;

pub(super) struct Builder<ER> {
    pub config: NodeConfig,
    contract_handler_name: String,
    add_noise: bool,
    event_register: ER,
    contracts: Vec<(ContractContainer, WrappedState, bool)>,
    contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
    /// Seed for deterministic RNG in this node's transport layer
    pub rng_seed: u64,
}

impl<ER: NetEventRegister> Builder<ER> {
    /// Buils an in-memory node. Does nothing upon construction,
    pub fn build(
        builder: NodeConfig,
        event_register: ER,
        contract_handler_name: String,
        add_noise: bool,
        rng_seed: u64,
    ) -> Builder<ER> {
        Builder {
            config: builder.clone(),
            contract_handler_name,
            add_noise,
            event_register,
            contracts: Vec::new(),
            contract_subscribers: HashMap::new(),
            rng_seed,
        }
    }
}

/// A simulated in-memory network topology.
pub struct SimNetwork {
    name: String,
    clean_up_tmp_dirs: bool,
    labels: Vec<(NodeLabel, TransportPublicKey)>,
    pub(crate) event_listener: TestEventListener,
    user_ev_controller: Option<watch::Sender<(EventId, TransportPublicKey)>>,
    receiver_ch: watch::Receiver<(EventId, TransportPublicKey)>,
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
    /// Master seed for deterministic RNG - used to derive per-peer seeds
    seed: u64,
}

impl SimNetwork {
    /// Default seed for deterministic simulation
    pub const DEFAULT_SEED: u64 = 0xDEADBEEF_CAFEBABE;

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        name: &str,
        gateways: usize,
        nodes: usize,
        ring_max_htl: usize,
        rnd_if_htl_above: usize,
        max_connections: usize,
        min_connections: usize,
        seed: u64,
    ) -> Self {
        assert!(nodes > 0);
        let (user_ev_controller, mut receiver_ch) =
            watch::channel((0, TransportKeypair::new().public().clone()));
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
            seed,
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

    /// Derives a deterministic per-peer seed from the master seed and peer index.
    fn derive_peer_seed(&self, peer_index: usize) -> u64 {
        // Use a simple but effective mixing function
        let mut seed = self.seed;
        seed = seed.wrapping_add(peer_index as u64);
        seed ^= seed >> 33;
        seed = seed.wrapping_mul(0xff51afd7ed558ccd);
        seed ^= seed >> 33;
        seed = seed.wrapping_mul(0xc4ceb9fe1a85ec53);
        seed ^= seed >> 33;
        seed
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

    /// Configures fault injection for the network simulation.
    ///
    /// This enables deterministic fault injection using the simulation framework's
    /// `FaultConfig`. Faults include:
    /// - Message drops (via `message_loss_rate`) - deterministic with seeded RNG
    /// - Network partitions
    /// - Node crashes
    /// - Latency injection (via `latency_range`)
    ///
    /// The fault injector uses the network's seed for deterministic behavior.
    ///
    /// # Example
    /// ```ignore
    /// use freenet::simulation::FaultConfig;
    /// use std::time::Duration;
    ///
    /// let mut sim = SimNetwork::new(...).await;
    /// sim.with_fault_injection(FaultConfig::builder()
    ///     .message_loss_rate(0.1)
    ///     .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
    ///     .build());
    /// ```
    pub fn with_fault_injection(&mut self, config: crate::simulation::FaultConfig) {
        use crate::node::network_bridge::{set_fault_injector, FaultInjectorState};
        // Use a derived seed for fault injection to maintain determinism
        let fault_seed = self.seed.wrapping_add(0xFA01_7777);
        let state = FaultInjectorState::new(config, fault_seed);
        set_fault_injector(Some(std::sync::Arc::new(std::sync::Mutex::new(state))));
    }

    /// Clears any configured fault injection.
    pub fn clear_fault_injection(&mut self) {
        use crate::node::network_bridge::set_fault_injector;
        set_fault_injector(None);
    }

    /// Configures fault injection with VirtualTime mode enabled.
    ///
    /// VirtualTime mode queues messages with latency injection instead of using
    /// real-time delays. Use `advance_virtual_time()` to advance time and deliver
    /// pending messages.
    ///
    /// # Example
    /// ```ignore
    /// use freenet::simulation::{FaultConfig, VirtualTime};
    /// use std::time::Duration;
    ///
    /// let mut sim = SimNetwork::new(...).await;
    /// let vt = VirtualTime::new();
    /// sim.with_fault_injection_virtual_time(
    ///     FaultConfig::builder()
    ///         .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
    ///         .build(),
    ///     vt.clone(),
    /// );
    ///
    /// // Later, advance time to deliver pending messages
    /// vt.advance(Duration::from_millis(100));
    /// let delivered = sim.advance_virtual_time();
    /// ```
    pub fn with_fault_injection_virtual_time(
        &mut self,
        config: crate::simulation::FaultConfig,
        virtual_time: crate::simulation::VirtualTime,
    ) {
        use crate::node::network_bridge::{set_fault_injector, FaultInjectorState};
        let fault_seed = self.seed.wrapping_add(0xFA01_7777);
        let state = FaultInjectorState::new(config, fault_seed).with_virtual_time(virtual_time);
        set_fault_injector(Some(std::sync::Arc::new(std::sync::Mutex::new(state))));
    }

    /// Advances virtual time and delivers pending messages.
    ///
    /// This should be called after advancing the VirtualTime instance to deliver
    /// messages whose deadlines have passed.
    ///
    /// Returns the number of messages delivered.
    pub fn advance_virtual_time(&mut self) -> usize {
        use crate::node::network_bridge::get_fault_injector;
        if let Some(injector) = get_fault_injector() {
            let mut state = injector.lock().unwrap();
            state.advance_time()
        } else {
            0
        }
    }

    /// Returns the current network statistics from fault injection.
    ///
    /// These statistics track:
    /// - Messages sent, delivered, dropped
    /// - Drop reasons (loss rate, partition, crash)
    /// - Latency injection metrics
    ///
    /// Returns None if fault injection is not configured.
    pub fn get_network_stats(&self) -> Option<crate::node::network_bridge::NetworkStats> {
        use crate::node::network_bridge::get_fault_injector;
        get_fault_injector().map(|injector| {
            let state = injector.lock().unwrap();
            state.stats().clone()
        })
    }

    /// Resets the network statistics.
    pub fn reset_network_stats(&mut self) {
        use crate::node::network_bridge::get_fault_injector;
        if let Some(injector) = get_fault_injector() {
            let mut state = injector.lock().unwrap();
            state.reset_stats();
        }
    }

    #[allow(unused)]
    pub fn debug(&mut self) {
        self.clean_up_tmp_dirs = false;
    }

    async fn config_gateways(&mut self, num: NonZeroUsize) {
        info!("Building {} gateways", num);
        let mut configs = Vec::with_capacity(num.into());
        for node_no in 0..num.into() {
            let label = NodeLabel::gateway(&self.name, node_no);
            let port = crate::util::get_free_port().unwrap();
            let keypair = crate::transport::TransportKeypair::new();
            let addr = (Ipv6Addr::LOCALHOST, port).into();
            let peer_key_location = PeerKeyLocation::new(keypair.public().clone(), addr);
            let location = Location::random();

            let config_args = ConfigArgs {
                id: Some(format!("{label}")),
                mode: Some(OperationMode::Local),
                ..Default::default()
            };
            // TODO: it may be unnecessary use config_args.build() for the simulation. Related with the TODO in Config line 238
            let mut config = NodeConfig::new(config_args.build().await.unwrap())
                .await
                .unwrap();
            config.key_pair = keypair;
            config.network_listener_ip = Ipv6Addr::LOCALHOST.into();
            config.network_listener_port = port;
            config.with_own_addr(addr);
            config
                .with_location(location)
                .max_hops_to_live(self.ring_max_htl)
                .max_number_of_connections(self.max_connections)
                .min_number_of_connections(self.min_connections)
                .is_gateway()
                .rnd_if_htl_above(self.rnd_if_htl_above);
            self.event_listener
                .add_node(label.clone(), config.key_pair.public().clone());
            configs.push((
                config,
                GatewayConfig {
                    label,
                    peer_key_location,
                    location,
                },
            ));
        }
        configs[0].0.should_connect = false;

        let gateways: Vec<_> = configs.iter().map(|(_, gw)| gw.clone()).collect();
        for (mut this_node, this_config) in configs {
            for GatewayConfig {
                peer_key_location,
                location,
                ..
            } in gateways
                .iter()
                .filter(|config| this_config.label != config.label)
            {
                this_node.add_gateway(InitPeerNode::new(peer_key_location.clone(), *location));
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
            let peer_seed = self.derive_peer_seed(this_config.label.number());
            let gateway = Builder::build(
                this_node,
                event_listener,
                format!("{}-{label}", self.name, label = this_config.label),
                self.add_noise,
                peer_seed,
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
            let label = NodeLabel::node(&self.name, node_no);

            let config_args = ConfigArgs {
                id: Some(format!("{label}")),
                mode: Some(OperationMode::Local),
                ..Default::default()
            };
            let mut config = NodeConfig::new(config_args.build().await.unwrap())
                .await
                .unwrap();
            for GatewayConfig {
                peer_key_location,
                location,
                ..
            } in &gateways
            {
                config.add_gateway(InitPeerNode::new(peer_key_location.clone(), *location));
            }
            let port = crate::util::get_free_port().unwrap();
            let addr = (Ipv6Addr::LOCALHOST, port).into();
            config.network_listener_port = port;
            config.network_listener_ip = Ipv6Addr::LOCALHOST.into();
            config.key_pair = crate::transport::TransportKeypair::new();
            config.with_own_addr(addr);
            config
                .max_hops_to_live(self.ring_max_htl)
                .rnd_if_htl_above(self.rnd_if_htl_above)
                .max_number_of_connections(self.max_connections);

            self.event_listener
                .add_node(label.clone(), config.key_pair.public().clone());

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
            let peer_seed = self.derive_peer_seed(node_no);
            let node = Builder::build(
                config,
                event_listener,
                format!("{}-{label}", self.name),
                self.add_noise,
                peer_seed,
            );
            self.nodes.push((node, label));
        }
    }

    pub async fn start_with_rand_gen<R>(
        &mut self,
        seed: u64,
        max_contract_num: usize,
        iterations: usize,
    ) -> Vec<tokio::task::JoinHandle<anyhow::Result<()>>>
    where
        R: RandomEventGenerator + Send + 'static,
    {
        use crate::node::network_bridge::in_memory::is_peer_registered;

        let total_peer_num = self.gateways.len() + self.nodes.len();
        let mut peers = vec![];

        // Phase 1: Start all gateways first and collect their addresses
        let gateways: Vec<_> = self.gateways.drain(..).collect();
        let mut gateway_addrs = Vec::with_capacity(gateways.len());

        for (node, config) in gateways {
            let label = config.label.clone();
            // Use the peer_key_location address from GatewayConfig - this is the address
            // that will be registered in the peer registry
            let gateway_addr = config.peer_key_location.peer_addr.as_known()
                .expect("Gateway should have known address");
            gateway_addrs.push(*gateway_addr);

            tracing::debug!(peer = %label, addr = %gateway_addr, "starting gateway");
            let mut user_events = MemoryEventsGen::<R>::new_with_seed(
                self.receiver_ch.clone(),
                node.config.key_pair.public().clone(),
                seed,
            );
            user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);
            let span = tracing::info_span!("in_mem_gateway", %label);
            self.labels.push((label, node.config.key_pair.public().clone()));

            let node_task = async move { node.run_node(user_events, span).await };
            let handle = GlobalExecutor::spawn(node_task);
            peers.push(handle);

            tokio::time::sleep(self.start_backoff).await;
        }

        // Phase 2: Wait for all gateways to be registered in the peer registry
        // This prevents the race condition where regular nodes try to connect
        // before gateways are ready to receive connections
        let registration_timeout = Duration::from_secs(10);
        let poll_interval = Duration::from_millis(10);
        let start = std::time::Instant::now();

        'wait_loop: loop {
            let mut all_registered = true;
            for addr in &gateway_addrs {
                if !is_peer_registered(addr) {
                    all_registered = false;
                    break;
                }
            }

            if all_registered {
                tracing::debug!("All {} gateways registered", gateway_addrs.len());
                // Give gateways additional time to fully initialize their event loops
                // before regular nodes start connecting
                tokio::time::sleep(Duration::from_millis(100)).await;
                tracing::debug!("Starting regular nodes");
                break 'wait_loop;
            }

            if start.elapsed() > registration_timeout {
                tracing::warn!(
                    "Timeout waiting for gateway registration, some may not be ready. \
                     Registered: {}/{}",
                    gateway_addrs.iter().filter(|a| is_peer_registered(a)).count(),
                    gateway_addrs.len()
                );
                break 'wait_loop;
            }

            tokio::time::sleep(poll_interval).await;
        }

        // Phase 3: Start all regular nodes
        let nodes: Vec<_> = self.nodes.drain(..).collect();
        for (node, label) in nodes {
            tracing::debug!(peer = %label, "starting regular node");
            let mut user_events = MemoryEventsGen::<R>::new_with_seed(
                self.receiver_ch.clone(),
                node.config.key_pair.public().clone(),
                seed,
            );
            user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);
            let span = tracing::info_span!("in_mem_node", %label);
            self.labels.push((label, node.config.key_pair.public().clone()));

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
            let pub_key = builder.config.key_pair.public();
            self.labels.push((label.clone(), pub_key.clone()));
            peers.push((label, builder.config));
        }
        self.labels.sort_by(|(a, _), (b, _)| a.cmp(b));
        peers.sort_by(|(a, _), (b, _)| a.cmp(b));
        peers
    }

    /// Returns the connectivity in the network per peer (that is all the connections
    /// this peers has registered).
    pub fn node_connectivity(
        &self,
    ) -> HashMap<NodeLabel, (TransportPublicKey, HashMap<NodeLabel, Distance>)> {
        let mut peers_connections = HashMap::with_capacity(self.labels.len());
        let key_to_label: HashMap<_, _> = self.labels.iter().map(|(k, v)| (v, k)).collect();
        for (label, key) in &self.labels {
            let conns = self
                .event_listener
                .connections(key)
                .map(|(k, d)| (key_to_label[&k.pub_key].clone(), d))
                .collect::<HashMap<_, _>>();
            peers_connections.insert(label.clone(), (key.clone(), conns));
        }
        peers_connections
    }

    /// Start an event chain for this simulation. Allows passing a different controller for the peers.
    ///
    /// If done make sure you set the proper receiving side for the controller. For example in the
    /// nodes built through the [`build_peers`](`Self::build_peers`) method.
    pub fn event_chain(
        mut self,
        total_events: u32,
        controller: Option<watch::Sender<(EventId, TransportPublicKey)>>,
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
    pub fn check_connectivity(&self, time_out: Duration) -> anyhow::Result<()> {
        self.connectivity(time_out, 1.0)
    }

    /// Checks that a percentage (given as a float between 0 and 1) of the nodes has at least
    /// one connection to any other peers.
    pub fn check_partial_connectivity(
        &self,
        time_out: Duration,
        percent: f64,
    ) -> anyhow::Result<()> {
        self.connectivity(time_out, percent)
    }

    fn connectivity(&self, time_out: Duration, percent: f64) -> anyhow::Result<()> {
        let num_nodes = self.number_of_nodes;
        let mut connected = HashSet::new();
        let elapsed = Instant::now();
        while elapsed.elapsed() < time_out && (connected.len() as f64 / num_nodes as f64) < percent
        {
            for node in self.number_of_gateways..num_nodes + self.number_of_gateways {
                if !connected.contains(&node) && self.connected(&NodeLabel::node(&self.name, node))
                {
                    connected.insert(node);
                }
            }
        }

        let expected =
            HashSet::from_iter(self.number_of_gateways..num_nodes + self.number_of_gateways);
        let mut missing: Vec<_> = expected
            .difference(&connected)
            .map(|n| format!("{}-node-{n}", self.name))
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

    pub fn connected(&self, peer: &NodeLabel) -> bool {
        let pos = self
            .labels
            .binary_search_by(|(label, _)| label.cmp(peer))
            .expect("peer not found");
        self.event_listener.is_connected(&self.labels[pos].1)
    }

    /// Returns a copy of all network event logs captured during the simulation.
    ///
    /// These logs can be used to verify deterministic behavior by comparing
    /// event sequences across runs with the same seed.
    pub async fn get_event_logs(&self) -> Vec<crate::tracing::NetLogMessage> {
        self.event_listener.logs.lock().await.clone()
    }

    /// Returns event logs filtered and sorted for deterministic comparison.
    ///
    /// Events are sorted by (transaction, peer_addr, event_kind_name).
    /// Timestamps are ignored since they may vary between runs.
    pub async fn get_deterministic_event_summary(&self) -> Vec<EventSummary> {
        let logs = self.event_listener.logs.lock().await;
        let mut summaries: Vec<EventSummary> = logs
            .iter()
            .map(|log| {
                let event_detail = format!("{:?}", log.kind);
                // Use the structured variant_name() method instead of parsing debug output
                let event_kind_name = log.kind.variant_name().to_string();
                // Extract contract key using the structured accessor
                let contract_key = log.kind.contract_key().map(|k| format!("{:?}", k));
                // Extract state hash using the structured accessor
                let state_hash = log.kind.state_hash().map(String::from);
                EventSummary {
                    tx: log.tx,
                    peer_addr: log.peer_id.addr,
                    event_kind_name,
                    contract_key,
                    state_hash,
                    event_detail,
                }
            })
            .collect();
        // Sort for deterministic comparison
        summaries.sort();
        summaries
    }

    /// Counts events by type for quick comparison.
    pub async fn get_event_counts(&self) -> std::collections::HashMap<String, usize> {
        let logs = self.event_listener.logs.lock().await;
        let mut counts = std::collections::HashMap::new();
        for log in logs.iter() {
            // Use the structured variant_name() method instead of parsing debug output
            let key = log.kind.variant_name();
            *counts.entry(key.to_string()).or_insert(0) += 1;
        }
        counts
    }

    /// Recommended to calling after `check_connectivity` to ensure enough time
    /// elapsed for all peers to become connected.
    ///
    /// Checks that there is a good connectivity over the simulated network,
    /// meaning that:
    ///
    /// - at least 50% of the peers have more than the minimum connections
    /// - the average number of connections per peer is above the mean between max and min connections
    pub fn network_connectivity_quality(&self) -> anyhow::Result<()> {
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

    /// Checks convergence of contract states across all peers.
    ///
    /// Returns a `ConvergenceResult` containing:
    /// - Which contracts have converged (all replicas have the same state hash)
    /// - Which contracts have not converged (different state hashes across replicas)
    /// - Per-contract details of state hashes per peer
    ///
    /// This is useful for testing eventual consistency properties.
    pub async fn check_convergence(&self) -> ConvergenceResult {
        let summary = self.get_deterministic_event_summary().await;

        // Group (contract_key -> peer_addr -> latest_state_hash)
        let mut contract_states: HashMap<String, HashMap<SocketAddr, String>> = HashMap::new();

        for event in &summary {
            if let (Some(contract_key), Some(state_hash)) = (&event.contract_key, &event.state_hash)
            {
                // Keep the latest state for each peer/contract pair
                contract_states
                    .entry(contract_key.clone())
                    .or_default()
                    .insert(event.peer_addr, state_hash.clone());
            }
        }

        let mut converged = Vec::new();
        let mut diverged = Vec::new();

        for (contract_key, peer_states) in contract_states {
            if peer_states.len() < 2 {
                // Need at least 2 peers to check convergence
                continue;
            }

            let unique_states: HashSet<&String> = peer_states.values().collect();

            if unique_states.len() == 1 {
                let state = unique_states.into_iter().next().unwrap().clone();
                converged.push(ConvergedContract {
                    contract_key,
                    state_hash: state,
                    replica_count: peer_states.len(),
                });
            } else {
                diverged.push(DivergedContract {
                    contract_key,
                    peer_states: peer_states
                        .into_iter()
                        .map(|(addr, hash)| (addr, hash))
                        .collect(),
                });
            }
        }

        ConvergenceResult { converged, diverged }
    }

    /// Waits for convergence of contract states, polling at regular intervals.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for convergence
    /// * `poll_interval` - How often to check convergence
    /// * `min_contracts` - Minimum number of contracts that must be replicated (default: 1)
    ///
    /// # Returns
    /// * `Ok(ConvergenceResult)` - If convergence achieved (no diverged contracts)
    /// * `Err(ConvergenceResult)` - If timeout reached with diverged contracts
    ///
    /// # Example
    /// ```ignore
    /// let result = sim.await_convergence(
    ///     Duration::from_secs(30),
    ///     Duration::from_millis(500),
    ///     1,
    /// ).await;
    ///
    /// match result {
    ///     Ok(r) => println!("{} contracts converged", r.converged.len()),
    ///     Err(r) => panic!("{} contracts still diverged", r.diverged.len()),
    /// }
    /// ```
    pub async fn await_convergence(
        &self,
        timeout: Duration,
        poll_interval: Duration,
        min_contracts: usize,
    ) -> Result<ConvergenceResult, ConvergenceResult> {
        let start = Instant::now();

        loop {
            let result = self.check_convergence().await;

            // Check if we have enough contracts and all have converged
            let total_replicated = result.converged.len() + result.diverged.len();
            if total_replicated >= min_contracts && result.diverged.is_empty() {
                tracing::info!(
                    "Convergence achieved: {} contracts converged in {:?}",
                    result.converged.len(),
                    start.elapsed()
                );
                return Ok(result);
            }

            // Check timeout
            if start.elapsed() >= timeout {
                tracing::warn!(
                    "Convergence timeout after {:?}: {} converged, {} diverged",
                    timeout,
                    result.converged.len(),
                    result.diverged.len()
                );
                return Err(result);
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Asserts that all replicated contracts have converged.
    ///
    /// This is a convenience method that combines `await_convergence` with
    /// an assertion. It panics if convergence is not achieved.
    ///
    /// # Panics
    /// Panics if convergence is not achieved within the timeout.
    pub async fn assert_convergence(&self, timeout: Duration, poll_interval: Duration) {
        match self.await_convergence(timeout, poll_interval, 1).await {
            Ok(result) => {
                tracing::info!(
                    "Convergence assertion passed: {} contracts",
                    result.converged.len()
                );
            }
            Err(result) => {
                let diverged_details: Vec<String> = result
                    .diverged
                    .iter()
                    .map(|d| {
                        format!(
                            "Contract {}: {} different states across {} peers",
                            d.contract_key,
                            d.unique_state_count(),
                            d.peer_states.len()
                        )
                    })
                    .collect();

                panic!(
                    "Convergence assertion failed after {:?}: {} contracts diverged\n{}",
                    timeout,
                    result.diverged.len(),
                    diverged_details.join("\n")
                );
            }
        }
    }

    /// Returns the convergence rate as a ratio (0.0 to 1.0).
    ///
    /// Convergence rate = converged_contracts / total_replicated_contracts
    pub async fn convergence_rate(&self) -> f64 {
        let result = self.check_convergence().await;
        let total = result.converged.len() + result.diverged.len();
        if total == 0 {
            return 1.0; // No contracts replicated yet
        }
        result.converged.len() as f64 / total as f64
    }

    // =========================================================================
    // Gap 4: Direct State Query API (event-based)
    // =========================================================================

    /// Returns the latest known state hash for each contract on each peer.
    ///
    /// This provides a view of contract state distribution across the network
    /// by examining event logs. Returns a map of contract_key -> (peer_addr -> state_hash).
    ///
    /// # Example
    /// ```ignore
    /// let states = sim.get_contract_state_hashes().await;
    /// for (contract, peer_states) in states {
    ///     println!("Contract {}: {} replicas", contract, peer_states.len());
    ///     for (peer, hash) in peer_states {
    ///         println!("  {}: {}", peer, hash);
    ///     }
    /// }
    /// ```
    pub async fn get_contract_state_hashes(
        &self,
    ) -> HashMap<String, HashMap<SocketAddr, String>> {
        let summary = self.get_deterministic_event_summary().await;

        let mut contract_states: HashMap<String, HashMap<SocketAddr, String>> = HashMap::new();

        for event in &summary {
            if let (Some(contract_key), Some(state_hash)) = (&event.contract_key, &event.state_hash)
            {
                // Keep the latest state for each peer/contract pair
                contract_states
                    .entry(contract_key.clone())
                    .or_default()
                    .insert(event.peer_addr, state_hash.clone());
            }
        }

        contract_states
    }

    /// Returns a list of contracts and the peers that have them.
    ///
    /// This is useful for checking contract distribution across the network.
    pub async fn get_contract_distribution(&self) -> Vec<ContractDistribution> {
        let states = self.get_contract_state_hashes().await;
        states
            .into_iter()
            .map(|(contract_key, peer_states)| ContractDistribution {
                contract_key,
                replica_count: peer_states.len(),
                peers: peer_states.keys().cloned().collect(),
            })
            .collect()
    }

    // =========================================================================
    // Gap T4: Operation Completion Tracking
    // =========================================================================

    /// Returns a summary of operation completion status.
    ///
    /// This tracks Put, Get, Subscribe, and Update operations from request to
    /// completion (success or failure).
    ///
    /// # Example
    /// ```ignore
    /// let summary = sim.get_operation_summary().await;
    /// println!("Put: {}/{} completed ({:.1}% success)",
    ///     summary.put.completed(),
    ///     summary.put.requested,
    ///     summary.put.success_rate() * 100.0);
    /// ```
    pub async fn get_operation_summary(&self) -> OperationSummary {
        let logs = self.event_listener.logs.lock().await;

        let mut summary = OperationSummary::default();

        for log in logs.iter() {
            match &log.kind {
                // Put operations
                crate::tracing::EventKind::Put(put_event) => {
                    use crate::tracing::PutEvent;
                    match put_event {
                        PutEvent::Request { .. } => summary.put.requested += 1,
                        PutEvent::PutSuccess { .. } => summary.put.succeeded += 1,
                        PutEvent::PutFailure { .. } => summary.put.failed += 1,
                        PutEvent::BroadcastEmitted { .. } => summary.put.broadcasts_emitted += 1,
                        PutEvent::BroadcastReceived { .. } => summary.put.broadcasts_received += 1,
                    }
                }
                // Get operations
                crate::tracing::EventKind::Get(get_event) => {
                    use crate::tracing::GetEvent;
                    match get_event {
                        GetEvent::Request { .. } => summary.get.requested += 1,
                        GetEvent::GetSuccess { .. } => summary.get.succeeded += 1,
                        GetEvent::GetFailure { .. } => summary.get.failed += 1,
                        _ => {}
                    }
                }
                // Subscribe operations
                crate::tracing::EventKind::Subscribe(sub_event) => {
                    use crate::tracing::SubscribeEvent;
                    match sub_event {
                        SubscribeEvent::Request { .. } => summary.subscribe.requested += 1,
                        SubscribeEvent::SubscribeSuccess { .. } => {
                            summary.subscribe.succeeded += 1
                        }
                        SubscribeEvent::SubscribeNotFound { .. } => summary.subscribe.failed += 1,
                        _ => {}
                    }
                }
                // Update operations (no UpdateFailure variant exists)
                crate::tracing::EventKind::Update(update_event) => {
                    use crate::tracing::UpdateEvent;
                    match update_event {
                        UpdateEvent::Request { .. } => summary.update.requested += 1,
                        UpdateEvent::UpdateSuccess { .. } => summary.update.succeeded += 1,
                        UpdateEvent::BroadcastReceived { .. } => {
                            summary.update.broadcasts_received += 1
                        }
                        _ => {}
                    }
                }
                // Timeouts
                crate::tracing::EventKind::Timeout { .. } => {
                    summary.timeouts += 1;
                }
                _ => {}
            }
        }

        summary
    }

    /// Checks if all requested operations have completed (either succeeded or failed).
    ///
    /// Returns (completed, pending) counts.
    pub async fn operation_completion_status(&self) -> (usize, usize) {
        let summary = self.get_operation_summary().await;
        let completed = summary.total_completed();
        let requested = summary.total_requested();
        let pending = requested.saturating_sub(completed);
        (completed, pending)
    }

    /// Returns the overall operation success rate (0.0 to 1.0).
    pub async fn operation_success_rate(&self) -> f64 {
        let summary = self.get_operation_summary().await;
        summary.overall_success_rate()
    }

    /// Waits for all operations to complete, up to a timeout.
    ///
    /// # Returns
    /// * `Ok(OperationSummary)` - If all operations completed
    /// * `Err(OperationSummary)` - If timeout reached with pending operations
    pub async fn await_operation_completion(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<OperationSummary, OperationSummary> {
        let start = Instant::now();

        loop {
            let summary = self.get_operation_summary().await;
            let (completed, pending) = (summary.total_completed(), summary.total_requested());

            // All operations completed (or none requested)
            if pending == 0 || completed >= pending {
                return Ok(summary);
            }

            if start.elapsed() >= timeout {
                tracing::warn!(
                    "Operation completion timeout: {} completed, {} pending",
                    completed,
                    pending.saturating_sub(completed)
                );
                return Err(summary);
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Asserts that operation success rate meets the threshold.
    ///
    /// # Panics
    /// Panics if success rate is below the threshold.
    pub async fn assert_operation_success_rate(&self, min_rate: f64) {
        let summary = self.get_operation_summary().await;
        let rate = summary.overall_success_rate();

        if rate < min_rate {
            panic!(
                "Operation success rate {:.1}% is below threshold {:.1}%\n\
                 Put: {}/{} ({:.1}%), Get: {}/{} ({:.1}%), \
                 Subscribe: {}/{} ({:.1}%), Update: {}/{} ({:.1}%)",
                rate * 100.0,
                min_rate * 100.0,
                summary.put.succeeded,
                summary.put.completed(),
                summary.put.success_rate() * 100.0,
                summary.get.succeeded,
                summary.get.completed(),
                summary.get.success_rate() * 100.0,
                summary.subscribe.succeeded,
                summary.subscribe.completed(),
                summary.subscribe.success_rate() * 100.0,
                summary.update.succeeded,
                summary.update.completed(),
                summary.update.success_rate() * 100.0,
            );
        }
    }
}

/// Result of a convergence check.
#[derive(Debug, Clone)]
pub struct ConvergenceResult {
    /// Contracts where all replicas have the same state hash.
    pub converged: Vec<ConvergedContract>,
    /// Contracts where replicas have different state hashes.
    pub diverged: Vec<DivergedContract>,
}

impl ConvergenceResult {
    /// Returns the total number of contracts checked.
    pub fn total_contracts(&self) -> usize {
        self.converged.len() + self.diverged.len()
    }

    /// Returns the convergence rate as a ratio (0.0 to 1.0).
    pub fn rate(&self) -> f64 {
        if self.total_contracts() == 0 {
            return 1.0;
        }
        self.converged.len() as f64 / self.total_contracts() as f64
    }

    /// Returns true if all replicated contracts have converged.
    pub fn is_converged(&self) -> bool {
        self.diverged.is_empty()
    }
}

/// A contract that has converged (all replicas have the same state).
#[derive(Debug, Clone)]
pub struct ConvergedContract {
    /// The contract key
    pub contract_key: String,
    /// The state hash that all replicas have
    pub state_hash: String,
    /// Number of replicas that have this state
    pub replica_count: usize,
}

/// A contract that has diverged (replicas have different states).
#[derive(Debug, Clone)]
pub struct DivergedContract {
    /// The contract key
    pub contract_key: String,
    /// Map of peer address to their state hash
    pub peer_states: Vec<(SocketAddr, String)>,
}

impl DivergedContract {
    /// Returns the number of unique states across replicas.
    pub fn unique_state_count(&self) -> usize {
        let unique: HashSet<&String> = self.peer_states.iter().map(|(_, h)| h).collect();
        unique.len()
    }
}

// =============================================================================
// Gap 4: Contract Distribution Types
// =============================================================================

/// Information about how a contract is distributed across the network.
#[derive(Debug, Clone)]
pub struct ContractDistribution {
    /// The contract key
    pub contract_key: String,
    /// Number of replicas
    pub replica_count: usize,
    /// Peers that have this contract
    pub peers: Vec<SocketAddr>,
}

// =============================================================================
// Gap T4: Operation Tracking Types
// =============================================================================

/// Summary of operation completion status across the network.
#[derive(Debug, Clone, Default)]
pub struct OperationSummary {
    /// Put operation statistics
    pub put: PutOperationStats,
    /// Get operation statistics
    pub get: OperationStats,
    /// Subscribe operation statistics
    pub subscribe: OperationStats,
    /// Update operation statistics
    pub update: UpdateOperationStats,
    /// Total number of timed-out operations
    pub timeouts: usize,
}

impl OperationSummary {
    /// Returns total number of operations requested.
    pub fn total_requested(&self) -> usize {
        self.put.requested + self.get.requested + self.subscribe.requested + self.update.requested
    }

    /// Returns total number of operations completed (succeeded + failed).
    pub fn total_completed(&self) -> usize {
        self.put.completed()
            + self.get.completed()
            + self.subscribe.completed()
            + self.update.completed()
    }

    /// Returns total number of successful operations.
    pub fn total_succeeded(&self) -> usize {
        self.put.succeeded + self.get.succeeded + self.subscribe.succeeded + self.update.succeeded
    }

    /// Returns total number of failed operations.
    pub fn total_failed(&self) -> usize {
        self.put.failed + self.get.failed + self.subscribe.failed + self.update.failed
    }

    /// Returns overall success rate (0.0 to 1.0).
    pub fn overall_success_rate(&self) -> f64 {
        let completed = self.total_completed();
        if completed == 0 {
            return 1.0; // No operations completed yet
        }
        self.total_succeeded() as f64 / completed as f64
    }

    /// Returns true if all requested operations have completed.
    pub fn all_completed(&self) -> bool {
        self.total_completed() >= self.total_requested()
    }
}

/// Statistics for a basic operation type (Get, Subscribe).
#[derive(Debug, Clone, Default)]
pub struct OperationStats {
    /// Number of operations requested
    pub requested: usize,
    /// Number of operations that succeeded
    pub succeeded: usize,
    /// Number of operations that failed
    pub failed: usize,
}

impl OperationStats {
    /// Returns total completed operations (succeeded + failed).
    pub fn completed(&self) -> usize {
        self.succeeded + self.failed
    }

    /// Returns success rate (0.0 to 1.0).
    pub fn success_rate(&self) -> f64 {
        let completed = self.completed();
        if completed == 0 {
            return 1.0;
        }
        self.succeeded as f64 / completed as f64
    }
}

/// Statistics for Put operations (includes broadcast tracking).
#[derive(Debug, Clone, Default)]
pub struct PutOperationStats {
    /// Number of Put operations requested
    pub requested: usize,
    /// Number of Put operations that succeeded
    pub succeeded: usize,
    /// Number of Put operations that failed
    pub failed: usize,
    /// Number of broadcasts emitted
    pub broadcasts_emitted: usize,
    /// Number of broadcasts received by peers
    pub broadcasts_received: usize,
}

impl PutOperationStats {
    /// Returns total completed operations (succeeded + failed).
    pub fn completed(&self) -> usize {
        self.succeeded + self.failed
    }

    /// Returns success rate (0.0 to 1.0).
    pub fn success_rate(&self) -> f64 {
        let completed = self.completed();
        if completed == 0 {
            return 1.0;
        }
        self.succeeded as f64 / completed as f64
    }

    /// Returns broadcast propagation rate (received / emitted).
    pub fn broadcast_propagation_rate(&self) -> f64 {
        if self.broadcasts_emitted == 0 {
            return 1.0;
        }
        self.broadcasts_received as f64 / self.broadcasts_emitted as f64
    }
}

/// Statistics for Update operations (includes broadcast tracking).
#[derive(Debug, Clone, Default)]
pub struct UpdateOperationStats {
    /// Number of Update operations requested
    pub requested: usize,
    /// Number of Update operations that succeeded
    pub succeeded: usize,
    /// Number of Update operations that failed
    pub failed: usize,
    /// Number of update broadcasts received by subscribers
    pub broadcasts_received: usize,
}

impl UpdateOperationStats {
    /// Returns total completed operations (succeeded + failed).
    pub fn completed(&self) -> usize {
        self.succeeded + self.failed
    }

    /// Returns success rate (0.0 to 1.0).
    pub fn success_rate(&self) -> f64 {
        let completed = self.completed();
        if completed == 0 {
            return 1.0;
        }
        self.succeeded as f64 / completed as f64
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
            clean_up_tmp_dirs(self.labels.iter().map(|(l, _)| l));
        }
    }
}

fn clean_up_tmp_dirs<'a>(labels: impl Iterator<Item = &'a NodeLabel>) {
    for label in labels {
        let p = std::env::temp_dir().join(format!(
            "freenet-executor-{sim}-{label}",
            sim = "sim",
            label = label
        ));
        let _ = std::fs::remove_dir_all(p);
    }
}

use super::op_state_manager::OpManager;
use crate::client_events::ClientEventsProxy;
use crate::contract::OperationMode;

pub(super) trait NetworkBridgeExt: Clone + 'static {
    /// Receive a message and the source address of the sender
    fn recv(
        &mut self,
    ) -> impl Future<Output = Result<(NetMessage, Option<SocketAddr>), ConnectionError>> + Send;
}

struct RunnerConfig<NB, UsrEv>
where
    NB: NetworkBridge,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    peer_key: PeerKeyLocation,
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

async fn run_node<NB, UsrEv>(mut config: RunnerConfig<NB, UsrEv>) -> anyhow::Result<()>
where
    NB: NetworkBridge + NetworkBridgeExt,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    let join_task =
        connect::initial_join_procedure(config.op_manager.clone(), &config.gateways).await?;
    let (client_responses, _cli_response_sender) = contract::client_responses_channel();
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
        client_event_handling(
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
    let result = run_event_listener(node_controller_rx, config)
        .instrument(parent_span)
        .await;

    join_task.abort();
    let _ = join_task.await;
    result
}

/// Starts listening to incoming events. Will attempt to join the ring if any gateways have been provided.
async fn run_event_listener<NB, UsrEv>(
    mut node_controller_rx: tokio::sync::mpsc::Receiver<NodeEvent>,
    RunnerConfig {
        peer_key,
        gateways,
        parent_span,
        op_manager,
        mut conn_manager,
        mut notification_channel,
        event_register,
        mut executor_listener,
        client_wait_for_transaction: mut wait_for_event,
        ..
    }: RunnerConfig<NB, UsrEv>,
) -> anyhow::Result<()>
where
    NB: NetworkBridge + NetworkBridgeExt,
    UsrEv: ClientEventsProxy + Send + 'static,
{
    // todo: this two containers need to be clean up on transaction time-out
    let mut pending_from_executor = HashSet::new();
    let mut tx_to_client: HashMap<Transaction, crate::client_events::ClientId> = HashMap::new();
    loop {
        let (msg, source_addr) = tokio::select! {
            msg = conn_manager.recv() => {
                match msg {
                    Ok((m, addr)) => {
                        tracing::debug!(source_addr = ?addr, msg_id = %m.id(), "Received message from network");
                        (Ok(Either::Left(m)), addr)
                    },
                    Err(e) => (Err(anyhow::Error::from(e)), None),
                }
            }
            msg = notification_channel.notifications_receiver.recv() => {
                if let Some(msg) = msg {
                    match &msg {
                        Either::Left(net_msg) => {
                            tracing::debug!(msg_id = %net_msg.id(), "Received NetMessage from notification channel (no source_addr)");
                        }
                        Either::Right(_) => {}
                    }
                    (Ok(msg), None)
                } else {
                    anyhow::bail!("notification channel shutdown, fatal error");
                }
            }
            msg = node_controller_rx.recv() => {
                if let Some(msg) = msg {
                    (Ok(Either::Right(msg)), None)
                } else {
                    anyhow::bail!("node controller channel shutdown, fatal error");
                }
            }
            event_id = wait_for_event.relay_transaction_result_to_client() => {
                if let Ok((client_id, transaction)) = event_id {
                    match transaction {
                        contract::WaitingTransaction::Transaction(transaction) => {
                            tx_to_client.insert(transaction, client_id);
                        }
                        contract::WaitingTransaction::Subscription { contract_key } => {
                            // For subscriptions, we track the client waiting for responses
                            // related to this contract key. The actual subscription response
                            // will be handled through the contract notification system.
                            tracing::debug!(
                                "Client {} waiting for subscription to contract {}",
                                client_id,
                                contract_key
                            );
                            // Note: Unlike regular transactions, subscriptions don't have a
                            // transaction ID to track. The subscription system handles routing
                            // updates to subscribed clients through the contract key.
                        }
                    }
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
            super::handle_aborted_op(tx, &op_manager, &gateways).await?;
        }

        // Handle locally-initiated outbound connect requests:
        // If we received a ConnectMsg::Request from the notification channel (source_addr = None)
        // and we have a stored ConnectOp with first_hop, this is OUR outbound request
        // that needs to be sent to the gateway, not processed locally.
        if source_addr.is_none() {
            // Check if this is a Connect Request message
            let connect_tx = match &msg {
                Ok(Either::Left(NetMessage::V1(NetMessageV1::Connect(connect_msg)))) => {
                    connect::extract_connect_request_tx(connect_msg)
                }
                _ => None,
            };

            if let Some(tx_id) = connect_tx {
                // Check if we have this operation stored (meaning we initiated it)
                if let Ok(Some(crate::operations::OpEnum::Connect(connect_op))) =
                    op_manager.pop(&tx_id)
                {
                    if let Some(ref first_hop) = connect_op.first_hop {
                        if let Some(target_addr) = first_hop.socket_addr() {
                            tracing::debug!(
                                tx = %tx_id,
                                target = %target_addr,
                                "Routing locally-initiated connect request to gateway"
                            );
                            // Push the operation back - we still need to track it
                            if let Err(e) = op_manager
                                .push(tx_id, crate::operations::OpEnum::Connect(connect_op))
                                .await
                            {
                                tracing::error!(tx = %tx_id, error = %e, "Failed to push connect op back");
                            }
                            // Send to the gateway
                            if let Ok(Either::Left(net_msg)) = msg {
                                if let Err(e) = conn_manager.send(target_addr, net_msg).await {
                                    tracing::error!(tx = %tx_id, error = %e, "Failed to send connect request to gateway");
                                }
                            }
                            continue;
                        }
                    }
                    // Push back if we couldn't route it
                    if let Err(e) = op_manager
                        .push(tx_id, crate::operations::OpEnum::Connect(connect_op))
                        .await
                    {
                        tracing::error!(tx = %tx_id, error = %e, "Failed to push connect op back");
                    }
                }
            }
        }

        let msg = match msg {
            Ok(Either::Left(msg)) => msg,
            Ok(Either::Right(action)) => match action {
                NodeEvent::DropConnection(peer_addr) => {
                    tracing::info!("Dropping connection to {peer_addr}");
                    // Look up the peer by address in the ring
                    if let Some(peer_loc) = op_manager
                        .ring
                        .connection_manager
                        .get_peer_by_addr(peer_addr)
                    {
                        // Convert PeerKeyLocation to PeerId for the disconnect/prune operations
                        let peer_id = PeerId::new(
                            peer_loc
                                .socket_addr()
                                .expect("peer should have socket address"),
                            peer_loc.pub_key().clone(),
                        );
                        // Capture connection duration before pruning
                        let connection_duration_ms = op_manager
                            .ring
                            .connection_manager
                            .get_connection_duration_ms(peer_addr);
                        if let Some(event) = crate::tracing::NetEventLog::disconnected_with_context(
                            &op_manager.ring,
                            &peer_id,
                            crate::tracing::DisconnectReason::Explicit(
                                "connection dropped by node request".to_string(),
                            ),
                            connection_duration_ms,
                            None,
                            None,
                        ) {
                            event_register.register_events(Either::Left(event)).await;
                        }
                        let prune_result = op_manager.ring.prune_connection(peer_id).await;

                        // Handle orphaned transactions immediately (retry via alternate routes)
                        for tx in prune_result.orphaned_transactions {
                            if let Err(err) =
                                super::handle_aborted_op(tx, &op_manager, &gateways).await
                            {
                                tracing::warn!(
                                    %tx,
                                    error = %err,
                                    "Failed to handle orphaned transaction"
                                );
                            }
                        }
                    }
                    continue;
                }
                NodeEvent::ConnectPeer { peer, .. } => {
                    tracing::info!("Notifying connection to {peer}");
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
                NodeEvent::QueryConnections { .. } => {
                    unimplemented!()
                }
                NodeEvent::TransactionTimedOut(_) => {
                    unimplemented!()
                }
                NodeEvent::TransactionCompleted(_) => {
                    unimplemented!()
                }
                NodeEvent::LocalSubscribeComplete { .. } => {
                    unimplemented!()
                }
                NodeEvent::QuerySubscriptions { .. } => {
                    unimplemented!()
                }
                NodeEvent::QueryNodeDiagnostics { .. } => {
                    unimplemented!()
                }
                NodeEvent::ExpectPeerConnection { addr } => {
                    tracing::debug!(%addr, "ExpectPeerConnection ignored in testing impl");
                    continue;
                }
                NodeEvent::BroadcastProximityCache { message } => {
                    tracing::debug!(?message, "BroadcastProximityCache ignored in testing impl");
                    continue;
                }
                NodeEvent::ClientDisconnected { client_id } => {
                    tracing::debug!(%client_id, "Client disconnected");
                    op_manager
                        .ring
                        .remove_client_from_all_subscriptions(client_id);
                    continue;
                }
            },
            Err(err) => {
                tracing::error!("Error receiving message: {}", err);
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

        let msg = super::process_message(
            msg,
            op_manager,
            conn_manager.clone(),
            event_listener,
            executor_callback,
            source_addr,
        )
        .instrument(span);
        GlobalExecutor::spawn(msg);
    }
}
