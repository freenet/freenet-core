//! Simulation network implementation for testing Freenet nodes.
//!
//! This module provides `SimNetwork`, a fully in-memory simulation framework for testing
//! Freenet's peer-to-peer network behavior. It enables deterministic testing of complex
//! distributed scenarios without real network I/O.
//!
//! # Key Components
//!
//! - [`SimNetwork`]: The main simulation controller that manages virtual nodes
//! - [`NodeLabel`]: Identifies nodes in the simulation (gateways and regular nodes)
//! - [`FaultConfig`]: Configures fault injection (message loss, partitions, latency)
//! - [`VirtualTime`]: Controls deterministic time progression
//!
//! # Features
//!
//! - **Deterministic execution**: All randomness is seeded for reproducible tests
//! - **Fault injection**: Simulate message loss, network partitions, and node crashes
//! - **Virtual time**: Control time progression for testing time-dependent behavior
//! - **Node lifecycle**: Crash and restart nodes while preserving identity
//! - **Network-scoped state**: Each simulation is isolated from others
//!
//! # Example
//!
//! ```ignore
//! use freenet::dev_tool::SimNetwork;
//! use freenet::simulation::FaultConfig;
//!
//! // Create a 3-node network with 1 gateway
//! let mut sim = SimNetwork::new(
//!     "my-test",
//!     1,  // gateways
//!     2,  // regular nodes
//!     7,  // ring_max_htl
//!     3,  // rnd_if_htl_above
//!     10, // max_connections
//!     2,  // min_connections
//!     42, // seed for determinism
//! ).await;
//!
//! // Start the network
//! let handles = sim.start_with_rand_gen::<rand::rngs::SmallRng>(42, 1, 1).await;
//!
//! // Inject 10% message loss
//! sim.with_fault_injection(FaultConfig::builder()
//!     .message_loss_rate(0.1)
//!     .build());
//!
//! // Advance virtual time to trigger pending message deliveries
//! sim.advance_time(Duration::from_millis(100));
//! ```
//!
//! # Network Isolation
//!
//! Each `SimNetwork` instance is identified by its name and maintains isolated state.
//! This allows running multiple independent simulations in parallel tests without
//! interference. The fault injection state is automatically cleaned up when the
//! simulation is dropped.
//!
//! # Thread Safety
//!
//! The simulation is thread-safe and can be used with `#[tokio::test]` multi-threaded
//! test configurations. Internal state is protected by appropriate synchronization
//! primitives.

use freenet_stdlib::prelude::*;
use futures::Future;
use rand::prelude::IndexedRandom;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{Ipv6Addr, SocketAddr},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{broadcast, mpsc, watch, Mutex};
use tracing::info;

#[cfg(feature = "trace-ot")]
use crate::tracing::CombinedRegister;
use crate::{
    client_events::{
        test::{MemoryEventsGen, RandomEventGenerator},
        ClientId,
    },
    config::{ConfigArgs, GlobalExecutor, GlobalRng},
    dev_tool::TransportKeypair,
    node::{InitPeerNode, NetEventRegister, NodeConfig},
    ring::{Distance, Location, PeerKeyLocation},
    simulation::{FaultConfig, VirtualTime},
    tracing::TestEventListener,
    transport::{
        in_memory_socket::{register_network_time_source, unregister_network_time_source},
        TransportPublicKey,
    },
};

mod in_memory;
mod network;
pub mod turmoil_runner;

pub use self::network::{NetworkPeer, PeerMessage, PeerStatus};
pub use self::turmoil_runner::{run_turmoil_simulation, TurmoilConfig, TurmoilResult};

pub(crate) type EventId = u32;

/// A controlled operation to execute on a specific node during simulation.
///
/// `SimOperation` allows tests to specify exact operations instead of relying
/// on random event generation. This enables precise testing of specific scenarios
/// like subscription topology formation.
///
/// # Usage
///
/// ```ignore
/// use freenet::dev_tool::{SimNetwork, SimOperation, NodeLabel};
///
/// let mut sim = SimNetwork::new("test", 1, 3, ...).await;
///
/// // Create a contract and have specific nodes subscribe
/// let contract = SimOperation::create_test_contract(42);
/// let operations = vec![
///     (NodeLabel::gateway("test", 0), SimOperation::Put {
///         contract: contract.clone(),
///         state: vec![1, 2, 3],
///         subscribe: true,
///     }),
///     (NodeLabel::node("test", 0), SimOperation::Subscribe {
///         contract_id: *contract.key().id(),
///     }),
/// ];
///
/// let handles = sim.start_with_controlled_events(operations).await;
/// ```
#[derive(Clone, Debug)]
pub enum SimOperation {
    /// PUT a new contract with initial state.
    Put {
        /// The contract container (code + parameters)
        contract: ContractContainer,
        /// Initial state bytes
        state: Vec<u8>,
        /// Whether to subscribe to updates after PUT
        subscribe: bool,
    },
    /// GET a contract's current state.
    Get {
        /// The contract instance ID to retrieve
        contract_id: ContractInstanceId,
        /// Whether to return the contract code
        return_contract_code: bool,
        /// Whether to subscribe to updates after GET
        subscribe: bool,
    },
    /// SUBSCRIBE to a contract's updates.
    Subscribe {
        /// The contract instance ID to subscribe to
        contract_id: ContractInstanceId,
    },
    /// UPDATE a contract's state.
    Update {
        /// The contract key to update
        key: ContractKey,
        /// New state data
        data: Vec<u8>,
    },
}

impl SimOperation {
    /// Creates a deterministic test contract from a seed.
    ///
    /// The contract code and parameters are derived from the seed,
    /// making it reproducible across test runs.
    pub fn create_test_contract(seed: u8) -> ContractContainer {
        let mut code_bytes = vec![0u8; 32];
        let mut params_bytes = vec![0u8; 16];

        // Fill with deterministic bytes based on seed
        for (i, byte) in code_bytes.iter_mut().enumerate() {
            *byte = seed.wrapping_add(i as u8);
        }
        for (i, byte) in params_bytes.iter_mut().enumerate() {
            *byte = seed.wrapping_add(i as u8).wrapping_mul(2);
        }

        let code = ContractCode::from(code_bytes);
        let params = Parameters::from(params_bytes);
        ContractWasmAPIVersion::V1(WrappedContract::new(code.into(), params)).into()
    }

    /// Creates a deterministic test state from a seed.
    pub fn create_test_state(seed: u8) -> Vec<u8> {
        let mut state_bytes = vec![0u8; 64];
        for (i, byte) in state_bytes.iter_mut().enumerate() {
            *byte = seed.wrapping_add(i as u8).wrapping_mul(3);
        }
        state_bytes
    }

    /// Creates a CRDT-mode test state with version prefix.
    ///
    /// Format: [version: u64 LE][64 bytes of data]
    ///
    /// Use this with `register_crdt_contract()` to test version-aware delta handling.
    /// The CRDT mode enables testing of PR #2763's summary caching fix by:
    /// - Using version-aware summaries (version + hash)
    /// - Computing version-specific deltas
    /// - Failing delta application when versions don't match
    pub fn create_crdt_state(version: u64, seed: u8) -> Vec<u8> {
        // State must be > 80 bytes for delta to be "efficient"
        // (efficiency check: summary_size * 2 < state_size, where summary = 40 bytes)
        // Using 128 bytes of data for total state size of 136 bytes
        let mut state_bytes = Vec::with_capacity(8 + 128);
        // Add version prefix
        state_bytes.extend_from_slice(&version.to_le_bytes());
        // Add data (128 bytes to make delta efficient)
        for i in 0..128u8 {
            state_bytes.push(seed.wrapping_add(i).wrapping_mul(3));
        }
        state_bytes
    }

    /// Converts this operation to a ClientRequest.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn into_client_request(self) -> freenet_stdlib::client_api::ClientRequest<'static> {
        use freenet_stdlib::client_api::{ClientRequest, ContractRequest};

        match self {
            SimOperation::Put {
                contract,
                state,
                subscribe,
            } => ClientRequest::ContractOp(ContractRequest::Put {
                contract,
                state: WrappedState::new(state),
                related_contracts: RelatedContracts::new(),
                subscribe,
            }),
            SimOperation::Get {
                contract_id,
                return_contract_code,
                subscribe,
            } => ClientRequest::ContractOp(ContractRequest::Get {
                key: contract_id,
                return_contract_code,
                subscribe,
            }),
            SimOperation::Subscribe { contract_id } => {
                ClientRequest::ContractOp(ContractRequest::Subscribe {
                    key: contract_id,
                    summary: None,
                })
            }
            SimOperation::Update { key, data } => {
                ClientRequest::ContractOp(ContractRequest::Update {
                    key,
                    data: UpdateData::State(State::from(data)),
                })
            }
        }
    }
}

/// A scheduled operation for controlled event simulation.
#[derive(Clone, Debug)]
pub struct ScheduledOperation {
    /// Which node should execute this operation
    pub node: NodeLabel,
    /// The operation to execute
    pub operation: SimOperation,
}

impl ScheduledOperation {
    /// Create a new scheduled operation.
    pub fn new(node: NodeLabel, operation: SimOperation) -> Self {
        Self { node, operation }
    }
}

/// Result of a controlled simulation, including topology snapshots.
///
/// This struct captures the simulation result along with topology snapshots
/// taken at the end of the simulation (before cleanup). This allows tests
/// to validate subscription topology after the simulation completes.
#[derive(Debug)]
pub struct ControlledSimulationResult {
    /// The Turmoil simulation result
    pub turmoil_result: turmoil::Result,
    /// Topology snapshots captured at the end of simulation
    pub topology_snapshots: Vec<crate::ring::topology_registry::TopologySnapshot>,
}

#[derive(PartialEq, Eq, Hash, Clone, PartialOrd, Ord, Debug)]
pub struct NodeLabel(Arc<str>);

impl NodeLabel {
    /// Creates a gateway label for a network.
    pub fn gateway(network_name: &str, id: usize) -> Self {
        Self(format!("{network_name}-gateway-{id}").into())
    }

    /// Creates a regular node label for a network.
    pub fn node(network_name: &str, id: usize) -> Self {
        Self(format!("{network_name}-node-{id}").into())
    }

    /// Returns true if this is a gateway label.
    pub fn is_gateway(&self) -> bool {
        self.0.contains("-gateway-")
    }

    /// Returns true if this is a regular node label.
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
pub(crate) struct GatewayConfig {
    #[allow(dead_code)]
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

/// A stream of events for simulation testing.
///
/// `EventChain` provides a `Stream` of events that drive user interactions in
/// the simulated network.
///
/// # Ownership and Cleanup
///
/// There are two ways to obtain an `EventChain`:
///
/// 1. **`event_chain(&mut sim)`** - Borrows from `SimNetwork`. The `SimNetwork`
///    retains ownership of labels and handles cleanup on drop. The `EventChain`
///    is created with `clean_up_tmp_dirs = false`.
///
/// 2. **`into_event_chain(sim)`** - Consumes `SimNetwork`. The `EventChain` takes
///    ownership of labels and handles cleanup on drop. The `EventChain` is created
///    with `clean_up_tmp_dirs` set to whatever `SimNetwork` had.
///
/// This design allows tests to either retain access to `SimNetwork` for verification
/// methods, or to release it for simpler cleanup.
pub struct EventChain<S = watch::Sender<(EventId, TransportPublicKey)>> {
    labels: Vec<(NodeLabel, TransportPublicKey)>,
    user_ev_controller: S,
    total_events: u32,
    count: u32,
    rng: rand::rngs::SmallRng,
    /// Whether this EventChain is responsible for cleaning up temp directories.
    /// Set to `true` when created via `into_event_chain()` (consuming SimNetwork),
    /// `false` when created via `event_chain()` (borrowing from SimNetwork).
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

/// A controlled event chain that triggers events in a specific sequence.
///
/// Unlike `EventChain` which randomly selects peers, `ControlledEventChain`
/// triggers events in the exact order specified, allowing deterministic testing.
pub struct ControlledEventChain {
    user_ev_controller: watch::Sender<(EventId, TransportPublicKey)>,
    /// Sequence of (EventId, NodeLabel) to trigger, in order
    event_sequence: Vec<(EventId, NodeLabel)>,
    /// Map from NodeLabel to TransportPublicKey
    label_to_key: HashMap<NodeLabel, TransportPublicKey>,
    /// Current position in the event sequence
    current_index: usize,
}

impl ControlledEventChain {
    /// Create a new controlled event chain.
    pub fn new(
        user_ev_controller: watch::Sender<(EventId, TransportPublicKey)>,
        event_sequence: Vec<(EventId, NodeLabel)>,
        label_to_key: HashMap<NodeLabel, TransportPublicKey>,
    ) -> Self {
        Self {
            user_ev_controller,
            event_sequence,
            label_to_key,
            current_index: 0,
        }
    }

    /// Trigger the next event in the sequence.
    ///
    /// Returns the EventId that was triggered, or None if all events have been triggered.
    pub fn trigger_next(&mut self) -> Option<EventId> {
        if self.current_index >= self.event_sequence.len() {
            return None;
        }

        let (event_id, label) = &self.event_sequence[self.current_index];
        let key = self
            .label_to_key
            .get(label)
            .expect("Label should exist in mapping");

        match self.user_ev_controller.send((*event_id, key.clone())) {
            Ok(()) => {
                self.current_index += 1;
                Some(*event_id)
            }
            Err(e) => {
                tracing::error!("Failed to send event {}: {:?}", event_id, e);
                None
            }
        }
    }

    /// Trigger all remaining events in sequence.
    ///
    /// Returns the number of events triggered.
    pub fn trigger_all(&mut self) -> usize {
        let mut count = 0;
        while self.trigger_next().is_some() {
            count += 1;
        }
        count
    }

    /// Returns true if all events have been triggered.
    pub fn is_complete(&self) -> bool {
        self.current_index >= self.event_sequence.len()
    }

    /// Returns the number of events remaining.
    pub fn remaining(&self) -> usize {
        self.event_sequence.len().saturating_sub(self.current_index)
    }
}

impl futures::stream::Stream for ControlledEventChain {
    type Item = EventId;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.trigger_next() {
            Some(event_id) => std::task::Poll::Ready(Some(event_id)),
            None => std::task::Poll::Ready(None),
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
    event_register: ER,
    contracts: Vec<(ContractContainer, WrappedState, bool)>,
    contract_subscribers: HashMap<ContractKey, Vec<PeerKeyLocation>>,
    /// Seed for deterministic RNG in this node's transport layer
    pub rng_seed: u64,
    /// Network name for scoped fault injection
    pub network_name: String,
}

impl<ER: NetEventRegister> Builder<ER> {
    /// Builds an in-memory node. Does nothing upon construction.
    pub fn build(
        builder: NodeConfig,
        event_register: ER,
        contract_handler_name: String,
        rng_seed: u64,
        network_name: String,
    ) -> Builder<ER> {
        Builder {
            config: builder.clone(),
            contract_handler_name,
            event_register,
            contracts: Vec::new(),
            contract_subscribers: HashMap::new(),
            rng_seed,
            network_name,
        }
    }
}

/// Information about a running node, used for crash/restart operations.
#[derive(Debug)]
pub struct RunningNode {
    /// The label identifying this node
    pub label: NodeLabel,
    /// Socket address for fault injection
    pub addr: SocketAddr,
    /// Handle to abort the running task (AbortHandle can be cloned and used independently)
    pub abort_handle: tokio::task::AbortHandle,
}

/// Configuration saved for node restart.
///
/// When a node is started, its configuration is saved here so it can be
/// restarted with the same identity (keypair), location, and data directory.
#[derive(Clone)]
pub struct RestartableNodeConfig {
    /// The node's configuration (contains keypair, location, data dir, etc.)
    pub config: NodeConfig,
    /// The node label (reserved for future use in restart scenarios)
    #[allow(dead_code)]
    pub label: NodeLabel,
    /// Whether this is a gateway node
    pub is_gateway: bool,
    /// Gateway addresses to connect to (for non-gateway nodes)
    #[allow(dead_code)]
    pub gateway_configs: Vec<GatewayConfig>,
    /// Seed for deterministic RNG in this node's transport layer
    pub rng_seed: u64,
    /// Shared in-memory storage for contract state (persists across restarts)
    pub shared_storage: crate::wasm_runtime::MockStateStorage,
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
    /// Master seed for deterministic RNG - used to derive per-peer seeds
    seed: u64,
    /// VirtualTime for deterministic simulation - always enabled
    virtual_time: VirtualTime,
    /// Running nodes indexed by label for crash/restart operations
    running_nodes: HashMap<NodeLabel, RunningNode>,
    /// Map from label to socket address for quick lookup
    node_addresses: HashMap<NodeLabel, SocketAddr>,
    /// Saved configurations for node restart (preserved after crash)
    restartable_configs: HashMap<NodeLabel, RestartableNodeConfig>,
    /// All gateway configs (needed for restarting non-gateway nodes)
    all_gateway_configs: Vec<GatewayConfig>,
    /// Controls how MemoryEventsGen handles subscription notifications
    subscription_notification_mode: crate::client_events::test::SubscriptionNotificationMode,
    /// Shared collection of subscription notifications from all MemoryEventsGen instances
    subscription_notifications: Arc<
        Mutex<
            Vec<(
                ClientId,
                freenet_stdlib::prelude::ContractInstanceId,
                crate::client_events::HostResult,
            )>,
        >,
    >,
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

        // Seed GlobalRng for deterministic location generation
        // This ensures Location::random() calls in config_gateways/config_nodes are deterministic
        GlobalRng::set_seed(seed);

        let (user_ev_controller, mut receiver_ch) =
            watch::channel((0, TransportKeypair::new().public().clone()));
        receiver_ch.borrow_and_update();

        // VirtualTime is always enabled for deterministic simulation
        let virtual_time = VirtualTime::new();

        // Register the VirtualTime for this network so SimulationSocket can use it
        register_network_time_source(name, virtual_time.clone());

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
            seed,
            virtual_time,
            running_nodes: HashMap::new(),
            node_addresses: HashMap::new(),
            restartable_configs: HashMap::new(),
            all_gateway_configs: Vec::new(),
            subscription_notification_mode:
                crate::client_events::test::SubscriptionNotificationMode::default(),
            subscription_notifications: Arc::new(Mutex::new(Vec::new())),
        };
        net.config_gateways(
            gateways
                .try_into()
                .expect("should have at least one gateway"),
        )
        .await;
        net.config_nodes(nodes).await;

        // Auto-configure fault injection with VirtualTime enabled
        // Users can call with_fault_injection() to customize
        net.init_default_fault_injection();

        net
    }

    /// Initializes the default fault injection with VirtualTime but no faults.
    /// Users can call with_fault_injection() to add message loss, latency, etc.
    fn init_default_fault_injection(&mut self) {
        use crate::node::network_bridge::{set_fault_injector, FaultInjectorState};
        let fault_seed = self.seed.wrapping_add(0xFA01_7777);
        let state = FaultInjectorState::new(FaultConfig::default(), fault_seed)
            .with_virtual_time(self.virtual_time.clone());
        set_fault_injector(
            &self.name,
            Some(std::sync::Arc::new(std::sync::Mutex::new(state))),
        );
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

    /// Set the subscription notification mode for MemoryEventsGen instances.
    /// Default is Disabled (for CRDT convergence tests).
    /// Set to Collect for subscription renewal tests that need to inspect Subscribe events.
    pub fn with_subscription_mode(
        &mut self,
        mode: crate::client_events::test::SubscriptionNotificationMode,
    ) {
        self.subscription_notification_mode = mode;
    }

    /// Get a handle to the subscription notifications collection.
    /// Use this after a simulation completes to inspect collected Subscribe events.
    pub fn subscription_notifications_handle(
        &self,
    ) -> Arc<
        Mutex<
            Vec<(
                ClientId,
                freenet_stdlib::prelude::ContractInstanceId,
                crate::client_events::HostResult,
            )>,
        >,
    > {
        self.subscription_notifications.clone()
    }

    /// Returns the VirtualTime instance for this simulation.
    ///
    /// VirtualTime is always enabled. Use this to advance time and control
    /// message delivery timing.
    ///
    /// # Example
    /// ```ignore
    /// let mut sim = SimNetwork::new(...).await;
    ///
    /// // Advance virtual time by 100ms
    /// sim.virtual_time().advance(Duration::from_millis(100));
    ///
    /// // Deliver pending messages
    /// let delivered = sim.advance_virtual_time();
    /// ```
    pub fn virtual_time(&self) -> &VirtualTime {
        &self.virtual_time
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
    /// VirtualTime is automatically used for deterministic latency injection.
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
    ///
    /// // Advance time and deliver pending messages
    /// sim.virtual_time().advance(Duration::from_millis(100));
    /// sim.advance_virtual_time();
    /// ```
    pub fn with_fault_injection(&mut self, config: FaultConfig) {
        use crate::node::network_bridge::{set_fault_injector, FaultInjectorState};
        // Use a derived seed for fault injection to maintain determinism
        let fault_seed = self.seed.wrapping_add(0xFA01_7777);
        // Always use VirtualTime for deterministic behavior
        let state = FaultInjectorState::new(config, fault_seed)
            .with_virtual_time(self.virtual_time.clone());
        set_fault_injector(
            &self.name,
            Some(std::sync::Arc::new(std::sync::Mutex::new(state))),
        );
    }

    /// Clears any configured fault injection and resets to default (VirtualTime only).
    pub fn clear_fault_injection(&mut self) {
        self.init_default_fault_injection();
    }

    /// Configures fault injection with a custom VirtualTime instance.
    ///
    /// This is useful if you want to use a shared VirtualTime across multiple
    /// simulations or have more control over time advancement.
    ///
    /// For most cases, use [`with_fault_injection`](Self::with_fault_injection) instead,
    /// which uses the built-in VirtualTime.
    #[deprecated(
        since = "0.1.0",
        note = "VirtualTime is now always enabled. Use with_fault_injection() and virtual_time() instead."
    )]
    pub fn with_fault_injection_virtual_time(
        &mut self,
        config: FaultConfig,
        virtual_time: VirtualTime,
    ) {
        use crate::node::network_bridge::{set_fault_injector, FaultInjectorState};
        let fault_seed = self.seed.wrapping_add(0xFA01_7777);
        let state = FaultInjectorState::new(config, fault_seed).with_virtual_time(virtual_time);
        set_fault_injector(
            &self.name,
            Some(std::sync::Arc::new(std::sync::Mutex::new(state))),
        );
    }

    /// Advances virtual time and delivers pending messages.
    ///
    /// This should be called after advancing the VirtualTime instance to deliver
    /// messages whose deadlines have passed.
    ///
    /// Returns the number of messages delivered.
    pub fn advance_virtual_time(&mut self) -> usize {
        use crate::node::network_bridge::get_fault_injector;
        if let Some(injector) = get_fault_injector(&self.name) {
            let mut state = injector.lock().unwrap();
            state.advance_time()
        } else {
            0
        }
    }

    /// Advances virtual time by the given duration and delivers pending messages.
    ///
    /// This is a convenience method combining `virtual_time().advance()` and
    /// `advance_virtual_time()`.
    ///
    /// Returns the number of messages delivered.
    pub fn advance_time(&mut self, duration: Duration) -> usize {
        self.virtual_time.advance(duration);
        self.advance_virtual_time()
    }

    /// Returns the current network statistics from fault injection.
    ///
    /// These statistics track:
    /// - Messages sent, delivered, dropped
    /// - Drop reasons (loss rate, partition, crash)
    /// - Latency injection metrics
    pub fn get_network_stats(&self) -> Option<crate::node::network_bridge::NetworkStats> {
        use crate::node::network_bridge::get_fault_injector;
        get_fault_injector(&self.name).map(|injector| {
            let state = injector.lock().unwrap();
            state.stats().clone()
        })
    }

    /// Resets the network statistics.
    pub fn reset_network_stats(&mut self) {
        use crate::node::network_bridge::get_fault_injector;
        if let Some(injector) = get_fault_injector(&self.name) {
            let mut state = injector.lock().unwrap();
            state.reset_stats();
        }
    }

    // =========================================================================
    // Node Lifecycle Management (Crash/Restart)
    // =========================================================================

    /// Crashes a node, stopping its execution and blocking all messages to/from it.
    ///
    /// The node's task is aborted and all messages to/from its address are dropped.
    /// In-flight operations will fail. Use [`restart_node`](Self::restart_node) to
    /// bring the node back (note: restart is not yet implemented).
    ///
    /// # Example
    /// ```ignore
    /// let mut sim = SimNetwork::new(...).await;
    /// let handles = sim.start_with_rand_gen::<SmallRng>(seed, 10, 5).await;
    ///
    /// // Crash node "node-0"
    /// sim.crash_node(&NodeLabel::node(0));
    ///
    /// // Messages to/from this node will be dropped
    /// // Other nodes should handle the failure gracefully
    /// ```
    pub fn crash_node(&mut self, label: &NodeLabel) -> bool {
        use crate::node::network_bridge::get_fault_injector;

        // Get the node's address
        let addr = match self.node_addresses.get(label) {
            Some(addr) => *addr,
            None => {
                tracing::warn!(?label, "Cannot crash node: address not found");
                return false;
            }
        };

        // Mark as crashed in fault injector
        if let Some(injector) = get_fault_injector(&self.name) {
            let mut state = injector.lock().unwrap();
            state.config.crash_node(addr);
            tracing::info!(?label, ?addr, "Node marked as crashed in fault injector");
        }

        // Abort the running task if we have a handle
        if let Some(running) = self.running_nodes.remove(label) {
            running.abort_handle.abort();
            tracing::info!(?label, "Node task aborted");
            true
        } else {
            tracing::warn!(
                ?label,
                "Node not found in running_nodes (may not be started yet)"
            );
            false
        }
    }

    /// Recovers a crashed node, allowing messages to flow again.
    ///
    /// This removes the node from the crashed set, but does NOT restart the node's
    /// execution. The node will not process messages, but other nodes can attempt
    /// to reach it (messages won't be dropped due to crash status).
    ///
    /// For full restart, see the roadmap in `docs/architecture/simulation-testing-design.md`.
    pub fn recover_node(&mut self, label: &NodeLabel) -> bool {
        use crate::node::network_bridge::get_fault_injector;

        let addr = match self.node_addresses.get(label) {
            Some(addr) => addr,
            None => {
                tracing::warn!(?label, "Cannot recover node: address not found");
                return false;
            }
        };

        if let Some(injector) = get_fault_injector(&self.name) {
            let mut state = injector.lock().unwrap();
            state.config.recover_node(addr);
            tracing::info!(
                ?label,
                ?addr,
                "Node recovered (no longer marked as crashed)"
            );
            true
        } else {
            false
        }
    }

    /// Checks if a node is currently marked as crashed.
    pub fn is_node_crashed(&self, label: &NodeLabel) -> bool {
        use crate::node::network_bridge::get_fault_injector;

        let addr = match self.node_addresses.get(label) {
            Some(addr) => addr,
            None => return false,
        };

        if let Some(injector) = get_fault_injector(&self.name) {
            let state = injector.lock().unwrap();
            state.config.is_crashed(addr)
        } else {
            false
        }
    }

    /// Returns the socket address for a node label, if known.
    pub fn node_address(&self, label: &NodeLabel) -> Option<SocketAddr> {
        self.node_addresses.get(label).copied()
    }

    /// Returns all node labels and their addresses.
    pub fn all_node_addresses(&self) -> &HashMap<NodeLabel, SocketAddr> {
        &self.node_addresses
    }

    /// Restarts a crashed node, preserving its identity.
    ///
    /// This method:
    /// 1. Retrieves the saved configuration (keypair, data directory, location)
    /// 2. Unregisters the old peer from the transport layer
    /// 3. Creates a new node with the same identity
    /// 4. Starts the node task
    ///
    /// # What's Preserved
    /// - **Keypair/identity**: Same public key and address
    /// - **Data directory path**: Same location for any disk-based storage
    /// - **Network location**: Same ring location
    /// - **Gateway configs**: Same gateway connections
    ///
    /// # What's NOT Preserved (Current Limitation)
    /// - **In-memory state**: Memory caches are lost on crash
    /// - **In-flight transactions**: Any pending operations are lost
    /// - **Contract state**: Currently stored on disk (SQLite), which may or may not
    ///   survive the abrupt task abort depending on flush timing
    ///
    /// # Future Work
    /// For truly deterministic state persistence, we need to implement shared
    /// in-memory storage using `MockStateStorage` instead of SQLite. This would
    /// require making `Executor` generic over the storage type.
    ///
    /// # Arguments
    /// * `label` - The label of the node to restart
    /// * `seed` - Seed for random event generation
    /// * `max_contract_num` - Maximum number of contracts for event generation
    /// * `iterations` - Number of iterations for event generation
    ///
    /// # Returns
    /// * `Some(JoinHandle)` if restart was successful
    /// * `None` if the node config was not found or restart failed
    ///
    /// # Example
    /// ```ignore
    /// // Crash a node
    /// sim.crash_node(&label);
    /// assert!(sim.is_node_crashed(&label));
    ///
    /// // Restart it with same identity
    /// let handle = sim.restart_node::<SmallRng>(&label, 0x5678, 10, 5).await;
    /// assert!(handle.is_some());
    /// assert!(!sim.is_node_crashed(&label));
    /// ```
    pub async fn restart_node<R>(
        &mut self,
        label: &NodeLabel,
        seed: u64,
        max_contract_num: usize,
        iterations: usize,
    ) -> Option<tokio::task::JoinHandle<anyhow::Result<()>>>
    where
        R: crate::client_events::test::RandomEventGenerator + Send + 'static,
    {
        use crate::node::network_bridge::get_fault_injector;
        use crate::transport::in_memory_socket::unregister_socket;

        // Get the saved restartable config
        let restart_config = match self.restartable_configs.get(label) {
            Some(config) => config.clone(),
            None => {
                tracing::warn!(?label, "Cannot restart node: config not found");
                return None;
            }
        };

        // Get the node address
        let node_addr = match self.node_addresses.get(label) {
            Some(addr) => *addr,
            None => {
                tracing::warn!(?label, "Cannot restart node: address not found");
                return None;
            }
        };

        tracing::info!(?label, ?node_addr, "Restarting node with persisted state");

        // Unregister the old socket from transport (the new node will re-register)
        unregister_socket(&self.name, &node_addr);

        // Clear crash status in fault injector
        if let Some(injector) = get_fault_injector(&self.name) {
            let mut state = injector.lock().unwrap();
            state.config.recover_node(&node_addr);
        }

        // Create a new Builder with the saved configuration
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

        let builder = Builder::build(
            restart_config.config.clone(),
            event_listener,
            format!("{}-{}", self.name, label),
            restart_config.rng_seed,
            self.name.clone(),
        );

        // Calculate total peers for event generation params
        let total_peer_num = self.labels.len();

        // Create user events for the restarted node
        let mut user_events = MemoryEventsGen::<R>::new_with_seed(
            self.receiver_ch.clone(),
            restart_config.config.key_pair.public().clone(),
            seed,
        );
        user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);

        // Create the appropriate span
        let span = if restart_config.is_gateway {
            tracing::info_span!("in_mem_gateway_restart", %label)
        } else {
            tracing::info_span!("in_mem_node_restart", %label)
        };

        // Start the node task with shared storage for state persistence
        let shared_storage = restart_config.shared_storage.clone();
        let node_task = async move {
            builder
                .run_node_with_shared_storage(user_events, span, shared_storage)
                .await
        };
        let handle = GlobalExecutor::spawn(node_task);

        // Track the new running node
        self.running_nodes.insert(
            label.clone(),
            RunningNode {
                label: label.clone(),
                addr: node_addr,
                abort_handle: handle.abort_handle(),
            },
        );

        tracing::info!(?label, "Node restarted successfully with persisted state");
        Some(handle)
    }

    /// Checks if a node has a saved configuration for restart.
    pub fn can_restart(&self, label: &NodeLabel) -> bool {
        self.restartable_configs.contains_key(label)
    }

    #[allow(unused)]
    pub fn debug(&mut self) {
        self.clean_up_tmp_dirs = false;
    }

    /// Derives a deterministic port from seed and peer index for simulation.
    /// Uses ports in the dynamic range (49152-65535) to avoid conflicts.
    fn derive_deterministic_port(&self, peer_index: usize) -> u16 {
        const BASE_PORT: u16 = 50000;
        const PORT_RANGE: u16 = 10000;
        // Use peer seed to get a deterministic offset
        let peer_seed = self.derive_peer_seed(peer_index);
        BASE_PORT + ((peer_seed % PORT_RANGE as u64) as u16)
    }

    async fn config_gateways(&mut self, num: NonZeroUsize) {
        info!("Building {} gateways", num);
        let mut configs = Vec::with_capacity(num.into());
        for node_no in 0..num.into() {
            let label = NodeLabel::gateway(&self.name, node_no);
            // Use deterministic port for simulation instead of querying system
            let port = self.derive_deterministic_port(node_no);
            let keypair = crate::transport::TransportKeypair::new();
            let addr: SocketAddr = (Ipv6Addr::LOCALHOST, port).into();
            let peer_key_location = PeerKeyLocation::new(keypair.public().clone(), addr);
            // Use location computed from address for consistency with PeerKeyLocation::location()
            let location = Location::from_address(&addr);

            // Track address for crash/restart operations
            self.node_addresses.insert(label.clone(), addr);

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
        // All gateways are passive - they don't initiate connections to other gateways.
        // This ensures deterministic startup order where only regular nodes initiate connections.
        for config in &mut configs {
            config.0.should_connect = false;
        }

        let gateways: Vec<_> = configs.iter().map(|(_, gw)| gw.clone()).collect();
        // Store all gateway configs for use when restarting non-gateway nodes
        self.all_gateway_configs = gateways.clone();

        // Note: Gateways don't need to know about each other - they are passive entry points.
        // Only regular nodes need to know about gateways. This simplifies the topology and
        // improves determinism by avoiding gateway-to-gateway connection attempts.
        for (this_node, this_config) in configs {
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
                peer_seed,
                self.name.clone(),
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
            let port = self.derive_deterministic_port(node_no);
            let addr: SocketAddr = (Ipv6Addr::LOCALHOST, port).into();
            // Use location computed from address for consistency with PeerKeyLocation::location().
            // This ensures the stored location matches what other peers compute when looking at our address.
            let location = Location::from_address(&addr);
            config.network_listener_port = port;
            config.network_listener_ip = Ipv6Addr::LOCALHOST.into();
            config.key_pair = crate::transport::TransportKeypair::new();
            config.with_own_addr(addr);
            config
                .with_location(location)
                .max_hops_to_live(self.ring_max_htl)
                .rnd_if_htl_above(self.rnd_if_htl_above)
                .max_number_of_connections(self.max_connections);

            // Track address for crash/restart operations
            self.node_addresses.insert(label.clone(), addr);

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
                peer_seed,
                self.name.clone(),
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
        use crate::ring::topology_registry::set_current_network_name;
        use crate::transport::in_memory_socket::is_socket_registered;

        // Set the current network name so Ring can register topology snapshots
        set_current_network_name(&self.name);

        let total_peer_num = self.gateways.len() + self.nodes.len();
        let mut peers = vec![];

        // Phase 1: Start all gateways first and collect their addresses
        let gateways: Vec<_> = self.gateways.drain(..).collect();
        let mut gateway_addrs = Vec::with_capacity(gateways.len());

        for (node, config) in gateways {
            let label = config.label.clone();
            // Use the peer_key_location address from GatewayConfig - this is the address
            // that will be registered in the peer registry
            let gateway_addr = *config
                .peer_key_location
                .peer_addr
                .as_known()
                .expect("Gateway should have known address");
            gateway_addrs.push(gateway_addr);

            tracing::debug!(peer = %label, addr = %gateway_addr, "starting gateway");

            // Create shared in-memory storage for this node (persists across restarts)
            let shared_storage = crate::wasm_runtime::MockStateStorage::new();

            // Save restartable config BEFORE starting (NodeConfig gets consumed)
            self.restartable_configs.insert(
                label.clone(),
                RestartableNodeConfig {
                    config: node.config.clone(),
                    label: label.clone(),
                    is_gateway: true,
                    gateway_configs: self.all_gateway_configs.clone(),
                    rng_seed: node.rng_seed,
                    shared_storage: shared_storage.clone(),
                },
            );

            let mut user_events = MemoryEventsGen::<R>::new_with_seed(
                self.receiver_ch.clone(),
                node.config.key_pair.public().clone(),
                seed,
            );
            user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);
            let span = tracing::info_span!("in_mem_gateway", %label);
            self.labels
                .push((label.clone(), node.config.key_pair.public().clone()));

            // Use shared in-memory storage for state persistence across restarts
            let node_task = async move {
                node.run_node_with_shared_storage(user_events, span, shared_storage)
                    .await
            };
            let handle = GlobalExecutor::spawn(node_task);

            // Track running node for crash/restart
            self.running_nodes.insert(
                label.clone(),
                RunningNode {
                    label: label.clone(),
                    addr: gateway_addr,
                    abort_handle: handle.abort_handle(),
                },
            );

            peers.push(handle);

            tokio::time::sleep(self.start_backoff).await;
        }

        // Phase 2: Wait for all gateways to be registered in the peer registry
        // This prevents the race condition where regular nodes try to connect
        // before gateways are ready to receive connections
        let registration_timeout = Duration::from_secs(10);
        let poll_interval = Duration::from_millis(10);
        // Use tokio::time::Instant instead of std::time::Instant to work correctly
        // with start_paused = true in tests. std::time::Instant uses wall-clock time
        // which doesn't advance when tokio's time is paused.
        let start = tokio::time::Instant::now();

        'wait_loop: loop {
            let mut all_registered = true;
            for addr in &gateway_addrs {
                if !is_socket_registered(&self.name, addr) {
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
                    gateway_addrs
                        .iter()
                        .filter(|a| is_socket_registered(&self.name, a))
                        .count(),
                    gateway_addrs.len()
                );
                break 'wait_loop;
            }

            tokio::time::sleep(poll_interval).await;
        }

        // Phase 3: Start all regular nodes
        let nodes: Vec<_> = self.nodes.drain(..).collect();
        for (node, label) in nodes {
            // Get node address from tracked addresses
            let node_addr = self
                .node_addresses
                .get(&label)
                .copied()
                .expect("Node address should be tracked");

            tracing::debug!(peer = %label, addr = %node_addr, "starting regular node");

            // Create shared in-memory storage for this node (persists across restarts)
            let shared_storage = crate::wasm_runtime::MockStateStorage::new();

            // Save restartable config BEFORE starting (NodeConfig gets consumed)
            self.restartable_configs.insert(
                label.clone(),
                RestartableNodeConfig {
                    config: node.config.clone(),
                    label: label.clone(),
                    is_gateway: false,
                    gateway_configs: self.all_gateway_configs.clone(),
                    rng_seed: node.rng_seed,
                    shared_storage: shared_storage.clone(),
                },
            );

            let mut user_events = MemoryEventsGen::<R>::new_with_seed(
                self.receiver_ch.clone(),
                node.config.key_pair.public().clone(),
                seed,
            );
            user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);
            let span = tracing::info_span!("in_mem_node", %label);
            self.labels
                .push((label.clone(), node.config.key_pair.public().clone()));

            // Use shared in-memory storage for state persistence across restarts
            let node_task = async move {
                node.run_node_with_shared_storage(user_events, span, shared_storage)
                    .await
            };
            let handle = GlobalExecutor::spawn(node_task);

            // Track running node for crash/restart
            self.running_nodes.insert(
                label.clone(),
                RunningNode {
                    label: label.clone(),
                    addr: node_addr,
                    abort_handle: handle.abort_handle(),
                },
            );

            peers.push(handle);

            tokio::time::sleep(self.start_backoff).await;
        }

        self.labels.sort_by(|(a, _), (b, _)| a.cmp(b));
        peers
    }

    /// Starts the network with controlled events instead of random event generation.
    ///
    /// This method allows tests to specify exact operations to execute on specific nodes,
    /// enabling precise testing of scenarios like subscription topology formation.
    ///
    /// # Arguments
    ///
    /// * `operations` - A list of `(NodeLabel, SimOperation)` pairs specifying which
    ///   operations to execute on which nodes.
    ///
    /// # Returns
    ///
    /// A vector of join handles for the node tasks and the number of operations scheduled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let contract = SimOperation::create_test_contract(42);
    /// let operations = vec![
    ///     ScheduledOperation::new(
    ///         NodeLabel::gateway("test", 0),
    ///         SimOperation::Put {
    ///             contract: contract.clone(),
    ///             state: vec![1, 2, 3],
    ///             subscribe: true,
    ///         }
    ///     ),
    ///     ScheduledOperation::new(
    ///         NodeLabel::node("test", 0),
    ///         SimOperation::Subscribe {
    ///             contract_id: *contract.key().id(),
    ///         }
    ///     ),
    /// ];
    ///
    /// let (handles, num_ops) = sim.start_with_controlled_events(operations).await;
    /// ```
    #[cfg(any(test, feature = "testing"))]
    pub async fn start_with_controlled_events(
        &mut self,
        operations: Vec<ScheduledOperation>,
    ) -> (Vec<tokio::task::JoinHandle<anyhow::Result<()>>>, usize) {
        use crate::ring::topology_registry::set_current_network_name;
        use crate::transport::in_memory_socket::is_socket_registered;

        // Set the current network name so Ring can register topology snapshots
        set_current_network_name(&self.name);

        let num_operations = operations.len();
        let mut peers = vec![];

        // Build a map of label -> list of (event_id, operation)
        let mut operations_by_node: HashMap<NodeLabel, Vec<(EventId, SimOperation)>> =
            HashMap::new();
        for (event_id, scheduled_op) in operations.into_iter().enumerate() {
            operations_by_node
                .entry(scheduled_op.node)
                .or_default()
                .push((event_id as EventId, scheduled_op.operation));
        }

        // Phase 1: Start all gateways first and collect their addresses
        let gateways: Vec<_> = self.gateways.drain(..).collect();
        let mut gateway_addrs = Vec::with_capacity(gateways.len());

        for (node, config) in gateways {
            let label = config.label.clone();
            let gateway_addr = *config
                .peer_key_location
                .peer_addr
                .as_known()
                .expect("Gateway should have known address");
            gateway_addrs.push(gateway_addr);

            tracing::debug!(peer = %label, addr = %gateway_addr, "starting gateway with controlled events");

            // Create shared in-memory storage for this node (persists across restarts)
            let shared_storage = crate::wasm_runtime::MockStateStorage::new();

            // Save restartable config BEFORE starting (NodeConfig gets consumed)
            self.restartable_configs.insert(
                label.clone(),
                RestartableNodeConfig {
                    config: node.config.clone(),
                    label: label.clone(),
                    is_gateway: true,
                    gateway_configs: self.all_gateway_configs.clone(),
                    rng_seed: node.rng_seed,
                    shared_storage: shared_storage.clone(),
                },
            );

            // Create MemoryEventsGen without RNG (deterministic mode)
            let mut user_events = MemoryEventsGen::new(
                self.receiver_ch.clone(),
                node.config.key_pair.public().clone(),
            )
            .with_subscription_mode(self.subscription_notification_mode)
            .with_shared_notification_collection(self.subscription_notifications.clone());

            // Populate events for this node
            if let Some(node_ops) = operations_by_node.remove(&label) {
                let events: Vec<_> = node_ops
                    .into_iter()
                    .map(|(id, op)| (id, op.into_client_request()))
                    .collect();
                user_events.generate_events(events);
            }

            let span = tracing::info_span!("in_mem_gateway_controlled", %label);
            self.labels
                .push((label.clone(), node.config.key_pair.public().clone()));

            // Use shared in-memory storage for state persistence across restarts
            let node_task = async move {
                node.run_node_with_shared_storage(user_events, span, shared_storage)
                    .await
            };
            let handle = GlobalExecutor::spawn(node_task);

            // Track running node for crash/restart
            self.running_nodes.insert(
                label.clone(),
                RunningNode {
                    label: label.clone(),
                    addr: gateway_addr,
                    abort_handle: handle.abort_handle(),
                },
            );

            peers.push(handle);

            tokio::time::sleep(self.start_backoff).await;
        }

        // Phase 2: Wait for all gateways to be registered
        let registration_timeout = Duration::from_secs(10);
        let poll_interval = Duration::from_millis(10);
        let start = tokio::time::Instant::now();

        'wait_loop: loop {
            let mut all_registered = true;
            for addr in &gateway_addrs {
                if !is_socket_registered(&self.name, addr) {
                    all_registered = false;
                    break;
                }
            }

            if all_registered {
                tracing::debug!(
                    "All {} gateways registered in {:?}",
                    gateway_addrs.len(),
                    start.elapsed()
                );
                break 'wait_loop;
            }

            if start.elapsed() > registration_timeout {
                tracing::warn!(
                    "Timeout waiting for gateway registration. {} of {} registered.",
                    gateway_addrs
                        .iter()
                        .filter(|a| is_socket_registered(&self.name, a))
                        .count(),
                    gateway_addrs.len()
                );
                break 'wait_loop;
            }

            tokio::time::sleep(poll_interval).await;
        }

        // Phase 3: Start regular nodes
        for (node, label) in std::mem::take(&mut self.nodes) {
            let node_addr = *self
                .node_addresses
                .get(&label)
                .expect("Node address should be tracked");

            tracing::debug!(peer = %label, addr = %node_addr, "starting regular node with controlled events");

            // Create shared in-memory storage for this node (persists across restarts)
            let shared_storage = crate::wasm_runtime::MockStateStorage::new();

            // Save restartable config BEFORE starting (NodeConfig gets consumed)
            self.restartable_configs.insert(
                label.clone(),
                RestartableNodeConfig {
                    config: node.config.clone(),
                    label: label.clone(),
                    is_gateway: false,
                    gateway_configs: self.all_gateway_configs.clone(),
                    rng_seed: node.rng_seed,
                    shared_storage: shared_storage.clone(),
                },
            );

            // Create MemoryEventsGen without RNG (deterministic mode)
            let mut user_events = MemoryEventsGen::new(
                self.receiver_ch.clone(),
                node.config.key_pair.public().clone(),
            )
            .with_subscription_mode(self.subscription_notification_mode)
            .with_shared_notification_collection(self.subscription_notifications.clone());

            // Populate events for this node
            if let Some(node_ops) = operations_by_node.remove(&label) {
                let events: Vec<_> = node_ops
                    .into_iter()
                    .map(|(id, op)| (id, op.into_client_request()))
                    .collect();
                user_events.generate_events(events);
            }

            let span = tracing::info_span!("in_mem_node_controlled", %label);
            self.labels
                .push((label.clone(), node.config.key_pair.public().clone()));

            // Use shared in-memory storage for state persistence across restarts
            let node_task = async move {
                node.run_node_with_shared_storage(user_events, span, shared_storage)
                    .await
            };
            let handle = GlobalExecutor::spawn(node_task);

            // Track running node for crash/restart
            self.running_nodes.insert(
                label.clone(),
                RunningNode {
                    label: label.clone(),
                    addr: node_addr,
                    abort_handle: handle.abort_handle(),
                },
            );

            peers.push(handle);

            tokio::time::sleep(self.start_backoff).await;
        }

        // Warn if there were operations for unknown nodes
        if !operations_by_node.is_empty() {
            let unknown_nodes: Vec<_> = operations_by_node.keys().collect();
            tracing::warn!(
                "Operations scheduled for unknown nodes: {:?}",
                unknown_nodes
            );
        }

        self.labels.sort_by(|(a, _), (b, _)| a.cmp(b));
        (peers, num_operations)
    }

    /// Creates an event chain for controlled events.
    ///
    /// Unlike the standard `event_chain()`, this allows specifying the exact
    /// sequence of events to trigger, enabling deterministic testing.
    ///
    /// # Arguments
    ///
    /// * `event_sequence` - Pairs of `(EventId, NodeLabel)` specifying which events
    ///   to trigger on which nodes, in order.
    ///
    /// # Returns
    ///
    /// A ControlledEventChain that can be used as a Stream.
    pub fn controlled_event_chain(
        &mut self,
        event_sequence: Vec<(EventId, NodeLabel)>,
    ) -> ControlledEventChain {
        let user_ev_controller = self
            .user_ev_controller
            .take()
            .expect("controller should be set");
        let label_to_key: HashMap<_, _> = self.labels.iter().cloned().collect();
        ControlledEventChain::new(user_ev_controller, event_sequence, label_to_key)
    }

    /// Returns the locations of all peers (gateways + nodes) without consuming them.
    pub fn get_peer_locations(&self) -> Vec<f64> {
        let mut locations = Vec::new();
        for (builder, _) in &self.gateways {
            if let Some(loc) = &builder.config.location {
                locations.push(loc.as_f64());
            }
        }
        for (builder, _) in &self.nodes {
            if let Some(loc) = &builder.config.location {
                locations.push(loc.as_f64());
            }
        }
        locations
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
    /// This method borrows the SimNetwork, allowing you to call verification methods
    /// (like `check_convergence()`, `get_operation_summary()`) after events complete.
    ///
    /// If done make sure you set the proper receiving side for the controller. For example in the
    /// nodes built through the [`build_peers`](`Self::build_peers`) method.
    ///
    /// # Example
    /// ```ignore
    /// let mut stream = sim.event_chain(100, None);
    /// while stream.next().await.is_some() {
    ///     tokio::time::sleep(Duration::from_millis(100)).await;
    /// }
    /// drop(stream); // Drop the stream before verification
    /// let result = sim.check_convergence().await; // sim is still usable!
    /// ```
    pub fn event_chain(
        &mut self,
        total_events: u32,
        controller: Option<watch::Sender<(EventId, TransportPublicKey)>>,
    ) -> EventChain {
        let user_ev_controller = controller.unwrap_or_else(|| {
            self.user_ev_controller
                .take()
                .expect("controller should be set")
        });
        // Clone labels - SimNetwork retains ownership for verification methods
        let labels = self.labels.clone();
        // EventChain no longer handles cleanup - SimNetwork's Drop does
        EventChain::new(labels, user_ev_controller, total_events, false)
    }

    /// Consumes the SimNetwork and returns an event chain.
    ///
    /// Use this when you don't need to access SimNetwork after events complete.
    /// For post-event verification, use [`event_chain`](Self::event_chain) instead.
    #[deprecated(
        since = "0.1.0",
        note = "Use event_chain(&mut self) instead to retain access to SimNetwork for verification"
    )]
    pub fn into_event_chain(
        mut self,
        total_events: u32,
        controller: Option<watch::Sender<(EventId, TransportPublicKey)>>,
    ) -> EventChain {
        let user_ev_controller = controller.unwrap_or_else(|| {
            self.user_ev_controller
                .take()
                .expect("controller should be set")
        });
        let labels = std::mem::take(&mut self.labels);
        let debug_val = self.clean_up_tmp_dirs;
        self.clean_up_tmp_dirs = false; // EventChain handles cleanup
        EventChain::new(labels, user_ev_controller, total_events, debug_val)
    }

    /// Checks that all peers in the network have acquired at least one connection to any
    /// other peers.
    ///
    /// This function advances VirtualTime to allow connection messages to be delivered.
    pub async fn check_connectivity(&mut self, time_out: Duration) -> anyhow::Result<()> {
        self.connectivity(time_out, 1.0).await
    }

    /// Checks that a percentage (given as a float between 0 and 1) of the nodes has at least
    /// one connection to any other peers.
    ///
    /// This function advances VirtualTime to allow connection messages to be delivered.
    pub async fn check_partial_connectivity(
        &mut self,
        time_out: Duration,
        percent: f64,
    ) -> anyhow::Result<()> {
        self.connectivity(time_out, percent).await
    }

    /// Internal connectivity check that advances VirtualTime.
    ///
    /// Issue #2725: The previous implementation used std::time::Instant (real wall-clock time)
    /// but never advanced VirtualTime. Since simulations use VirtualTime for deterministic
    /// message delivery, connection handshake messages were never delivered, causing all
    /// connectivity checks to fail with "found disconnected nodes".
    ///
    /// This implementation advances VirtualTime in small steps while checking connectivity,
    /// allowing connection messages to be delivered and processed.
    async fn connectivity(&mut self, time_out: Duration, percent: f64) -> anyhow::Result<()> {
        let num_nodes = self.number_of_nodes;
        let mut connected = HashSet::new();
        let elapsed = std::time::Instant::now();

        // Time step for advancing VirtualTime - small enough for responsiveness,
        // large enough to batch message delivery efficiently
        let time_step = Duration::from_millis(100);

        while elapsed.elapsed() < time_out && (connected.len() as f64 / num_nodes as f64) < percent
        {
            // Advance VirtualTime to trigger message delivery
            // This is critical for VirtualTime-based simulations where messages
            // are scheduled for delivery at VirtualTime + latency
            self.advance_time(time_step);

            // Yield to tokio so tasks can process delivered messages
            tokio::task::yield_now().await;

            // Small real-time sleep to allow task scheduling without spinning CPU
            tokio::time::sleep(Duration::from_millis(10)).await;

            // Check which nodes have connected
            for node in self.number_of_gateways..num_nodes + self.number_of_gateways {
                if !connected.contains(&node)
                    && self.is_connected(&NodeLabel::node(&self.name, node))
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

        // Calculate the percentage of nodes that are connected
        let connected_percent = connected.len() as f64 / num_nodes as f64;
        // Fail if fewer nodes are connected than the required percentage (with tolerance)
        if connected_percent < (percent - 0.01/* 1% error tolerance */) {
            missing.sort();
            let show_max = missing.len().min(100);
            tracing::error!("Nodes without connection: {:?}(..)", &missing[..show_max],);
            tracing::error!(
                "Total nodes without connection: {:?},  ({:.1}% connected < {:.1}% required)",
                missing.len(),
                connected_percent * 100.0,
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

    pub fn is_connected(&self, peer: &NodeLabel) -> bool {
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

    /// Returns a handle to the event logs that can be accessed after `run_simulation` consumes `self`.
    ///
    /// This is useful for tests that need to compare event logs between runs when using
    /// Turmoil's deterministic scheduler via `run_simulation()`.
    ///
    /// # Example
    /// ```ignore
    /// let sim = SimNetwork::new(...).await;
    /// let logs_handle = sim.event_logs_handle();
    ///
    /// sim.run_simulation::<SmallRng, _, _>(...)?;
    ///
    /// // Access logs after simulation completes
    /// let logs = logs_handle.lock().await;
    /// ```
    pub fn event_logs_handle(&self) -> Arc<tokio::sync::Mutex<Vec<crate::tracing::NetLogMessage>>> {
        self.event_listener.logs.clone()
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

        // Guard against division by zero
        if num_nodes == 0 {
            anyhow::bail!("cannot check connectivity quality with zero nodes");
        }

        let min_connections_threshold = (num_nodes as f64 * HIGHER_THAN_MIN_THRESHOLD) as usize;
        let node_connectivity = self.node_connectivity();

        let mut connections_per_peer: Vec<_> = node_connectivity
            .iter()
            .map(|(k, v)| (k, v.1.len()))
            .filter(|&(k, _)| !k.is_gateway())
            .map(|(_, v)| v)
            .collect();

        // Guard against empty connections list
        if connections_per_peer.is_empty() {
            anyhow::bail!("no non-gateway nodes found for connectivity check");
        }

        // ensure at least "most" normal nodes have more than one connection
        connections_per_peer.sort_unstable_by_key(|num_conn| *num_conn);
        if connections_per_peer
            .get(min_connections_threshold)
            .copied()
            .unwrap_or(0)
            < self.min_connections
        {
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
    ///
    /// # Implementation Note
    ///
    /// This method iterates through logs in insertion order (chronological order)
    /// rather than sorting by transaction ID. This is critical because broadcast
    /// events are logged with the sender's original transaction ID, not a new
    /// local timestamp. Sorting by transaction ID would cause delayed broadcasts
    /// with older transaction IDs to appear before newer local updates in the
    /// sorted order, resulting in incorrect "latest state" detection.
    ///
    /// For example, if Peer A updates to state S3 (tx=T15) and then receives a
    /// delayed broadcast of state S1 (tx=T1 < T15), the actual state is S1 (the
    /// broadcast was applied), but tx-sorted order would show S3 as "latest".
    pub async fn check_convergence(&self) -> ConvergenceResult {
        // Get logs in insertion order (chronological order within the simulation).
        // DO NOT use get_deterministic_event_summary() here - it sorts by transaction ID,
        // which is incorrect for determining latest state because broadcasts use the
        // sender's original transaction ID rather than a local timestamp.
        let logs = self.event_listener.logs.lock().await;

        // Group (contract_key -> peer_addr -> latest_state_hash)
        // Use BTreeMap for deterministic iteration order in DST
        let mut contract_states: BTreeMap<String, BTreeMap<SocketAddr, String>> = BTreeMap::new();

        // Iterate in insertion order - the last event for each (contract, peer) pair
        // is the actual current state.
        //
        // IMPORTANT: Use stored_state_hash() instead of state_hash() to only consider
        // events that represent actual stored state (PutSuccess, UpdateSuccess, BroadcastApplied).
        // Using state_hash() would incorrectly include BroadcastReceived events which record
        // the incoming state hash BEFORE it's applied, not the actual stored state.
        for log in logs.iter() {
            let contract_key = log.kind.contract_key().map(|k| format!("{:?}", k));
            let state_hash = log.kind.stored_state_hash().map(String::from);

            if let (Some(contract_key), Some(state_hash)) = (contract_key, state_hash) {
                // Keep the latest state for each peer/contract pair
                contract_states
                    .entry(contract_key)
                    .or_default()
                    .insert(log.peer_id.addr, state_hash);
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
                    peer_states: peer_states.into_iter().collect(),
                });
            }
        }

        ConvergenceResult {
            converged,
            diverged,
        }
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
        // Use tokio::time::Instant for deterministic behavior in simulation
        let start = tokio::time::Instant::now();

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

    /// Returns the number of unique contracts that have been subscribed to.
    ///
    /// Counts distinct contracts from SubscribeSuccess events. Use this to verify
    /// that subscribed contracts are actually getting replicated and converged.
    ///
    /// # Example
    /// ```ignore
    /// let subscribed_count = sim.count_subscribed_contracts().await;
    /// let converged = sim.check_convergence().await;
    /// assert_eq!(subscribed_count, converged.total_contracts(),
    ///     "All subscribed contracts should be replicated and checked for convergence");
    /// ```
    pub async fn count_subscribed_contracts(&self) -> usize {
        use std::collections::HashSet;
        let logs = self.event_listener.logs.lock().await;

        let mut subscribed_contracts: HashSet<String> = HashSet::new();

        for log in logs.iter() {
            if let crate::tracing::EventKind::Subscribe(
                crate::tracing::SubscribeEvent::SubscribeSuccess { key, .. },
            ) = &log.kind
            {
                subscribed_contracts.insert(format!("{:?}", key));
            }
        }

        subscribed_contracts.len()
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
    ) -> BTreeMap<String, BTreeMap<SocketAddr, String>> {
        let summary = self.get_deterministic_event_summary().await;

        // Use BTreeMap for deterministic iteration order in DST
        let mut contract_states: BTreeMap<String, BTreeMap<SocketAddr, String>> = BTreeMap::new();

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
                        PutEvent::ResponseSent { .. } => {} // Response tracking, doesn't affect summary
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
                        SubscribeEvent::SubscribeSuccess { .. } => summary.subscribe.succeeded += 1,
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
        // Use tokio::time::Instant for deterministic behavior in simulation
        let start = tokio::time::Instant::now();

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

    /// Waits for network quiescence - when no new network activity is detected.
    ///
    /// This is useful for determining when broadcasts have finished propagating.
    /// It works by monitoring the event log and waiting until no new entries are
    /// added for `quiescence_duration`.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for quiescence
    /// * `quiescence_duration` - How long activity must be quiet to consider quiesced
    /// * `poll_interval` - How often to check for new activity
    ///
    /// # Returns
    /// * `Ok(usize)` - Total log entries when quiesced
    /// * `Err(usize)` - Log entries at timeout (still active)
    pub async fn await_network_quiescence(
        &self,
        timeout: Duration,
        quiescence_duration: Duration,
        poll_interval: Duration,
    ) -> Result<usize, usize> {
        let start = tokio::time::Instant::now();
        let mut last_log_count = 0usize;
        let mut quiet_since = tokio::time::Instant::now();

        loop {
            let current_count = self.event_listener.logs.lock().await.len();

            if current_count != last_log_count {
                // Activity detected, reset quiet timer
                last_log_count = current_count;
                quiet_since = tokio::time::Instant::now();
            } else if quiet_since.elapsed() >= quiescence_duration {
                // Been quiet long enough
                tracing::info!(
                    "Network quiesced after {:?} with {} log entries",
                    start.elapsed(),
                    current_count
                );
                return Ok(current_count);
            }

            if start.elapsed() >= timeout {
                tracing::warn!(
                    "Network quiescence timeout after {:?}: {} log entries, still active",
                    timeout,
                    current_count
                );
                return Err(current_count);
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

    // =========================================================================
    // Turmoil-based Deterministic Simulation
    // =========================================================================

    /// Runs the simulation with deterministic scheduling using Turmoil.
    ///
    /// This method sets up all nodes as Turmoil hosts and runs the simulation
    /// deterministically. All tokio async operations are scheduled by Turmoil's
    /// deterministic scheduler, making tests reproducible.
    ///
    /// # Arguments
    ///
    /// * `seed` - Seed for random event generation
    /// * `max_contract_num` - Maximum number of contracts in the simulation
    /// * `iterations` - Number of iterations/events to run
    /// * `simulation_duration` - Maximum duration for the simulation
    /// * `test_fn` - A closure containing the test logic to run inside Turmoil
    ///
    /// # Example
    ///
    /// ```ignore
    /// use freenet::dev_tool::SimNetwork;
    /// use std::time::Duration;
    ///
    /// let sim = SimNetwork::new("test", 1, 5, 7, 3, 10, 2, 42).await;
    ///
    /// sim.run_simulation::<rand::rngs::SmallRng, _, _>(
    ///     42,
    ///     10,
    ///     100,
    ///     Duration::from_secs(60),
    ///     || async {
    ///         // Test assertions here
    ///         tokio::time::sleep(Duration::from_secs(5)).await;
    ///         Ok(())
    ///     },
    /// )?;
    /// ```
    ///
    /// # Determinism
    ///
    /// Running with the same seed will produce the same execution order and
    /// results. This is essential for:
    /// - Reproducing bugs
    /// - Property-based testing
    /// - CI reliability
    pub fn run_simulation<R, F, Fut>(
        mut self,
        seed: u64,
        max_contract_num: usize,
        iterations: usize,
        simulation_duration: Duration,
        test_fn: F,
    ) -> turmoil::Result
    where
        R: RandomEventGenerator + Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = turmoil::Result> + 'static,
    {
        use crate::config::{GlobalRng, GlobalSimulationTime};
        use std::sync::Mutex;

        // Set up deterministic RNG and time for reproducible simulation
        GlobalRng::set_seed(seed);

        // Derive simulation epoch from seed for deterministic ULID generation
        // Base: 2020-01-01 00:00:00 UTC, Range: ~5 years (keeps dates sensible: 2020-2025)
        const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
        const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years in ms
        let epoch_offset = seed % RANGE_MS;
        GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + epoch_offset);

        // Build Turmoil simulation with seeded RNG for deterministic execution
        let mut sim = turmoil::Builder::new()
            .simulation_duration(simulation_duration)
            .rng_seed(seed)
            .build();

        // Get total peer count for event generation
        let total_peer_num = self.gateways.len() + self.nodes.len();

        // Register all gateways as Turmoil hosts
        // Turmoil's sim.host requires Fn (can be called multiple times),
        // so we wrap non-Clone values in Arc<Mutex<Option<T>>> and take them on first call
        let gateways: Vec<_> = self.gateways.drain(..).collect();
        for (node, config) in gateways {
            let label = config.label.clone();
            let host_name = label.to_string();
            let receiver_ch = self.receiver_ch.clone();

            // Create shared in-memory storage for this node
            let shared_storage = crate::wasm_runtime::MockStateStorage::new();

            let mut user_events = MemoryEventsGen::<R>::new_with_seed(
                receiver_ch,
                node.config.key_pair.public().clone(),
                seed,
            );
            user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);

            let span = tracing::info_span!("turmoil_gateway", %label);

            // Store label for later reference
            self.labels
                .push((label.clone(), node.config.key_pair.public().clone()));

            // Wrap in Option so we can take() on first call
            let node = Arc::new(Mutex::new(Some(node)));
            let user_events = Arc::new(Mutex::new(Some(user_events)));
            let span = Arc::new(Mutex::new(Some(span)));
            let shared_storage = Arc::new(Mutex::new(Some(shared_storage)));

            sim.host(host_name, move || {
                let node = node.clone();
                let user_events = user_events.clone();
                let span = span.clone();
                let shared_storage = shared_storage.clone();

                async move {
                    let node = node
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let user_events = user_events
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let span = span
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let shared_storage = shared_storage
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");

                    node.run_node_with_shared_storage(user_events, span, shared_storage)
                        .await
                        .map_err(|e| {
                            Box::new(std::io::Error::other(e.to_string()))
                                as Box<dyn std::error::Error>
                        })
                }
            });
        }

        // Register all regular nodes as Turmoil hosts
        let nodes: Vec<_> = self.nodes.drain(..).collect();
        for (node, label) in nodes {
            let host_name = label.to_string();
            let receiver_ch = self.receiver_ch.clone();

            // Create shared in-memory storage for this node
            let shared_storage = crate::wasm_runtime::MockStateStorage::new();

            let mut user_events = MemoryEventsGen::<R>::new_with_seed(
                receiver_ch,
                node.config.key_pair.public().clone(),
                seed,
            );
            user_events.rng_params(label.number(), total_peer_num, max_contract_num, iterations);

            let span = tracing::info_span!("turmoil_node", %label);

            // Store label for later reference
            self.labels
                .push((label.clone(), node.config.key_pair.public().clone()));

            // Wrap in Option so we can take() on first call
            let node = Arc::new(Mutex::new(Some(node)));
            let user_events = Arc::new(Mutex::new(Some(user_events)));
            let span = Arc::new(Mutex::new(Some(span)));
            let shared_storage = Arc::new(Mutex::new(Some(shared_storage)));

            sim.host(host_name, move || {
                let node = node.clone();
                let user_events = user_events.clone();
                let span = span.clone();
                let shared_storage = shared_storage.clone();

                async move {
                    let node = node
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let user_events = user_events
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let span = span
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let shared_storage = shared_storage
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");

                    node.run_node_with_shared_storage(user_events, span, shared_storage)
                        .await
                        .map_err(|e| {
                            Box::new(std::io::Error::other(e.to_string()))
                                as Box<dyn std::error::Error>
                        })
                }
            });
        }

        // Take the event controller and labels for triggering events
        let user_ev_controller = self
            .user_ev_controller
            .take()
            .expect("user_ev_controller should be set");
        let labels: Vec<_> = self.labels.clone();

        // Register the test function as a Turmoil client
        sim.client("test", async move {
            // Give nodes time to start and establish connections
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Use a seeded RNG for deterministic peer selection
            use rand::prelude::*;
            use rand::SeedableRng;
            let mut event_rng = <rand::rngs::SmallRng as SeedableRng>::seed_from_u64(seed);

            // Trigger events by sending signals to peer event generators
            // Each signal tells one peer to generate its next random event
            // Use longer delays to ensure each event completes before the next starts
            for event_id in 0..iterations as u32 {
                // Pick a random peer to generate an event
                if let Some((_, peer_key)) = labels.choose(&mut event_rng) {
                    if user_ev_controller
                        .send((event_id, peer_key.clone()))
                        .is_err()
                    {
                        tracing::warn!(event_id, "Failed to send event signal - receivers dropped");
                        break;
                    }

                    // Longer delay between events to allow full processing
                    // This is critical for determinism - each operation must complete
                    // before the next one starts
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }

            // Wait for events to fully propagate through the network
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Run the user's test function
            test_fn().await
        });

        // Run the simulation
        sim.run()
    }

    /// Run a deterministic simulation with controlled events using Turmoil.
    ///
    /// Unlike `run_simulation` which uses random events, this method takes
    /// a predefined sequence of operations that will be executed in order.
    /// This is useful for testing specific scenarios like subscription topology.
    ///
    /// The simulation runs under Turmoil's deterministic scheduler, so
    /// `tokio::time::sleep` and other time-dependent operations are controlled.
    ///
    /// # Arguments
    /// * `seed` - Random seed for deterministic simulation
    /// * `operations` - Sequence of operations to execute in order
    /// * `simulation_duration` - Maximum duration for the simulation
    /// * `post_operations_wait` - Time to wait after operations complete (for recovery, etc.)
    ///
    /// # Returns
    /// A `turmoil::Result` indicating success or failure.
    ///
    /// # Example
    /// ```ignore
    /// let operations = vec![
    ///     ScheduledOperation::new(NodeLabel::gateway("test", 0), SimOperation::Put { ... }),
    ///     ScheduledOperation::new(NodeLabel::node("test", 1), SimOperation::Subscribe { ... }),
    /// ];
    /// let result = sim.run_controlled_simulation(
    ///     SEED,
    ///     operations,
    ///     Duration::from_secs(120),
    ///     Duration::from_secs(60), // Wait for orphan recovery
    /// );
    /// // After simulation, check result.topology_snapshots
    /// ```
    #[cfg(any(test, feature = "testing"))]
    pub fn run_controlled_simulation(
        mut self,
        seed: u64,
        operations: Vec<ScheduledOperation>,
        simulation_duration: Duration,
        post_operations_wait: Duration,
    ) -> ControlledSimulationResult {
        use crate::config::{GlobalRng, GlobalSimulationTime};
        use crate::ring::topology_registry::{
            get_all_topology_snapshots, set_current_network_name,
        };
        use std::collections::HashMap;
        use std::sync::Mutex;

        // Set up deterministic RNG and time for reproducible simulation
        GlobalRng::set_seed(seed);

        // Derive simulation epoch from seed for deterministic ULID generation
        const BASE_EPOCH_MS: u64 = 1577836800000; // 2020-01-01 00:00:00 UTC
        const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000; // ~5 years in ms
        let epoch_offset = seed % RANGE_MS;
        GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + epoch_offset);

        // Set the current network name for topology registration
        set_current_network_name(&self.name);

        // Save network name for topology retrieval after simulation
        let network_name = self.name.clone();

        // Build Turmoil simulation with seeded RNG for deterministic execution
        let mut sim = turmoil::Builder::new()
            .simulation_duration(simulation_duration)
            .rng_seed(seed)
            .build();

        // Build a map of label -> list of (event_id, operation)
        let mut operations_by_node: HashMap<NodeLabel, Vec<(EventId, SimOperation)>> =
            HashMap::new();
        let mut operation_sequence: Vec<(EventId, NodeLabel)> = Vec::new();

        for (event_id, scheduled_op) in operations.into_iter().enumerate() {
            let event_id = event_id as EventId;
            operation_sequence.push((event_id, scheduled_op.node.clone()));
            operations_by_node
                .entry(scheduled_op.node)
                .or_default()
                .push((event_id, scheduled_op.operation));
        }

        // Register all gateways as Turmoil hosts
        let gateways: Vec<_> = self.gateways.drain(..).collect();
        for (node, config) in gateways {
            let label = config.label.clone();
            let host_name = label.to_string();
            let receiver_ch = self.receiver_ch.clone();

            // Create shared in-memory storage for this node
            let shared_storage = crate::wasm_runtime::MockStateStorage::new();

            // Create MemoryEventsGen without RNG (deterministic mode)
            // Clone receiver_ch so each node gets its own subscription
            let subscription_notifications = self.subscription_notifications.clone();
            let subscription_mode = self.subscription_notification_mode;
            let mut user_events =
                MemoryEventsGen::new(receiver_ch.clone(), node.config.key_pair.public().clone())
                    .with_subscription_mode(subscription_mode)
                    .with_shared_notification_collection(subscription_notifications);

            // Populate events for this node
            if let Some(node_ops) = operations_by_node.remove(&label) {
                let events: Vec<_> = node_ops
                    .into_iter()
                    .map(|(id, op)| (id, op.into_client_request()))
                    .collect();
                user_events.generate_events(events);
            }

            let span = tracing::info_span!("turmoil_gateway_controlled", %label);

            // Store label for later reference
            self.labels
                .push((label.clone(), node.config.key_pair.public().clone()));

            // Wrap in Option so we can take() on first call
            let node = Arc::new(Mutex::new(Some(node)));
            let user_events = Arc::new(Mutex::new(Some(user_events)));
            let span = Arc::new(Mutex::new(Some(span)));
            let shared_storage = Arc::new(Mutex::new(Some(shared_storage)));

            sim.host(host_name, move || {
                let node = node.clone();
                let user_events = user_events.clone();
                let span = span.clone();
                let shared_storage = shared_storage.clone();

                async move {
                    let node = node
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let user_events = user_events
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let span = span
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let shared_storage = shared_storage
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");

                    node.run_node_with_shared_storage(user_events, span, shared_storage)
                        .await
                        .map_err(|e| {
                            Box::new(std::io::Error::other(e.to_string()))
                                as Box<dyn std::error::Error>
                        })
                }
            });
        }

        // Register all regular nodes as Turmoil hosts
        let nodes: Vec<_> = self.nodes.drain(..).collect();
        for (node, label) in nodes {
            let host_name = label.to_string();
            let receiver_ch = self.receiver_ch.clone();

            // Create shared in-memory storage for this node
            let shared_storage = crate::wasm_runtime::MockStateStorage::new();

            // Create MemoryEventsGen without RNG (deterministic mode)
            // Clone receiver_ch so each node gets its own subscription
            let subscription_notifications = self.subscription_notifications.clone();
            let subscription_mode = self.subscription_notification_mode;
            let mut user_events =
                MemoryEventsGen::new(receiver_ch.clone(), node.config.key_pair.public().clone())
                    .with_subscription_mode(subscription_mode)
                    .with_shared_notification_collection(subscription_notifications);

            // Populate events for this node
            if let Some(node_ops) = operations_by_node.remove(&label) {
                let events: Vec<_> = node_ops
                    .into_iter()
                    .map(|(id, op)| (id, op.into_client_request()))
                    .collect();
                user_events.generate_events(events);
            }

            let span = tracing::info_span!("turmoil_node_controlled", %label);

            // Store label for later reference
            self.labels
                .push((label.clone(), node.config.key_pair.public().clone()));

            // Wrap in Option so we can take() on first call
            let node = Arc::new(Mutex::new(Some(node)));
            let user_events = Arc::new(Mutex::new(Some(user_events)));
            let span = Arc::new(Mutex::new(Some(span)));
            let shared_storage = Arc::new(Mutex::new(Some(shared_storage)));

            sim.host(host_name, move || {
                let node = node.clone();
                let user_events = user_events.clone();
                let span = span.clone();
                let shared_storage = shared_storage.clone();

                async move {
                    let node = node
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let user_events = user_events
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let span = span
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");
                    let shared_storage = shared_storage
                        .lock()
                        .unwrap()
                        .take()
                        .expect("Turmoil host should only be called once");

                    node.run_node_with_shared_storage(user_events, span, shared_storage)
                        .await
                        .map_err(|e| {
                            Box::new(std::io::Error::other(e.to_string()))
                                as Box<dyn std::error::Error>
                        })
                }
            });
        }

        // Take the event controller and labels for triggering events
        let user_ev_controller = self
            .user_ev_controller
            .take()
            .expect("user_ev_controller should be set");
        let labels: Vec<_> = self.labels.clone();

        // Build a map from NodeLabel to peer key for event triggering
        let label_to_key: HashMap<NodeLabel, _> = labels.into_iter().collect();

        // Register the test client that triggers controlled events
        sim.client("test", async move {
            // Give nodes time to start and establish connections
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Trigger events in the specified order
            for (event_id, node_label) in operation_sequence {
                if let Some(peer_key) = label_to_key.get(&node_label) {
                    tracing::info!(
                        event_id,
                        node = %node_label,
                        "Triggering controlled event"
                    );

                    if user_ev_controller
                        .send((event_id, peer_key.clone()))
                        .is_err()
                    {
                        tracing::warn!(
                            event_id,
                            node = %node_label,
                            "Failed to send event signal - receivers dropped"
                        );
                        break;
                    }

                    // Wait for operation to complete before triggering next
                    tokio::time::sleep(Duration::from_secs(3)).await;
                } else {
                    tracing::warn!(
                        event_id,
                        node = %node_label,
                        "No peer key found for node label"
                    );
                }
            }

            // Wait for post-operation processing (orphan recovery, topology stabilization, etc.)
            tracing::info!(
                wait_secs = post_operations_wait.as_secs(),
                "Waiting for post-operation processing"
            );
            tokio::time::sleep(post_operations_wait).await;

            Ok(())
        });

        // Run the simulation
        let turmoil_result = sim.run();

        // Capture topology snapshots BEFORE self is dropped (which clears them)
        let topology_snapshots = get_all_topology_snapshots(&network_name);

        ControlledSimulationResult {
            turmoil_result,
            topology_snapshots,
        }
    }

    // =========================================================================
    // Subscription Topology Validation
    // =========================================================================

    /// Get the network name for this simulation.
    pub fn network_name(&self) -> &str {
        &self.name
    }

    /// Get all subscription topology snapshots registered for this network.
    ///
    /// Returns snapshots registered by nodes via the topology registry.
    /// Call this after nodes have been running to see their subscription state.
    pub fn get_topology_snapshots(&self) -> Vec<crate::ring::topology_registry::TopologySnapshot> {
        crate::ring::topology_registry::get_all_topology_snapshots(&self.name)
    }

    /// Validate subscription topology for a specific contract.
    ///
    /// Checks for:
    /// - Bidirectional cycles that create isolated islands
    /// - Orphan seeders without recovery paths
    /// - Unreachable seeders
    /// - Proximity violations in upstream selection
    ///
    /// Returns a validation result with any issues found.
    pub fn validate_subscription_topology(
        &self,
        contract_id: &freenet_stdlib::prelude::ContractInstanceId,
        contract_location: f64,
    ) -> crate::ring::topology_registry::TopologyValidationResult {
        crate::ring::topology_registry::validate_topology(
            &self.name,
            contract_id,
            contract_location,
        )
    }

    /// Clear all topology snapshots for this network.
    ///
    /// Call this at the start of a test or after topology validation
    /// to reset the state.
    pub fn clear_topology_snapshots(&self) {
        crate::ring::topology_registry::clear_topology_snapshots(&self.name);
    }

    /// Assert that subscription topology is healthy for a contract.
    ///
    /// # Panics
    /// Panics if any topology issues are detected.
    pub fn assert_topology_healthy(
        &self,
        contract_id: &freenet_stdlib::prelude::ContractInstanceId,
        contract_location: f64,
    ) {
        let result = self.validate_subscription_topology(contract_id, contract_location);

        if !result.is_healthy() {
            let mut issues = Vec::new();

            if !result.bidirectional_cycles.is_empty() {
                issues.push(format!(
                    "ISSUE #2720: {} bidirectional cycles found: {:?}",
                    result.bidirectional_cycles.len(),
                    result.bidirectional_cycles
                ));
            }

            if !result.orphan_seeders.is_empty() {
                issues.push(format!(
                    "ISSUE #2719: {} orphan seeders found: {:?}",
                    result.orphan_seeders.len(),
                    result.orphan_seeders
                ));
            }

            if !result.unreachable_seeders.is_empty() {
                issues.push(format!(
                    "ISSUE #2720: {} unreachable seeders found: {:?}",
                    result.unreachable_seeders.len(),
                    result.unreachable_seeders
                ));
            }

            if !result.proximity_violations.is_empty() {
                issues.push(format!(
                    "ISSUE #2721: {} proximity violations found",
                    result.proximity_violations.len()
                ));
            }

            panic!(
                "Subscription topology has {} issues:\n{}",
                result.issue_count,
                issues.join("\n")
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
    /// Includes timeouts as failed operations.
    pub fn overall_success_rate(&self) -> f64 {
        let completed = self.total_completed() + self.timeouts;
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
            .finish()
    }
}

impl Drop for SimNetwork {
    fn drop(&mut self) {
        // Clean up the fault injector for this network
        use crate::node::network_bridge::set_fault_injector;
        use crate::ring::topology_registry::{
            clear_current_network_name, clear_topology_snapshots,
        };
        set_fault_injector(&self.name, None);

        // Clean up the VirtualTime registration for this network
        unregister_network_time_source(&self.name);

        // Clean up topology registry for this network
        clear_topology_snapshots(&self.name);
        clear_current_network_name();

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

/// Check convergence from event logs.
///
/// This function can be used after `run_simulation` completes to check
/// if all contracts have converged to the same state across replicas.
///
/// # Arguments
/// * `logs` - Event logs obtained via `sim.event_logs_handle()` before calling `run_simulation`
///
/// # Example
/// ```ignore
/// let logs_handle = sim.event_logs_handle();
/// let result = sim.run_simulation::<...>(...);
/// let convergence = check_convergence_from_logs(&logs_handle).await;
/// ```
pub async fn check_convergence_from_logs(
    logs: &Arc<tokio::sync::Mutex<Vec<crate::tracing::NetLogMessage>>>,
) -> ConvergenceResult {
    let logs = logs.lock().await;

    // Group (contract_key -> peer_addr -> latest_state_hash)
    // Use BTreeMap for deterministic iteration order
    let mut contract_states: BTreeMap<String, BTreeMap<SocketAddr, String>> = BTreeMap::new();

    // Iterate in insertion order - the last event for each (contract, peer) pair
    // is the actual current state.
    for log in logs.iter() {
        let contract_key = log.kind.contract_key().map(|k| format!("{:?}", k));
        let state_hash = log.kind.stored_state_hash().map(String::from);

        if let (Some(contract_key), Some(state_hash)) = (contract_key, state_hash) {
            contract_states
                .entry(contract_key)
                .or_default()
                .insert(log.peer_id.addr, state_hash);
        }
    }

    let mut converged = Vec::new();
    let mut diverged = Vec::new();

    for (contract_key, peer_states) in contract_states {
        if peer_states.len() < 2 {
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
                peer_states: peer_states.into_iter().collect(),
            });
        }
    }

    ConvergenceResult {
        converged,
        diverged,
    }
}

use crate::contract::OperationMode;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that peer locations are deterministic with the same seed.
    ///
    /// Regression test for issue #2759 - SimNetwork should produce identical
    /// peer locations across multiple runs with the same seed.
    #[tokio::test]
    async fn test_deterministic_peer_locations() {
        const SEED: u64 = 0xDEADBEEF_CAFEBABE;

        // Create first network
        let sim1 = SimNetwork::new(
            "determinism-test-1",
            2,  // 2 gateways
            3,  // 3 regular nodes
            7,  // ring_max_htl
            3,  // rnd_if_htl_above
            10, // max_connections
            2,  // min_connections
            SEED,
        )
        .await;

        let locations1 = sim1.get_peer_locations();

        // Create second network with same seed
        let sim2 = SimNetwork::new(
            "determinism-test-2",
            2, // same config
            3,
            7,
            3,
            10,
            2,
            SEED, // same seed
        )
        .await;

        let locations2 = sim2.get_peer_locations();

        // Verify locations are identical
        assert_eq!(
            locations1, locations2,
            "Peer locations should be identical with the same seed.\n\
             Run 1: {:?}\n\
             Run 2: {:?}",
            locations1, locations2
        );
    }
}
