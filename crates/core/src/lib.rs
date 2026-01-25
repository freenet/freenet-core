/// Clients events related logic and type definitions.
pub(crate) mod client_events;

/// Peer node configuration.
pub mod config;

/// Handling of contracts and delegates functionality.
mod contract;

// Re-export for integration tests (tests/ directory needs pub access)
#[cfg(any(test, feature = "testing", feature = "redb"))]
pub use contract::storages;

/// Generated messages from the flatbuffers schema for the network monitor.
pub mod generated;

/// Network messages for transactions.
mod message;

/// Node configuration, implementations and execution (entry points for the binaries).
mod node;
pub use node::{run_local_node, run_network_node, EventLoopExitReason, Node, ShutdownHandle};

/// Network operation/transaction state machines.
mod operations;

/// Ring connections and routing.
mod ring;

/// Router implementation.
mod router;

/// Local server used to communicate with the peer core.
#[cfg(feature = "websocket")]
pub mod server;

/// Local network topology management.
mod topology;

/// Tracing and loging infrastructure. Includes our custom event log register. Tracing collectors, etc.
#[cfg_attr(test, allow(dead_code))]
pub mod tracing;

/// Code for communicating with other peers over UDP, handles hole-punching, error handling, etc.
pub mod transport;

pub mod util;

/// WASM code execution runtime, tailored for the contract and delegate APIs.
mod wasm_runtime;

/// Deterministic simulation testing framework.
pub mod simulation;

/// Exports to build a running local node.
pub mod local_node {
    use super::*;
    pub use contract::Executor;
    pub use contract::OperationMode;
    pub use node::NodeConfig;
}

/// Exports for the dev tool.
pub mod dev_tool {
    use super::*;
    pub use crate::config::{Config, GlobalTestMetrics};
    pub use client_events::{
        test::MemoryEventsGen, test::NetworkEventGenerator, test::SubscriptionNotificationMode,
        AuthToken, ClientEventsProxy, ClientId, OpenRequest,
    };
    pub use contract::{
        clear_crdt_contracts, is_crdt_contract, register_crdt_contract, storages::Storage,
        Executor, OperationMode,
    };
    pub use flatbuffers;
    pub use message::Transaction;
    pub use node::{
        testing_impl::{
            check_convergence_from_logs, run_turmoil_simulation, ContractDistribution,
            ControlledEventChain, ControlledSimulationResult, ConvergedContract, ConvergenceResult,
            DivergedContract, EventChain, EventSummary, NetworkPeer, NodeLabel, OperationStats,
            OperationSummary, PeerMessage, PeerStatus, PutOperationStats, RunningNode,
            ScheduledOperation, SimNetwork, SimOperation, TurmoilConfig, TurmoilResult,
            UpdateOperationStats,
        },
        InitPeerNode, NetworkStats, NodeConfig, PeerId,
    };
    pub use ring::Location;
    pub use transport::{TransportKeypair, TransportPublicKey};

    // Re-export topology registry for subscription validation in tests
    pub use ring::topology_registry::{
        clear_all_topology_snapshots, clear_current_network_name, clear_topology_snapshots,
        get_all_topology_snapshots, get_current_network_name, get_topology_snapshot,
        register_topology_snapshot, set_current_network_name, validate_topology,
        validate_topology_from_snapshots, ContractSubscription, ProximityViolation,
        TopologySnapshot, TopologyValidationResult,
    };
    pub use wasm_runtime::{ContractStore, DelegateStore, Runtime, SecretsStore, StateStore};

    // Re-export simulation types for test infrastructure
    pub use crate::simulation::{
        FaultConfig, FaultConfigBuilder, Partition, SimulationRng, TimeSource, VirtualTime,
        WakeupId,
    };

    // Re-export counter reset functions for deterministic simulation testing
    pub use crate::client_events::RequestId;
    pub use crate::contract::reset_event_id_counter;
    pub use crate::node::reset_channel_id_counter;
    pub use crate::test_utils::reset_global_node_index;
    pub use crate::transport::reset_nonce_counter;
    pub use crate::transport::StreamId;

    /// Reset all global simulation state for deterministic testing.
    ///
    /// This function resets all atomic counters and global registries to their
    /// initial state, ensuring that simulation runs with the same seed produce
    /// identical results.
    ///
    /// Call this at the start of each simulation run, AFTER setting the RNG seed
    /// with `GlobalRng::set_seed()`.
    ///
    /// # What gets reset:
    /// - Request ID counter
    /// - Client ID counter
    /// - Event ID counter
    /// - Channel ID counter
    /// - Stream ID counter
    /// - Nonce counter
    /// - Global node index
    /// - Socket registries
    /// - Address network mappings
    /// - Simulation time
    /// - Topology snapshots
    pub fn reset_all_simulation_state() {
        // Reset RNG (caller should set seed after this)
        crate::config::GlobalRng::clear_seed();

        // Reset simulation time (caller should set time after this if needed)
        crate::config::GlobalSimulationTime::clear_time();

        // Reset all atomic counters
        crate::client_events::RequestId::reset_counter();
        crate::client_events::ClientId::reset_counter();
        crate::contract::reset_event_id_counter();
        crate::node::reset_channel_id_counter();
        crate::transport::StreamId::reset_counter();
        crate::transport::reset_nonce_counter();
        crate::test_utils::reset_global_node_index();

        // Reset global registries
        crate::transport::in_memory_socket::clear_all_socket_registries();
        crate::transport::in_memory_socket::clear_all_address_networks();
        crate::transport::in_memory_socket::clear_all_network_time_sources();
        crate::node::clear_all_fault_injectors();

        // Clear topology snapshots from previous simulation runs
        crate::ring::topology_registry::clear_all_topology_snapshots();
    }
}

pub mod test_utils;
