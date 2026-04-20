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
pub use node::{EventLoopExitReason, Node, ShutdownHandle, run_local_node, run_network_node};

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
        AuthToken, ClientEventsProxy, ClientId, OpenRequest, test::MemoryEventsGen,
        test::NetworkEventGenerator,
    };
    pub use contract::{
        Executor, OperationMode, clear_crdt_contracts, is_crdt_contract, register_crdt_contract,
        storages::Storage,
    };
    pub use flatbuffers;
    pub use message::Transaction;
    pub use node::{
        InitPeerNode, NetworkStats, NodeConfig, PeerId,
        testing_impl::{
            ChurnConfig, ContractDistribution, ControlledEventChain, ControlledSimulationResult,
            ConvergedContract, ConvergenceResult, DivergedContract, EventChain, EventSummary,
            NetworkPeer, NodeLabel, OperationStats, OperationSummary, PeerMessage, PeerStatus,
            PutOperationStats, RunningNode, ScheduledOperation, SimNetwork, SimOperation,
            TurmoilConfig, TurmoilResult, UpdateOperationStats, check_convergence_from_logs,
            run_turmoil_simulation,
        },
    };
    pub use ring::Location;
    pub use transport::{TransportKeypair, TransportPublicKey};

    // #1454 Phase 3b — test hook to verify client-initiated GETs
    // actually route through the task-per-tx driver rather than being
    // satisfied by the `client_events.rs` local-cache shortcut.
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::get::op_ctx_task::DRIVER_CALL_COUNT as GET_DRIVER_CALL_COUNT;

    // #1454 Phase 5 / #3883 — test hook to verify the dispatch gate in
    // `handle_pure_network_message_v1` actually routes fresh inbound relay
    // GETs through the task-per-tx driver (vs. the legacy
    // `handle_op_request` fallthrough used for originator loopback,
    // GC-spawned retries, and `start_targeted_op` UPDATE auto-fetch).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::get::op_ctx_task::RELAY_DRIVER_CALL_COUNT as GET_RELAY_DRIVER_CALL_COUNT;

    // #1454 Phase 5 follow-up slice A (#3917) — test hook to verify
    // the dispatch gate in `handle_pure_network_message_v1` actually
    // routes fresh inbound relay PUTs through the task-per-tx driver
    // (vs. the legacy `handle_op_request` fallthrough used for
    // client-initiated loopback and GC-spawned retries).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::put::op_ctx_task::RELAY_PUT_DRIVER_CALL_COUNT;

    // Re-export state verification for telemetry-based consistency analysis
    pub use crate::tracing::state_verifier::{StateAnomaly, StateVerifier, VerificationReport};

    // Re-export topology registry for subscription validation in tests
    pub use ring::topology_registry::{
        ContractSubscription, ProximityViolation, TopologySnapshot, TopologyValidationResult,
        clear_all_topology_snapshots, clear_current_network_name, clear_topology_snapshots,
        get_all_topology_snapshots, get_current_network_name, get_topology_snapshot,
        register_topology_snapshot, set_current_network_name, validate_topology,
        validate_topology_from_snapshots,
    };
    pub use wasm_runtime::{
        ContractStore, DelegateStore, MockStateStorage, Runtime, SecretsStore, StateStore,
    };

    // Re-export simulation types for test infrastructure
    pub use crate::simulation::{
        FaultConfig, FaultConfigBuilder, Partition, SimulationRng, TimeSource, VirtualTime,
        WakeupId,
    };

    // Re-export fault injector for mid-simulation fault injection in Turmoil tests
    pub use crate::node::{FaultInjectorState, get_fault_injector, set_fault_injector};

    // Re-export counter reset functions for deterministic simulation testing
    pub use crate::client_events::RequestId;
    pub use crate::contract::reset_event_id_counter;
    pub use crate::node::reset_channel_id_counter;
    pub use crate::test_utils::reset_global_node_index;
    pub use crate::transport::StreamId;
    pub use crate::transport::reset_nonce_counter;
}

/// Deadlock detection for parking_lot locks in test builds.
///
/// Available when compiled with `--cfg test` (unit tests) or with the `testing`
/// feature flag (integration tests). Uses parking_lot's `deadlock_detection`
/// feature to catch Mutex/RwLock deadlocks at runtime.
#[cfg(any(test, feature = "testing"))]
pub mod deadlock_detection;

pub mod test_utils;
