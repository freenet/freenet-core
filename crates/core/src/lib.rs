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
pub use node::{
    EventLoopExitReason, Node, ShutdownHandle, enable_abort_on_fatal_listener_exit,
    enable_abort_on_redb_poison, enable_fast_crash_exit_code, run_local_node, run_network_node,
};

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

/// Outlier-detection primitive shared by per-contract governance and
/// peer-side load-shedding. See `docs/design/contract-hardening.md`.
mod governance;

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
    pub use crate::config::{
        Config, GlobalTestMetrics, KEK_SIZE, KekBackend, KekBackendKind, KekError, Secrets,
        ensure_kek_loaded, load_from_backend, read_backend_marker, replace_backend_marker,
        write_backend_marker,
    };
    // Backend constructors live in the `kek` submodule rather than the
    // top-level `config` re-export to keep `Config`'s public surface
    // free of platform-specific concrete types.
    pub use crate::config::kek::{FileKek, KeyringKek, SystemdCredentialKek, build_backend_for};
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

    // Test hooks: per-op driver call counters. Tests assert these
    // increment to confirm wire variants dispatch through their
    // driver (not a local-cache shortcut or legacy path).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::get::op_ctx_task::DRIVER_CALL_COUNT as GET_DRIVER_CALL_COUNT;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::get::op_ctx_task::RELAY_DRIVER_CALL_COUNT as GET_RELAY_DRIVER_CALL_COUNT;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::get::op_ctx_task::RELAY_GET_STREAMING_FORWARD_COUNT;
    // Deterministic stream-assembly fault injection + retry counter for
    // the GET driver's assembly-retry path (#4345).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::get::op_ctx_task::assembly_fault_injection as get_assembly_fault_injection;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::put::op_ctx_task::RELAY_PUT_DRIVER_CALL_COUNT;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::put::op_ctx_task::RELAY_PUT_STREAMING_DRIVER_CALL_COUNT;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::subscribe::op_ctx_task::RELAY_SUBSCRIBE_DRIVER_CALL_COUNT;

    // Test hooks for the relay-hop routing-event plumbing. Each counter
    // increments every time `operations::record_relay_route_event` fires
    // for the corresponding op type. Used by simulation tests to verify
    // that relay-forwarded operations actually feed the local Router's
    // failure-probability model — without these hooks, the router would
    // only see events from originator paths (the bug this work fixes).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::{
        RELAY_GET_ROUTE_EVENT_COUNT, RELAY_PUT_ROUTE_EVENT_COUNT,
        RELAY_SUBSCRIBE_ROUTE_EVENT_COUNT, RELAY_UPDATE_ROUTE_EVENT_COUNT,
    };

    // Re-export state verification for telemetry-based consistency analysis
    pub use crate::tracing::state_verifier::{StateAnomaly, StateVerifier, VerificationReport};

    // Re-export topology registry for subscription validation in tests
    pub use ring::topology_registry::{
        ContractSubscription, ProximityViolation, RenewalMetrics, TopologySnapshot,
        TopologyValidationResult, aggregate_renewal_metrics, clear_all_topology_snapshots,
        clear_current_network_name, clear_renewal_metrics, clear_topology_snapshots,
        get_all_renewal_metrics, get_all_topology_snapshots, get_current_network_name,
        get_renewal_metrics, get_topology_snapshot, register_topology_snapshot,
        set_current_network_name, validate_topology, validate_topology_from_snapshots,
    };
    pub use wasm_runtime::secret_export::{
        BundleKeyMaterial, ExportError, ImportReport, TargetScope, export_bundle, import_bundle,
        write_bundle_file,
    };
    pub use wasm_runtime::secret_snapshots::{
        RestoreError, RetentionPolicy, SnapshotMetadata, list_snapshots, restore_snapshot_file,
        snapshot_dir_for_encoded, thin_snapshots,
    };
    pub use wasm_runtime::{
        ContractStore, DelegateStore, ExportSecretEntry, MockStateStorage, Runtime, SecretScope,
        SecretStoreError, SecretsStore, StateStore, UserSecretContext,
    };

    /// Maximum size (in bytes) of a single contract-state blob a node will
    /// accept, re-exported for client-side pre-flight checks. This is the
    /// binding publish limit; see
    /// `wasm_runtime::state_store::MAX_STATE_SIZE` for the full limit
    /// hierarchy. `fdev website publish` uses this to reject oversized sites
    /// before sending a PUT (#4653).
    pub use wasm_runtime::MAX_STATE_SIZE as MAX_CONTRACT_STATE_SIZE;

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
