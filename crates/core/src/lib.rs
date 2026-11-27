/// Clients events related logic and type definitions.
pub(crate) mod client_events;

/// Peer node configuration.
pub mod config;

/// Handling of contracts and delegates functionality.
mod contract;

/// Contract conformance: the shared verifier and evidence model (RFC #5320).
///
/// Public because `fdev verify-merge` runs the *same* verifier the network does.
/// A developer-facing check that could disagree with the network-facing one would
/// be worse than no check at all.
pub mod conformance;

// Re-export for integration tests (tests/ directory needs pub access)
#[cfg(any(test, feature = "testing", feature = "redb"))]
pub use contract::storages;

/// Generated messages from the flatbuffers schema for the network monitor.
pub mod generated;

/// Network messages for transactions.
mod message;

/// Node configuration, implementations and execution (entry points for the binaries).
mod node;
#[doc(hidden)]
pub use node::QUIC_RESTART_EXIT_CODE;
pub use node::{
    EventLoopExitReason, Node, ShutdownHandle, enable_abort_on_fatal_listener_exit,
    enable_abort_on_redb_poison, enable_fast_crash_exit_code, listener_exit_is_graceful,
    run_local_node, run_local_node_with_listener, run_network_node, set_auto_update_state,
};
#[doc(hidden)]
pub use transport::connection_handler::QUIC_RESTORE_MIN_UPTIME;
#[doc(hidden)]
pub use transport::quic::breaker::{
    ENDPOINT_DOWN_EXITS_TO_TRIP, GATEWAY_QUIC_BREAKER_COOLDOWN, PEER_SHORT_RUNS_TO_TRIP_WITH_UDP,
    UDP_FALLBACK as QUIC_BREAKER_UDP_FALLBACK,
    short_runs_to_trip as quic_breaker_short_runs_to_trip,
};
pub use transport::quic::breaker::{
    STOP_NOTE_ENV_VAR as QUIC_BREAKER_STOP_NOTE_ENV_VAR,
    STOP_NOTE_FILE as QUIC_BREAKER_STOP_NOTE_FILE, enable_quic_crash_breaker,
    quic_breaker_clean_exit, quic_breaker_signals_ready, quic_crash_loop_update_now,
    record_deliberate_stop as record_quic_breaker_deliberate_stop,
    wait_for_transport_open as wait_for_quic_transport_open,
};
pub use transport::quic::fatal_exit::{
    FATAL_EXIT_UPDATE_CHECK_TIMEOUT, FatalExitCheckEnd, FatalExitUpdateCheck,
    register_fatal_exit_update_check,
};
pub use transport::quic::liveness::{
    enable_gateway_quic_restarts, note_gateway_supervisor_unverified,
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

/// `build.rs`'s choice of `rerun-if-changed` paths (#5667). Build scripts have
/// no test harness of their own, so the module is compiled here to test it.
#[cfg(test)]
#[path = "../build/git_watch.rs"]
mod build_git_watch;

/// `build.rs`'s choice of commit and dirty flag, including the
/// `.cargo_vcs_info.json` of a crates.io package, tested here for the same
/// reason.
#[cfg(test)]
#[path = "../build/vcs_info.rs"]
mod build_vcs_info;

/// Pin the process-start anchor used by the bootstrap-latency metric
/// (`freenet.bootstrap.time_to_min_connections_seconds`, issue #4787).
///
/// `Instant` cannot be constructed for "when this process started", so the
/// earliest instant available is whenever something first asks. Call this on
/// the first line of `main` and the metric genuinely measures from process
/// start — including config load, storage open, and every other startup step
/// that can delay CONNECT. Without it the anchor falls back to
/// `network_status::init()`, i.e. node start.
pub fn mark_process_start() {
    node::network_status::mark_process_start();
}

#[cfg(target_os = "linux")]
#[doc(hidden)]
pub use wasm_runtime::engine::compile_worker::production_worker_accepts_for_tests;
pub use wasm_runtime::engine::compile_worker::run_compile_worker_if_requested;

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
    /// Simulation hooks for the per-contract merge budget (#6190): make some
    /// contracts' merges never settle, and count merges and budget decisions.
    /// Process-global: a test that arms them must run in its own process.
    #[cfg(any(test, feature = "testing"))]
    pub mod sim_merge_budget {
        use std::sync::atomic::Ordering;

        use crate::contract::executor::mock_wasm_runtime::sim_hooks;
        use crate::ring::contract_merge_budget::sim;

        /// Arm the hooks: contracts whose first id byte is a multiple of
        /// `modulus` merge non-idempotently, and merges and budget decisions
        /// are counted. `modulus` must be non-zero.
        pub fn arm(modulus: u8) {
            assert!(modulus != 0, "use disarm()");
            sim_hooks::BAD_CONTRACT_MODULUS.store(modulus, Ordering::Relaxed);
            sim::ARMED.store(true, Ordering::Relaxed);
        }
        /// Disarm the hooks and drop their counts.
        pub fn disarm() {
            sim_hooks::BAD_CONTRACT_MODULUS.store(0, Ordering::Relaxed);
            sim::ARMED.store(false, Ordering::Relaxed);
            let _ = take_merge_counts();
            let _ = take_merge_budget_counts();
        }
        /// Take and reset per-contract merge-budget counts summed over all
        /// nodes: (first id byte, id string, admitted, dropped, charged).
        pub fn take_merge_budget_counts() -> Vec<(u8, String, u64, u64, u64)> {
            sim::COUNTS
                .lock()
                .take()
                .unwrap_or_default()
                .into_iter()
                .map(|(id, (a, d, c))| (id.as_bytes()[0], id.to_string(), a, d, c))
                .collect()
        }
        /// Take and reset per-contract `update_state` call counts:
        /// (first id byte, id string, count).
        pub fn take_merge_counts() -> Vec<(u8, String, u64)> {
            sim_hooks::MERGE_COUNTS
                .lock()
                .unwrap()
                .take()
                .unwrap_or_default()
                .into_iter()
                .map(|(id, n)| (id.as_bytes()[0], id.to_string(), n))
                .collect()
        }
    }
    /// Simulation hook for ANT-2026-K0YQ8CG4: make chosen nodes' contract
    /// handlers refuse every relay query, as a full relay lane does.
    /// Process-global: a test that uses it must run in its own process.
    #[cfg(any(test, feature = "testing"))]
    pub mod sim_relay_overload {
        /// Treat the node at `addr` as having a full relay lane.
        pub fn saturate(addr: std::net::SocketAddr) {
            crate::operations::relay_admission::sim_overload::saturate(addr);
        }
        /// Undo every `saturate`.
        pub fn clear() {
            crate::operations::relay_admission::sim_overload::clear();
        }
    }
    pub use crate::config::{
        Config, GlobalTestMetrics, KEK_SIZE, KekBackend, KekBackendKind, KekError, Secrets,
        ensure_kek_loaded, load_from_backend, read_backend_marker, replace_backend_marker,
        write_backend_marker,
    };
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::connect::native_wire_trace::NativeWireTrace;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::transport::quic::upgrade::sim_trace::{QuicUpgradeRecord, QuicUpgradeTrace};
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
    pub use node::testing_impl::SimNat;
    pub use node::{
        InitPeerNode, NetworkStats, NodeConfig, PeerId,
        testing_impl::{
            ChurnConfig, ContractDistribution, ControlledEventChain, ControlledSimulationResult,
            ConvergedContract, ConvergenceResult, DivergedContract, EventChain, EventSummary,
            FinalStateHandle, NetworkPeer, NodeLabel, OperationStats, OperationSummary,
            PeerMessage, PeerStatus, PutOperationStats, RunningNode, ScheduledOperation,
            SimNetwork, SimOperation, TurmoilConfig, TurmoilResult, UpdateOperationStats,
            check_convergence_from_logs, check_convergence_from_logs_and_state,
            run_turmoil_simulation,
        },
    };
    pub use ring::Location;
    pub use transport::nat_model::NatFiltering;
    pub use transport::{TransportKeypair, TransportPublicKey};

    // Nearest-neighbor ring lattice (successor+predecessor base edges). Always
    // ON in production above the minimum-degree floor; `set_nn_lattice_enabled` is
    // the test-only per-thread override the findability validation harness flips
    // to run the stock (lattice-off) arm against the fix (lattice-on) arm on
    // identical seeds — it ALSO force-activates the lattice below the floor so the
    // benefit tests exercise the ON path at their sparse max_connections.
    // `set_nn_lattice_force_active` resets only the floor bypass (used by the
    // benefit-test cleanup guard). No-op in production builds (compiled out).
    #[cfg(any(test, feature = "testing"))]
    pub use ring::put_cost_pick_hook::PutCostPickGuard;
    #[cfg(any(test, feature = "testing"))]
    pub use ring::{set_nn_lattice_enabled, set_nn_lattice_force_active};

    // Test hook: connection identity decisions since process start, by kind
    // (labels as in the `conn_identity_*` telemetry). Never reset.
    #[cfg(any(test, feature = "testing"))]
    pub use crate::transport::conn_identity_totals;

    // Test-only findability measurement harness (NOT for ship): scatter/cache
    // disable hook (guarantees a single copy) + per-op terminus tracing (rank /
    // HTL / hop / stop-reason / connectivity-gap-vs-give-up). See
    // operations::findability_probe. Compiled out in production.
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::findability_probe::{
        OpTraceRecord, ProbeOpKind, ProbeStopReason, clear_op_traces, scatter_disabled,
        set_scatter_disabled, take_op_traces,
    };

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
    // Deterministic streaming PUT relay stream-failure injection, and the
    // count of relay failures reported upstream (#5671).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::put::op_ctx_task::RELAY_PUT_STREAMING_FAILURES_REPORTED;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::put::op_ctx_task::relay_stream_fault_injection as put_relay_stream_fault_injection;
    // Silent loss of a node's inline UPDATE legs for one contract (#6143).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::update::inline_leg_fault_injection::seam as inline_leg_fault_injection;
    // Per-contract record of the summary-mismatch heal path (#6143).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::node::stale_peer_heal_observations;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::subscribe::op_ctx_task::RELAY_SUBSCRIBE_DRIVER_CALL_COUNT;

    // Per-contract off switch for the verify-soon check, for control arms (#6143).
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::update::verify_soon::PERSISTENT_FAULT_BOUND as VERIFY_SOON_PERSISTENT_FAULT_BOUND;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::update::verify_soon::SLOT_WAIT_BOUND as VERIFY_SOON_SLOT_WAIT_BOUND;
    #[cfg(any(test, feature = "testing"))]
    pub use crate::operations::update::verify_soon::test_switch as verify_soon_switch;

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
        clear_current_network_name, clear_renewal_metrics, clear_summarize_metrics,
        clear_topology_snapshots, get_all_renewal_metrics, get_all_summarize_wasm_calls,
        get_all_topology_snapshots, get_current_network_name, get_renewal_metrics,
        get_topology_snapshot, register_topology_snapshot, set_current_network_name,
        validate_topology, validate_topology_from_snapshots,
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
        FaultConfig, FaultConfigBuilder, Partition, QuicDatagramCut, QuicDatagramDrop,
        SimulationRng, TimeSource, VirtualTime, WakeupId,
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
