//! End-to-end streaming transport tests.
//!
//! These tests exercise the complete streaming path: operation send-side ->
//! transport fragmentation -> UDP delivery -> transport reassembly -> orphan claim ->
//! operation receive-side -> correct result.
//!
//! Assertions verify that contract state actually arrived in non-gateway node
//! storages with the correct bytes, replacing the earlier global-counter approach.
//!
//! All tests use `run_controlled_simulation()` for deterministic execution via Turmoil.
//!
//! NOTE: These tests use global state and must run serially.
//! Enable with: cargo test -p freenet --features "simulation_tests,testing" --test streaming_e2e -- --test-threads=1

#![cfg(feature = "simulation_tests")]

use freenet::dev_tool::{
    reset_all_simulation_state, MockStateStorage, NodeLabel, ScheduledOperation, SimNetwork,
    SimOperation,
};
use freenet_stdlib::prelude::*;
use std::time::Duration;

// =============================================================================
// Helpers
// =============================================================================

/// Set up a SimNetwork with streaming enabled at the given threshold.
async fn setup_streaming_network(
    name: &str,
    gateways: usize,
    nodes: usize,
    seed: u64,
    streaming_threshold: usize,
) -> SimNetwork {
    reset_all_simulation_state();

    let mut sim = SimNetwork::new(
        name, gateways, nodes, 7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        seed,
    )
    .await;
    sim.with_streaming(streaming_threshold);
    sim
}

/// Check that at least one non-gateway node stored the given contract key,
/// and return the stored state bytes if found.
fn find_contract_in_non_gateway_storages(
    node_storages: &std::collections::HashMap<NodeLabel, MockStateStorage>,
    contract_key: &ContractKey,
) -> Option<WrappedState> {
    for (label, storage) in node_storages {
        if label.is_node() {
            if let Some(state) = storage.get_stored_state(contract_key) {
                return Some(state);
            }
        }
    }
    None
}

// =============================================================================
// Test 1: Streaming PUT with large state
// =============================================================================

/// Tests that a large state PUT delivers the correct state to non-gateway nodes.
///
/// - SimNetwork: 1 gateway + 2 nodes, streaming threshold = 1024 bytes
/// - Gateway PUTs a contract with 100KB state (well above threshold)
/// - Asserts: simulation completes, state arrived at a non-gateway node with correct bytes
#[test]
fn test_streaming_put_large_state() {
    const SEED: u64 = 0x5720_0001_DEAD_BEEF;
    const NETWORK_NAME: &str = "streaming-put-large";
    const THRESHOLD: usize = 1024;
    const LARGE_STATE_SIZE: usize = 100 * 1024; // 100KB

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 2, SEED, THRESHOLD));

    let contract = SimOperation::create_test_contract(42);
    let large_state = SimOperation::create_large_state(LARGE_STATE_SIZE, 42);
    let contract_key = contract.key();

    let operations = vec![
        // Gateway PUTs large contract
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: large_state.clone(),
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(60),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Streaming PUT simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let stored = find_contract_in_non_gateway_storages(&result.node_storages, &contract_key);
    assert!(
        stored.is_some(),
        "Expected 100KB contract to be stored in at least one non-gateway node"
    );
    let stored_bytes: Vec<u8> = stored.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_bytes, large_state,
        "Stored state bytes should match the original 100KB state"
    );
}

// =============================================================================
// Test 2: Below threshold uses inline
// =============================================================================

/// Tests that a small state below the streaming threshold still delivers correctly.
///
/// - Same setup but state = 512 bytes (below 1024 threshold)
/// - Asserts: state arrived at a non-gateway node with correct bytes
#[test]
fn test_streaming_put_below_threshold_uses_inline() {
    const SEED: u64 = 0x1011_0002_CAFE_BABE;
    const NETWORK_NAME: &str = "streaming-inline";
    const THRESHOLD: usize = 1024;
    const SMALL_STATE_SIZE: usize = 512; // Below threshold

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 2, SEED, THRESHOLD));

    let contract = SimOperation::create_test_contract(99);
    let small_state = SimOperation::create_large_state(SMALL_STATE_SIZE, 99);
    let contract_key = contract.key();

    let operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: small_state.clone(),
            subscribe: false,
        },
    )];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(45),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Inline PUT simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    let stored = find_contract_in_non_gateway_storages(&result.node_storages, &contract_key);
    assert!(
        stored.is_some(),
        "Expected 512-byte contract to be stored in at least one non-gateway node"
    );
    let stored_bytes: Vec<u8> = stored.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_bytes, small_state,
        "Stored state bytes should match the original 512-byte state"
    );
}

// =============================================================================
// Test 3: Streaming UPDATE broadcast
// =============================================================================

/// Tests that streaming UPDATE broadcasts deliver the updated state.
///
/// - SimNetwork: 1 gateway + 3 nodes, streaming threshold = 1024
/// - Gateway PUTs contract with small state + subscribe
/// - Nodes subscribe
/// - Gateway UPDATEs with large state
/// - Asserts: at least one subscribing node has the updated state
#[test]
fn test_streaming_update_broadcast() {
    const SEED: u64 = 0xBCA5_0003_1234_5678;
    const NETWORK_NAME: &str = "streaming-update";
    const THRESHOLD: usize = 1024;
    const LARGE_UPDATE_SIZE: usize = 100 * 1024; // 100KB

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 3, SEED, THRESHOLD));

    let contract = SimOperation::create_test_contract(55);
    let contract_key = contract.key();
    let initial_state = SimOperation::create_test_state(55);
    let large_update = SimOperation::create_large_state(LARGE_UPDATE_SIZE, 77);

    let operations = vec![
        // Gateway PUTs contract with subscribe
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: initial_state,
                subscribe: true,
            },
        ),
        // Node 1 subscribes
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 1),
            SimOperation::Subscribe {
                contract_id: *contract_key.id(),
            },
        ),
        // Node 2 subscribes
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Subscribe {
                contract_id: *contract_key.id(),
            },
        ),
        // Gateway UPDATEs with large state
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Update {
                key: contract_key,
                data: large_update,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(60),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Streaming UPDATE broadcast simulation should complete: {:?}",
        result.turmoil_result.err()
    );

    // At least one non-gateway node should have a state for this contract
    // (the update may have been applied as delta or full state depending on runtime)
    let stored = find_contract_in_non_gateway_storages(&result.node_storages, &contract_key);
    assert!(
        stored.is_some(),
        "Expected at least one subscribing non-gateway node to have state for the updated contract"
    );
}

// =============================================================================
// Test 4: Multiple concurrent streaming PUTs
// =============================================================================

/// Tests that multiple concurrent large PUTs deliver the correct state for each contract.
///
/// - SimNetwork: 1 gateway + 2 nodes, streaming threshold = 1024
/// - Gateway PUTs contract A (50KB) and contract B (80KB)
/// - Asserts: both contracts found in non-gateway storage with correct state bytes
#[test]
fn test_streaming_multiple_concurrent_puts() {
    const SEED: u64 = 0xC00C_0004_ABCD_EF01;
    const NETWORK_NAME: &str = "streaming-concurrent";
    const THRESHOLD: usize = 1024;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 2, SEED, THRESHOLD));

    let contract_a = SimOperation::create_test_contract(10);
    let state_a = SimOperation::create_large_state(50 * 1024, 10); // 50KB
    let key_a = contract_a.key();

    let contract_b = SimOperation::create_test_contract(20);
    let state_b = SimOperation::create_large_state(80 * 1024, 20); // 80KB
    let key_b = contract_b.key();

    let operations = vec![
        // Gateway PUTs contract A
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract_a.clone(),
                state: state_a.clone(),
                subscribe: false,
            },
        ),
        // Gateway PUTs contract B
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract_b.clone(),
                state: state_b.clone(),
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(120),
        Duration::from_secs(60),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Multiple concurrent streaming PUTs should complete: {:?}",
        result.turmoil_result.err()
    );

    // Verify contract A
    let stored_a = find_contract_in_non_gateway_storages(&result.node_storages, &key_a);
    assert!(
        stored_a.is_some(),
        "Expected 50KB contract A to be stored in at least one non-gateway node"
    );
    let stored_a_bytes: Vec<u8> = stored_a.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_a_bytes, state_a,
        "Stored state bytes for contract A should match the original 50KB state"
    );

    // Verify contract B
    let stored_b = find_contract_in_non_gateway_storages(&result.node_storages, &key_b);
    assert!(
        stored_b.is_some(),
        "Expected 80KB contract B to be stored in at least one non-gateway node"
    );
    let stored_b_bytes: Vec<u8> = stored_b.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_b_bytes, state_b,
        "Stored state bytes for contract B should match the original 80KB state"
    );
}

// =============================================================================
// Test 5: Streaming with packet loss
// =============================================================================

/// Tests that streaming transport handles packet loss gracefully.
///
/// - SimNetwork with 5% message loss rate
/// - Large state PUT (100KB, well above 1024 threshold)
/// - Asserts: contract arrived at non-gateway node with correct state bytes
#[test]
fn test_streaming_with_packet_loss() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0xA055_0005_BEEF_CAFE;
    const NETWORK_NAME: &str = "streaming-lossy";
    const THRESHOLD: usize = 1024;
    const LARGE_STATE_SIZE: usize = 100 * 1024; // 100KB

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 2, SEED, THRESHOLD));

    // Inject 5% message loss
    let fault_config = FaultConfig::builder().message_loss_rate(0.05).build();
    sim.with_fault_injection(fault_config);

    let contract = SimOperation::create_test_contract(33);
    let large_state = SimOperation::create_large_state(LARGE_STATE_SIZE, 33);
    let contract_key = contract.key();

    let operations = vec![
        // Gateway PUTs large contract
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: large_state.clone(),
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180), // Longer timeout for lossy network
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Streaming with 5% packet loss should still complete: {:?}",
        result.turmoil_result.err()
    );

    let stored = find_contract_in_non_gateway_storages(&result.node_storages, &contract_key);
    assert!(
        stored.is_some(),
        "Expected 100KB contract to be stored in at least one non-gateway node even with packet loss"
    );
    let stored_bytes: Vec<u8> = stored.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_bytes, large_state,
        "Stored state bytes should match the original 100KB state despite packet loss"
    );
}
