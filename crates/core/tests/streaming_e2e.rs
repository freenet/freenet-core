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
//! Enable with: cargo test -p freenet --features "simulation_tests,testing" --test streaming_e2e

#![cfg(feature = "simulation_tests")]

use freenet::config::{GlobalRng, GlobalSimulationTime};
use freenet::dev_tool::{
    MockStateStorage, NodeLabel, ScheduledOperation, SimNetwork, SimOperation,
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
    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));

    let mut sim = SimNetwork::new(
        name, gateways, nodes, 7,  // ring_max_htl
        3,  // rnd_if_htl_above
        10, // max_connections
        2,  // min_connections
        seed,
    )
    .await;
    sim.with_streaming_threshold(streaming_threshold);
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

// =============================================================================
// Test 6: Streaming with packet reordering
// =============================================================================

/// Tests that streaming handles packet reordering via variable latency.
///
/// - SimNetwork: 1 gateway + 2 nodes, streaming threshold = 1024
/// - Fault injection: variable latency 10ms..100ms (causes reordering)
/// - Gateway PUTs a 100KB contract
/// - Asserts: correct state arrives despite fragment reordering
///
/// LockFreeStreamBuffer indexes by fragment number, so reordering should be
/// handled transparently.
#[test]
fn test_streaming_with_packet_reordering() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0xBE0F_0006_DEAD_1234;
    const NETWORK_NAME: &str = "streaming-reorder";
    const THRESHOLD: usize = 1024;
    const LARGE_STATE_SIZE: usize = 100 * 1024; // 100KB

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 2, SEED, THRESHOLD));

    // Inject variable latency to cause packet reordering
    let fault_config = FaultConfig::builder()
        .latency_range(Duration::from_millis(10)..Duration::from_millis(100))
        .build();
    sim.with_fault_injection(fault_config);

    let contract = SimOperation::create_test_contract(66);
    let large_state = SimOperation::create_large_state(LARGE_STATE_SIZE, 66);
    let contract_key = contract.key();

    let operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: large_state.clone(),
            subscribe: false,
        },
    )];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Streaming with packet reordering should complete: {:?}",
        result.turmoil_result.err()
    );

    let stored = find_contract_in_non_gateway_storages(&result.node_storages, &contract_key);
    assert!(
        stored.is_some(),
        "Expected 100KB contract to be stored despite packet reordering"
    );
    let stored_bytes: Vec<u8> = stored.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_bytes, large_state,
        "Stored state bytes should match the original 100KB state despite reordering"
    );
}

// =============================================================================
// Test 7: Multi-hop forwarding
// =============================================================================

/// Tests streaming through multi-hop forwarding with a larger network.
///
/// - SimNetwork: 1 gateway + 4 nodes, streaming threshold = 1024
/// - Gateway PUTs a 200KB contract
/// - With 4 nodes, the probability of multi-hop routing is higher
/// - Asserts: at least one non-gateway node received the contract
#[test]
fn test_streaming_multi_hop_forwarding() {
    const SEED: u64 = 0xF1A7_0007_CAFE_9876;
    const NETWORK_NAME: &str = "streaming-multihop";
    const THRESHOLD: usize = 1024;
    const LARGE_STATE_SIZE: usize = 200 * 1024; // 200KB

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 4, SEED, THRESHOLD));

    let contract = SimOperation::create_test_contract(88);
    let large_state = SimOperation::create_large_state(LARGE_STATE_SIZE, 88);
    let contract_key = contract.key();

    let operations = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: large_state.clone(),
            subscribe: false,
        },
    )];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(180),
        Duration::from_secs(90),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Multi-hop streaming PUT should complete: {:?}",
        result.turmoil_result.err()
    );

    let stored = find_contract_in_non_gateway_storages(&result.node_storages, &contract_key);
    assert!(
        stored.is_some(),
        "Expected 200KB contract to be stored in at least one non-gateway node via multi-hop"
    );
    let stored_bytes: Vec<u8> = stored.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_bytes, large_state,
        "Stored state bytes should match the original 200KB state"
    );
}

// =============================================================================
// Test 8: Streaming GET through relay hop
// =============================================================================

/// Tests that a large contract can be retrieved via streaming GET through relay nodes.
///
/// This exercises the relay pipe_stream path, which was the code path responsible
/// for the streaming hang bug (#3608). The scenario:
///
/// 1. Gateway PUTs a ~1MB contract (stored at nodes near its ring location)
/// 2. A different node GETs the contract, routing through intermediate relay nodes
/// 3. The relay uses pipe_stream to forward the streaming GET response
///
/// With 1 gateway + 4 nodes, the GET request from a non-storing node must relay
/// through at least one intermediate node, exercising the pipe_stream forwarding
/// path that the cwnd timeout protects.
#[test]
fn test_streaming_get_through_relay() {
    const SEED: u64 = 0xDE1A_0008_FACE_B00C;
    const NETWORK_NAME: &str = "streaming-get-relay";
    const THRESHOLD: usize = 1024;
    const LARGE_STATE_SIZE: usize = 1024 * 1024; // ~1MB, similar to River UI container

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 4, SEED, THRESHOLD));

    let contract = SimOperation::create_test_contract(0xAB);
    let large_state = SimOperation::create_large_state(LARGE_STATE_SIZE, 0xAB);
    let contract_key = contract.key();
    let contract_id = *contract_key.id();

    let operations = vec![
        // Gateway PUTs 1MB contract
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: large_state.clone(),
                subscribe: false,
            },
        ),
        // A different node GETs the contract — must relay through network
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 3),
            SimOperation::Get {
                contract_id,
                return_contract_code: false,
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300), // Longer timeout for 1MB streaming GET
        Duration::from_secs(120),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Streaming GET through relay should complete: {:?}",
        result.turmoil_result.err()
    );

    // The GET-requesting node should have the contract state
    let node3_label = NodeLabel::node(NETWORK_NAME, 3);
    let node3_storage = result
        .node_storages
        .get(&node3_label)
        .expect("node 3 should have a storage handle");
    let node3_state = node3_storage.get_stored_state(&contract_key);

    assert!(
        node3_state.is_some(),
        "Node 3 should have 1MB contract state after streaming GET through relay"
    );
    let stored_bytes: Vec<u8> = node3_state.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_bytes, large_state,
        "Stored state bytes should match the original 1MB state after relay GET"
    );
}

/// Regression test: 1MB streaming GET with packet loss.
///
/// This is the test that was missing and would have caught the loss_pause margin
/// bug in v0.2.22. The existing tests covered:
/// - 100KB streaming with 5% loss (too small for stall to manifest)
/// - 1MB streaming GET without loss (no loss_pause triggered)
///
/// This test combines both: a 1MB transfer that triggers streaming, with 5%
/// packet loss that triggers loss_pause recovery. With the old 2-packet margin,
/// this test would timeout because the sender stalls for 20s per loss event.
/// With the 50-packet margin, recovery completes quickly.
#[test]
fn test_streaming_get_1mb_with_packet_loss() {
    use freenet::simulation::FaultConfig;

    const SEED: u64 = 0xDE1A_000A_DEAD_BEEF;
    const NETWORK_NAME: &str = "streaming-get-lossy-1mb";
    const THRESHOLD: usize = 1024;
    const LARGE_STATE_SIZE: usize = 1024 * 1024; // 1MB — similar to River UI contract

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 4, SEED, THRESHOLD));

    // 5% message loss — enough to trigger loss_pause during the ~833-packet transfer
    let fault_config = FaultConfig::builder().message_loss_rate(0.05).build();
    sim.with_fault_injection(fault_config);

    let contract = SimOperation::create_test_contract(0xDF);
    let large_state = SimOperation::create_large_state(LARGE_STATE_SIZE, 0xDF);
    let contract_key = contract.key();
    let contract_id = *contract_key.id();

    let operations = vec![
        // Gateway PUTs 1MB contract
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: large_state.clone(),
                subscribe: false,
            },
        ),
        // A different node GETs the contract — must relay through network
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 3),
            SimOperation::Get {
                contract_id,
                return_contract_code: false,
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300), // Generous timeout for lossy 1MB transfer
        Duration::from_secs(120),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "1MB streaming GET with 5% packet loss should complete without stalling: {:?}",
        result.turmoil_result.err()
    );

    // The GET-requesting node should have the correct 1MB state
    let node3_label = NodeLabel::node(NETWORK_NAME, 3);
    let node3_storage = result
        .node_storages
        .get(&node3_label)
        .expect("node 3 should have a storage handle");
    let node3_state = node3_storage.get_stored_state(&contract_key);

    assert!(
        node3_state.is_some(),
        "Node 3 should have 1MB contract state after streaming GET with packet loss"
    );
    let stored_bytes: Vec<u8> = node3_state.unwrap().as_ref().to_vec();
    assert_eq!(
        stored_bytes, large_state,
        "Stored state bytes should match the original 1MB state despite packet loss"
    );
}

/// Regression test for #3704: streaming GET path must trigger auto-subscribe
/// and hosting announcement so that subsequent GETs are served from local cache.
///
/// Before the fix, the streaming GET completion path only called `record_get_access()`
/// but skipped `announce_contract_hosted()` and `start_subscription_request()`, so
/// `is_receiving_updates()` returned false and every subsequent GET hit the network.
#[test]
fn test_streaming_get_triggers_auto_subscribe() {
    const SEED: u64 = 0xDE1A_0009_CAFE_F00D;
    const NETWORK_NAME: &str = "streaming-get-auto-subscribe";
    const THRESHOLD: usize = 1024;
    const LARGE_STATE_SIZE: usize = 100 * 1024; // 100KB, above streaming threshold

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let sim = rt.block_on(setup_streaming_network(NETWORK_NAME, 1, 3, SEED, THRESHOLD));

    let contract = SimOperation::create_test_contract(0xCC);
    let large_state = SimOperation::create_large_state(LARGE_STATE_SIZE, 0xCC);
    let contract_key = contract.key();
    let contract_id = *contract_key.id();

    let operations = vec![
        // Gateway PUTs 100KB contract
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: large_state.clone(),
                subscribe: true,
            },
        ),
        // Node 2 GETs the contract (will be streamed since >1KB threshold).
        // subscribe=false here — we're relying on AUTO_SUBSCRIBE_ON_GET to kick in.
        ScheduledOperation::new(
            NodeLabel::node(NETWORK_NAME, 2),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ),
    ];

    let result = sim.run_controlled_simulation(
        SEED,
        operations,
        Duration::from_secs(300),
        Duration::from_secs(120),
    );

    assert!(
        result.turmoil_result.is_ok(),
        "Streaming GET should complete: {:?}",
        result.turmoil_result.err()
    );

    // Verify the GET-requesting node has the contract state
    let node2_label = NodeLabel::node(NETWORK_NAME, 2);
    let node2_storage = result
        .node_storages
        .get(&node2_label)
        .expect("node 2 should have a storage handle");
    let node2_state = node2_storage.get_stored_state(&contract_key);
    assert!(
        node2_state.is_some(),
        "Node 2 should have contract state after streaming GET"
    );

    // Key assertion: the GET-requesting node should be HOSTING the contract
    // (i.e., announce_contract_hosted was called, not just record_get_access)
    let node2_snapshot = result.topology_snapshots.iter().find(|s| {
        s.contracts
                .values()
                .any(|c| c.contract_key == contract_key && c.is_hosting)
                // Match by checking this is node 2's snapshot (not gateway)
                && s.peer_addr.ip() != std::net::IpAddr::from([1u8, 0, 0, 1])
    });

    assert!(
        node2_snapshot.is_some(),
        "After streaming GET, the requesting node should be hosting the contract. \
         Topology snapshots: {:#?}",
        result
            .topology_snapshots
            .iter()
            .map(|s| (
                s.peer_addr,
                s.contracts
                    .values()
                    .map(|c| (&c.contract_key, c.is_hosting, c.upstream.is_some()))
                    .collect::<Vec<_>>()
            ))
            .collect::<Vec<_>>()
    );
}

// =============================================================================
// Regression test for #1454 Phase 3b — driver must handle streaming GETs.
//
// The existing streaming tests above all satisfy the GETting node's request
// via the client-events local-cache shortcut (`client_events.rs:1108-1154`)
// because the gateway's `SimOperation::Put` propagates the contract to
// neighbors during PUT, leaving the GETting node with a relay-cached copy
// BEFORE the GET fires. That shortcut returns without ever calling
// `start_client_get`, so the driver's `Terminal::Streaming` path is
// untested.
//
// This test uses `SimOperation::SeedContract`, which seeds only the gateway's
// local store without any network propagation. Node 3 then cold-GETs — the
// local-cache shortcut misses, the request flows through the task-per-tx
// driver, and the terminal reply arrives as `ResponseStreaming` because the
// payload is above the streaming threshold. The driver's `Done` arm must
// produce a client-visible `HostResponse::GetResponse` with the correct
// state AND write the state to node 3's local store for subsequent access.
//
// Without the fix, `Terminal::Streaming` in the driver does not write the
// store (the bypass skips `process_message`, and
// `stream_handle.assemble()` never runs on the originator under
// task-per-tx), so `build_host_response`'s re-query returns `None` and the
// client receives an `OperationError`. Node 3's storage check then fails.
// =============================================================================

/// Set up a SimNetwork with a LOW ring_max_htl so PUT fan-out can't
/// reach the GETting node — the setup that forces a cold-cache GET
/// through the task-per-tx driver.
///
/// `setup_streaming_network` uses `ring_max_htl=7` which is enough
/// for PUT to propagate to every node in small topologies, making the
/// GETter's `client_events.rs` local-cache shortcut always satisfy
/// the request.
async fn setup_htl1_network(name: &str, nodes: usize, seed: u64, threshold: usize) -> SimNetwork {
    GlobalRng::set_seed(seed);
    const BASE_EPOCH_MS: u64 = 1577836800000;
    const RANGE_MS: u64 = 5 * 365 * 24 * 60 * 60 * 1000;
    GlobalSimulationTime::set_time_ms(BASE_EPOCH_MS + (seed % RANGE_MS));

    let mut sim = SimNetwork::new(
        name, 1,     // 1 gateway
        nodes, // N nodes
        1,     // ring_max_htl = 1 — PUT forwards at most once past the originator
        1,     // rnd_if_htl_above
        10,    // max_connections
        2,     // min_connections
        seed,
    )
    .await;
    sim.with_streaming_threshold(threshold);
    sim
}

/// Cold-cache streaming GET via task-per-tx driver.
///
/// Scope: bug #1 from the #3884 skeptical review — the driver's
/// `Terminal::Streaming` path does not write the contract state to the
/// local store, so any client GET of a >threshold contract from a node
/// that has no relay-cached copy returns `OperationError` on the client
/// channel and leaves local storage empty.
///
/// ## Driver isolation strategy
///
/// Uses `ring_max_htl = 1` so the gateway's PUT propagates at most one
/// hop. In a 10-node topology, at least one node (the one furthest
/// from the gateway) doesn't receive the state during PUT. That node's
/// cold GET falls through the `client_events.rs:1108` shortcut
/// (since `local_satisfies_request = false`) and invokes the
/// task-per-tx driver. Verification via `GET_DRIVER_CALL_COUNT`.
///
/// ## Known infrastructure gap
///
/// Currently `#[ignore]`'d because even with the driver correctly
/// reached, the HTL=1 topology used to force cold-cache leaves the
/// driver unable to route (driver retries exhaust with "channel
/// closed"). The driver fix itself is validated by unit tests in
/// `operations/get/op_ctx_task.rs`; the remaining work is test
/// infrastructure (either a SimNetwork helper that sets up a more
/// routable cold-cache topology, or a `SimOperation::SeedContract`
/// variant that also announces hosting). Tracked in #3883.
#[ignore = "Infrastructure gap — HTL=1 topology isn't routable; see docstring and #3883"]
#[test]
fn test_driver_streaming_get_cold_cache() {
    use freenet::dev_tool::GET_DRIVER_CALL_COUNT;
    use std::sync::atomic::Ordering;

    const SEED: u64 = 0xDE1A_0B0B_C01D_CA7E;
    const NETWORK_NAME_PHASE1: &str = "driver-streaming-cold-cache-p1";
    const NETWORK_NAME_PHASE2: &str = "driver-streaming-cold-cache-p2";
    const THRESHOLD: usize = 1024;
    const LARGE_STATE_SIZE: usize = 100 * 1024; // 100KB, above THRESHOLD
    const NODES: usize = 10;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let contract = SimOperation::create_test_contract(0xBC);
    let large_state = SimOperation::create_large_state(LARGE_STATE_SIZE, 0xBC);
    let contract_key = contract.key();
    let contract_id = *contract_key.id();

    // Phase 1: run the PUT in isolation to discover which nodes
    // DIDN'T receive the state during PUT fan-out (with HTL=1 and
    // 10 nodes, at least one should be cold).
    let sim1 = rt.block_on(setup_htl1_network(
        NETWORK_NAME_PHASE1,
        NODES,
        SEED,
        THRESHOLD,
    ));
    let phase1_ops = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME_PHASE1, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: large_state.clone(),
            subscribe: false,
        },
    )];
    let phase1 = sim1.run_controlled_simulation(
        SEED,
        phase1_ops,
        Duration::from_secs(120),
        Duration::from_secs(30),
    );
    assert!(
        phase1.turmoil_result.is_ok(),
        "Phase 1 (PUT only) should complete: {:?}",
        phase1.turmoil_result.err()
    );
    let cold_node_idx = (1..=NODES).find(|i| {
        let label = NodeLabel::node(NETWORK_NAME_PHASE1, *i);
        phase1
            .node_storages
            .get(&label)
            .is_some_and(|s| s.get_stored_state(&contract_key).is_none())
    });
    let Some(cold_idx) = cold_node_idx else {
        panic!(
            "Test precondition: no cold-cache node exists after HTL=1 PUT \
             in {NODES}-node topology. All nodes received the state during \
             fan-out, which contradicts HTL=1. Try a larger topology."
        );
    };

    // Phase 2: fresh network with the same topology (same seed →
    // same ring), gateway PUTs then the cold node GETs. The cold
    // node's local cache is empty → `local_satisfies_request=false`
    // → `client_events.rs` falls through to the driver.
    let baseline_calls = GET_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    let sim2 = rt.block_on(setup_htl1_network(
        NETWORK_NAME_PHASE2,
        NODES,
        SEED,
        THRESHOLD,
    ));
    let cold_node_label = NodeLabel::node(NETWORK_NAME_PHASE2, cold_idx);
    let phase2_ops = vec![
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME_PHASE2, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: large_state.clone(),
                subscribe: false,
            },
        ),
        ScheduledOperation::new(
            cold_node_label.clone(),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ),
    ];
    let phase2 = sim2.run_controlled_simulation(
        SEED,
        phase2_ops,
        Duration::from_secs(300),
        Duration::from_secs(120),
    );
    assert!(
        phase2.turmoil_result.is_ok(),
        "Phase 2 (PUT + cold GET) should complete: {:?}",
        phase2.turmoil_result.err()
    );

    // Driver-isolation probe: confirm the GET actually invoked the
    // task-per-tx driver (and wasn't short-circuited by
    // `client_events.rs`'s local-cache path).
    let driver_calls = GET_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    assert!(
        driver_calls > baseline_calls,
        "Test infrastructure failure: GET_DRIVER_CALL_COUNT did not advance during \
         phase 2. Either node {cold_idx} still had a relay-cached copy of the \
         contract (phase-1 topology diverged from phase-2 topology), or the \
         local-cache shortcut fired for some other reason. Without driver \
         invocation this test does not exercise bug #1 from PR #3884."
    );

    // Bug #1 regression: after the driver-routed streaming GET, the
    // cold-cache node must have stored the contract state locally.
    let storage = phase2
        .node_storages
        .get(&cold_node_label)
        .expect("cold-cache node should have a storage handle");
    let stored = storage.get_stored_state(&contract_key);
    assert!(
        stored.is_some(),
        "Bug #1 regression: cold-cache streaming GET through the driver did \
         not write the contract state to local storage on {cold_node_label:?}. \
         The driver's Terminal::Streaming path must ensure local caching."
    );
    assert_eq!(
        stored.unwrap().as_ref().to_vec(),
        large_state,
        "Stored state on {cold_node_label:?} must match the PUT state"
    );
}

/// Cold-cache non-streaming GET must auto-subscribe at originator.
///
/// Scope: bug #2 from the #3884 skeptical review — the driver never calls
/// `auto_subscribe_on_get_response`, so a client GET with `subscribe=false`
/// against a cold cache silently skips the AUTO_SUBSCRIBE_ON_GET fallback
/// that the legacy `process_message` branch would have invoked.
///
/// ## Driver isolation strategy
///
/// Same HTL=1 approach as `test_driver_streaming_get_cold_cache`.
/// Phase 1 discovers a cold node; phase 2 GETs from it with a small
/// (inline, not streamed) payload; asserts that the driver fired
/// (via `GET_DRIVER_CALL_COUNT`) AND that the cold node ended up
/// with an active subscription (AUTO_SUBSCRIBE_ON_GET fallback).
///
/// Same infrastructure gap as above — currently `#[ignore]`'d.
#[ignore = "Infrastructure gap — HTL=1 topology isn't routable; see docstring and #3883"]
#[test]
fn test_driver_inline_get_triggers_auto_subscribe() {
    use freenet::dev_tool::GET_DRIVER_CALL_COUNT;
    use std::sync::atomic::Ordering;

    const SEED: u64 = 0xDE1A_0B0C_A570_5C2B;
    const NETWORK_NAME_PHASE1: &str = "driver-inline-auto-sub-p1";
    const NETWORK_NAME_PHASE2: &str = "driver-inline-auto-sub-p2";
    const THRESHOLD: usize = 1024;
    // State well below THRESHOLD — forces inline Response, not streaming.
    const SMALL_STATE_SIZE: usize = 128;
    const NODES: usize = 10;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let contract = SimOperation::create_test_contract(0xBD);
    let small_state = SimOperation::create_large_state(SMALL_STATE_SIZE, 0xBD);
    let contract_key = contract.key();
    let contract_id = *contract_key.id();

    // Phase 1: discover which node doesn't receive the PUT.
    let sim1 = rt.block_on(setup_htl1_network(
        NETWORK_NAME_PHASE1,
        NODES,
        SEED,
        THRESHOLD,
    ));
    let phase1_ops = vec![ScheduledOperation::new(
        NodeLabel::gateway(NETWORK_NAME_PHASE1, 0),
        SimOperation::Put {
            contract: contract.clone(),
            state: small_state.clone(),
            subscribe: false,
        },
    )];
    let phase1 = sim1.run_controlled_simulation(
        SEED,
        phase1_ops,
        Duration::from_secs(120),
        Duration::from_secs(30),
    );
    assert!(
        phase1.turmoil_result.is_ok(),
        "Phase 1 (PUT only) should complete: {:?}",
        phase1.turmoil_result.err()
    );
    let cold_node_idx = (1..=NODES).find(|i| {
        let label = NodeLabel::node(NETWORK_NAME_PHASE1, *i);
        phase1
            .node_storages
            .get(&label)
            .is_some_and(|s| s.get_stored_state(&contract_key).is_none())
    });
    let Some(cold_idx) = cold_node_idx else {
        panic!(
            "Test precondition: no cold-cache node exists after HTL=1 PUT \
             in {NODES}-node topology."
        );
    };

    // Phase 2: fresh network, gateway PUTs + cold node GETs
    // (subscribe=false, inline payload).
    let baseline_calls = GET_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    let sim2 = rt.block_on(setup_htl1_network(
        NETWORK_NAME_PHASE2,
        NODES,
        SEED,
        THRESHOLD,
    ));
    let cold_node_label = NodeLabel::node(NETWORK_NAME_PHASE2, cold_idx);
    let phase2_ops = vec![
        ScheduledOperation::new(
            NodeLabel::gateway(NETWORK_NAME_PHASE2, 0),
            SimOperation::Put {
                contract: contract.clone(),
                state: small_state.clone(),
                subscribe: false,
            },
        ),
        ScheduledOperation::new(
            cold_node_label.clone(),
            SimOperation::Get {
                contract_id,
                return_contract_code: true,
                subscribe: false,
            },
        ),
    ];
    let phase2 = sim2.run_controlled_simulation(
        SEED,
        phase2_ops,
        Duration::from_secs(180),
        Duration::from_secs(90),
    );
    assert!(
        phase2.turmoil_result.is_ok(),
        "Phase 2 (PUT + cold GET) should complete: {:?}",
        phase2.turmoil_result.err()
    );

    // Driver-isolation probe.
    let driver_calls = GET_DRIVER_CALL_COUNT.load(Ordering::SeqCst);
    assert!(
        driver_calls > baseline_calls,
        "Test infrastructure failure: GET_DRIVER_CALL_COUNT did not advance \
         during phase 2. The cold node {cold_idx} must have routed through \
         the task-per-tx driver."
    );

    // Precondition: the driver-routed GET stored state locally.
    let storage = phase2
        .node_storages
        .get(&cold_node_label)
        .expect("cold-cache node should have a storage handle");
    assert!(
        storage.get_stored_state(&contract_key).is_some(),
        "Cold-cache node should have stored the contract state after the GET"
    );

    // Bug #2 regression: after the driver-routed GET with
    // subscribe=false, the cold node should have auto-subscribed.
    // AUTO_SUBSCRIBE_ON_GET = true in ring.rs:60.
    let auto_subscribed = phase2
        .topology_snapshots
        .iter()
        .any(|s| s.active_subscription_keys.contains(&contract_id));

    assert!(
        auto_subscribed,
        "Bug #2 regression: cold-cache GET through the driver did not \
         auto-subscribe the requesting node {cold_node_label:?}. The \
         driver's Done arm must invoke `auto_subscribe_on_get_response` \
         for successful GETs when the client did not explicitly set \
         `subscribe=true`. Active subscriptions: {:#?}",
        phase2
            .topology_snapshots
            .iter()
            .map(|s| (s.peer_addr, s.active_subscription_keys.clone()))
            .collect::<Vec<_>>()
    );
}
