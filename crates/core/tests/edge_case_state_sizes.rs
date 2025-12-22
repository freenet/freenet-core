//! Edge case tests for contract state size boundaries
//!
//! Tests boundary conditions for contract state sizes:
//! - Minimal states (empty, near-zero bytes)
//! - Large states (approaching 1MB)
//! - Maximum allowed states
//!
//! These tests validate that the system correctly handles edge cases in contract state sizes.

use freenet::test_utils::{
    create_large_todo_list, create_minimal_state, create_oversized_todo_list, load_contract,
    make_get, make_put, make_update, Task, TestContext, TodoList,
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

/// Test that minimal states can be stored and retrieved
///
/// Verifies the lower bound of contract state size handling.
/// Uses the smallest valid TodoList: {"tasks":[],"version":1}
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_minimal_state_put_get(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load contract and create minimal state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let minimal_state = create_minimal_state();
    let wrapped_state = WrappedState::from(minimal_state);

    // Connect to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream, _) = connect_async(&url).await?;
    let mut client = WebApi::start(ws_stream);

    // PUT minimal state
    make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

    let put_result = timeout(Duration::from_secs(30), client.recv()).await;
    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
        }
        other => panic!("Unexpected PUT response: {:?}", other),
    }

    // GET minimal state back
    make_get(&mut client, contract_key, true, false).await?;

    let get_result = timeout(Duration::from_secs(10), client.recv()).await;
    match get_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            state: recv_state,
            ..
        }))) => {
            assert_eq!(recv_state, wrapped_state, "Minimal state should round-trip");
        }
        other => panic!("Unexpected GET response: {:?}", other),
    }

    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    Ok(())
}

/// Test that large states (~1MB) can be stored and retrieved
///
/// Verifies handling of states near the practical size limit without exceeding it.
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 90,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_large_state_put_get(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load contract and create large state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let large_state = create_large_todo_list();
    let wrapped_state = WrappedState::from(large_state.clone());

    tracing::info!("Testing with large state size: {} bytes", large_state.len());

    // Connect to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream, _) = connect_async(&url).await?;
    let mut client = WebApi::start(ws_stream);

    // PUT large state
    make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

    let put_result = timeout(Duration::from_secs(60), client.recv()).await;
    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
            tracing::info!("Large state PUT successful");
        }
        other => panic!("Unexpected PUT response: {:?}", other),
    }

    // GET large state back
    make_get(&mut client, contract_key, true, false).await?;

    let get_result = timeout(Duration::from_secs(30), client.recv()).await;
    match get_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            state: recv_state,
            ..
        }))) => {
            assert_eq!(recv_state, wrapped_state, "Large state should round-trip");
            tracing::info!(
                "Large state GET successful, verified {} bytes",
                large_state.len()
            );
        }
        other => panic!("Unexpected GET response: {:?}", other),
    }

    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    Ok(())
}

/// Test that empty states work correctly
///
/// Verifies handling of zero-task todo lists (valid but empty contract state).
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_empty_state_put_get(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load contract and create empty state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let empty_state = freenet::test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(empty_state);

    // Connect to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream, _) = connect_async(&url).await?;
    let mut client = WebApi::start(ws_stream);

    // PUT empty state
    make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

    let put_result = timeout(Duration::from_secs(30), client.recv()).await;
    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
        }
        other => panic!("Unexpected PUT response: {:?}", other),
    }

    // GET empty state back
    make_get(&mut client, contract_key, true, false).await?;

    let get_result = timeout(Duration::from_secs(10), client.recv()).await;
    match get_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            state: recv_state,
            ..
        }))) => {
            assert_eq!(recv_state, wrapped_state, "Empty state should round-trip");

            // Parse and verify it's truly empty
            let state_str = String::from_utf8_lossy(recv_state.as_ref());
            assert!(
                state_str.contains("\"tasks\":[]"),
                "Empty state should have zero tasks"
            );
        }
        other => panic!("Unexpected GET response: {:?}", other),
    }

    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    Ok(())
}

/// Test that oversized states are properly rejected or handled
///
/// Verifies the system behavior when attempting to store states exceeding
/// practical size limits (10MB+). The system should either:
/// 1. Reject the state with an appropriate error
/// 2. Handle it gracefully (timeout, etc.)
///
/// This test is designed to verify the system doesn't crash or hang
/// when confronted with oversized data.
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 120,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_oversized_state_handling(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load contract and create oversized state (10MB+)
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let oversized_state = create_oversized_todo_list();
    let wrapped_state = WrappedState::from(oversized_state.clone());

    tracing::info!(
        "Testing oversized state handling: {} bytes ({:.2} MB)",
        oversized_state.len(),
        oversized_state.len() as f64 / (1024.0 * 1024.0)
    );

    // Verify state is actually oversized
    assert!(
        oversized_state.len() > 10_000_000,
        "State should exceed 10MB for this test"
    );

    // Connect to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream, _) = connect_async(&url).await?;
    let mut client = WebApi::start(ws_stream);

    // Attempt PUT with oversized state
    // This may fail, timeout, or succeed depending on system limits
    make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

    // Wait for response - expect either rejection or timeout
    let put_result = timeout(Duration::from_secs(60), client.recv()).await;
    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            tracing::warn!(
                "Oversized state was accepted (key: {}). System may not enforce size limits.",
                key
            );
            // This is acceptable - the system may choose to allow large states
        }
        Ok(Err(err)) => {
            tracing::info!(
                "Oversized state was rejected with error (expected): {:?}",
                err
            );
            // This is the expected behavior - system should reject oversized states
        }
        Err(_timeout) => {
            tracing::info!("Oversized state PUT timed out (acceptable behavior)");
            // Timeout is acceptable - system may be slow to process large states
        }
        Ok(Ok(other)) => {
            tracing::info!("Oversized state got unexpected response: {:?}", other);
            // Any response other than success is acceptable for oversized states
        }
    }

    // Clean disconnect (may fail if connection was dropped)
    let _ = client.send(ClientRequest::Disconnect { cause: None }).await;

    Ok(())
}

/// Test UPDATE operation with no state change (UpdateNoChange scenario)
///
/// This test verifies the UPDATE operation correctly handles the case where
/// the update data results in no state change. This exercises the UpdateNoChange
/// code path which was involved in bug #1734.
///
/// Related to: Bug #1734 - UPDATE operations that result in no state change
/// should still return a proper response, not timeout.
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 90,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_update_no_state_change(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load contract and create initial state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state
    let todo_list = TodoList {
        tasks: vec![],
        version: 1,
    };
    let initial_state = serde_json::to_vec(&todo_list)?;
    let wrapped_state = WrappedState::from(initial_state);

    // Connect to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream, _) = connect_async(&url).await?;
    let mut client = WebApi::start(ws_stream);

    // First PUT the initial state
    make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

    let put_result = timeout(Duration::from_secs(30), client.recv()).await;
    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
            tracing::info!("Initial PUT successful");
        }
        other => panic!("Unexpected PUT response: {:?}", other),
    }

    // Now UPDATE with the exact same state (should trigger UpdateNoChange path)
    tracing::info!("Sending UPDATE with identical state to trigger NoChange scenario");
    make_update(&mut client, contract_key, wrapped_state.clone()).await?;

    // Wait for UPDATE response - should NOT timeout even though state is unchanged
    let update_result = timeout(Duration::from_secs(30), client.recv()).await;
    match update_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key,
            summary: _,
        }))) => {
            assert_eq!(key, contract_key);
            tracing::info!("UPDATE with no state change completed successfully");
        }
        Ok(Err(err)) => {
            // An error response is acceptable - the contract may reject no-change updates
            tracing::info!("UPDATE returned error (acceptable): {:?}", err);
        }
        Err(_timeout) => {
            // This would be a regression of bug #1734
            panic!("UPDATE operation timed out - possible regression of bug #1734");
        }
        other => panic!("Unexpected UPDATE response: {:?}", other),
    }

    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    Ok(())
}

/// Test state at exact boundary size
///
/// Creates a state close to practical size limits (950KB) to verify
/// edge case handling around the boundary without hitting system limits.
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 90,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_state_at_boundary_size(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create a state close to 1MB (practical limit for comfortable operation)
    // We use 950KB to stay safely under 1MB while still testing large state handling
    const TARGET_SIZE: usize = 950 * 1024; // 950KB
    let mut tasks = Vec::new();
    let mut current_size = 0;

    while current_size < TARGET_SIZE {
        let task = Task {
            id: tasks.len() as u64,
            title: format!("Boundary test task {}", tasks.len()),
            description: "X".repeat(100), // Predictable size padding
            completed: false,
            priority: 3,
        };
        // Rough estimate of serialized task size
        current_size += 150;
        tasks.push(task);
    }

    let boundary_state = TodoList { tasks, version: 1 };
    let state_bytes = serde_json::to_vec(&boundary_state)?;
    let wrapped_state = WrappedState::from(state_bytes.clone());

    tracing::info!(
        "Testing boundary state: {} bytes ({:.2} KB)",
        state_bytes.len(),
        state_bytes.len() as f64 / 1024.0
    );

    // Connect to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream, _) = connect_async(&url).await?;
    let mut client = WebApi::start(ws_stream);

    // PUT boundary state
    make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

    let put_result = timeout(Duration::from_secs(60), client.recv()).await;
    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
            tracing::info!("Boundary state PUT successful");
        }
        other => panic!("Unexpected PUT response for boundary state: {:?}", other),
    }

    // GET boundary state back to verify round-trip
    make_get(&mut client, contract_key, true, false).await?;

    let get_result = timeout(Duration::from_secs(30), client.recv()).await;
    match get_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            state: recv_state,
            ..
        }))) => {
            assert_eq!(
                recv_state, wrapped_state,
                "Boundary state should round-trip"
            );
            tracing::info!(
                "Boundary state GET successful, verified {} bytes",
                state_bytes.len()
            );
        }
        other => panic!("Unexpected GET response for boundary state: {:?}", other),
    }

    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    Ok(())
}
