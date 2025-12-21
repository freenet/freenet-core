//! Edge case tests for contract state size boundaries
//!
//! Tests boundary conditions for contract state sizes:
//! - Minimal states (empty, near-zero bytes)
//! - Large states (approaching 1MB)
//! - Maximum allowed states
//!
//! Regression tests for issue #1885 Priority 6 - edge case handling.

use freenet::test_utils::{
    create_large_todo_list, create_minimal_state, load_contract, make_get, make_put, TestContext,
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

/// Test that minimal states (2 bytes) can be stored and retrieved
///
/// Verifies the lower bound of contract state size handling.
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
