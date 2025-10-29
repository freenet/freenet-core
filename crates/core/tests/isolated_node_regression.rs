//! Regression tests for isolated node functionality
//!
//! These tests ensure that isolated nodes (nodes with no peer connections) operate correctly:
//! 1. PUT operations cache contracts locally without network timeouts
//! 2. GET operations retrieve from local cache without self-routing attempts
//! 3. Complete PUT→GET workflow functions properly on isolated nodes
//! 4. SUBSCRIBE operations complete successfully for local contracts
//! 5. UPDATE operations complete successfully for local contracts

use freenet::test_utils::{
    load_contract, make_get, make_put, make_subscribe, make_update, TestContext,
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

/// Test complete PUT-then-GET workflow on isolated node
///
/// This regression test verifies isolated nodes operate correctly:
/// - PUT operations cache contracts locally without network timeouts (PR #1781)
/// - GET operations retrieve from local cache without self-routing attempts (PR #1806)
/// - Complete workflow functions properly without peer connections
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_isolated_node_put_get_workflow(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load test contract and state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = freenet::test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Connect to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream, _) = connect_async(&url).await?;
    let mut client = WebApi::start(ws_stream);

    println!("Step 1: Performing PUT operation to cache contract locally");

    // Perform PUT operation - this should cache the contract locally
    let put_start = std::time::Instant::now();
    make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

    // Wait for PUT response
    let put_result = timeout(Duration::from_secs(30), client.recv()).await;
    let put_elapsed = put_start.elapsed();

    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
            println!("PUT operation successful in {:?}", put_elapsed);
        }
        Ok(Ok(other)) => {
            panic!("Unexpected PUT response: {:?}", other);
        }
        Ok(Err(e)) => {
            panic!("PUT operation failed: {}", e);
        }
        Err(_) => {
            panic!("PUT operation timed out");
        }
    }

    println!("Contract verified in local cache");

    println!("Step 2: Performing GET operation using local cache");

    // Now perform GET operation - should use local cache without self-routing
    let get_start = std::time::Instant::now();
    make_get(&mut client, contract_key, true, false).await?;

    // Wait for GET response
    let get_result = timeout(Duration::from_secs(10), client.recv()).await;
    let get_elapsed = get_start.elapsed();

    // REGRESSION TEST: Verify GET completed quickly using local cache
    assert!(
        get_elapsed < Duration::from_secs(5),
        "GET from local cache should be fast, not hanging on self-routing. Elapsed: {:?}",
        get_elapsed
    );

    // Verify GET succeeded with correct data
    match get_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            contract: recv_contract,
            state: recv_state,
            ..
        }))) => {
            assert_eq!(
                recv_contract
                    .as_ref()
                    .expect("Contract should be present")
                    .key(),
                contract_key
            );
            assert_eq!(recv_state, wrapped_state);
            println!(
                "GET operation successful from local cache in {:?}",
                get_elapsed
            );
        }
        Ok(Ok(other)) => {
            panic!("Unexpected GET response: {:?}", other);
        }
        Ok(Err(e)) => {
            panic!("GET operation failed: {}", e);
        }
        Err(_) => {
            panic!("GET operation timed out");
        }
    }

    println!("PUT-then-GET workflow completed successfully without self-routing");

    // Properly close the client
    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Test concurrent GET operations to reproduce deduplication race condition (issue #1886)
///
/// This test attempts to reproduce the race condition where:
/// 1. Client 1 sends GET request → Router creates operation with TX
/// 2. Operation completes instantly (contract cached locally)
/// 3. Result delivered to Client 1, TX removed from tracking
/// 4. Client 2 sends identical GET request → Router tries to reuse removed TX
/// 5. Bug: Client 2 never receives response
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_concurrent_get_deduplication_race(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load a small test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = freenet::test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );

    // Connect multiple clients
    let (ws_stream1, _) = connect_async(&url).await?;
    let mut client1 = WebApi::start(ws_stream1);

    let (ws_stream2, _) = connect_async(&url).await?;
    let mut client2 = WebApi::start(ws_stream2);

    let (ws_stream3, _) = connect_async(&url).await?;
    let mut client3 = WebApi::start(ws_stream3);

    println!("Step 1: PUT contract to cache it locally");

    // Cache the contract locally using client1
    make_put(&mut client1, wrapped_state.clone(), contract.clone(), false).await?;
    let put_result = timeout(Duration::from_secs(30), client1.recv()).await;

    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
            println!("Contract cached successfully");
        }
        other => {
            panic!("PUT failed: {:?}", other);
        }
    }

    println!("Step 2: Concurrent GET requests from multiple clients");
    println!("This tests the deduplication race condition from issue #1886");

    // Send GET requests concurrently from all clients
    // The contract is cached, so these will complete instantly
    // This creates the race condition: TX may be removed before all clients register
    let get1 = async {
        make_get(&mut client1, contract_key, true, false).await?;
        let result = timeout(Duration::from_secs(5), client1.recv()).await;
        Ok::<_, anyhow::Error>((1, result))
    };

    let get2 = async {
        make_get(&mut client2, contract_key, true, false).await?;
        let result = timeout(Duration::from_secs(5), client2.recv()).await;
        Ok::<_, anyhow::Error>((2, result))
    };

    let get3 = async {
        make_get(&mut client3, contract_key, true, false).await?;
        let result = timeout(Duration::from_secs(5), client3.recv()).await;
        Ok::<_, anyhow::Error>((3, result))
    };

    // Execute all GETs concurrently
    let (result1, result2, result3) = tokio::join!(get1, get2, get3);

    // Verify all clients received responses
    let check_result =
        |client_num: i32, result: anyhow::Result<(i32, Result<Result<HostResponse, _>, _>)>| {
            match result {
                Ok((
                    _,
                    Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                        key,
                        state,
                        ..
                    }))),
                )) => {
                    assert_eq!(key, contract_key);
                    assert_eq!(state, wrapped_state);
                    println!("Client {}: Received GET response", client_num);
                    true
                }
                Ok((_, Ok(Ok(other)))) => {
                    println!("Client {}: Unexpected response: {:?}", client_num, other);
                    false
                }
                Ok((_, Ok(Err(e)))) => {
                    println!("Client {}: Error: {}", client_num, e);
                    false
                }
                Ok((_, Err(_))) => {
                    println!(
                        "Client {}: TIMEOUT - This is the bug from issue #1886!",
                        client_num
                    );
                    false
                }
                Err(e) => {
                    println!("Client {}: Failed to send request: {}", client_num, e);
                    false
                }
            }
        };

    let success1 = check_result(1, result1);
    let success2 = check_result(2, result2);
    let success3 = check_result(3, result3);

    // REGRESSION TEST: All clients should receive responses
    // If any client times out, it indicates the deduplication race condition
    assert!(
        success1 && success2 && success3,
        "All clients should receive GET responses. Failures indicate issue #1886 race condition."
    );

    println!("All clients received responses - no race condition detected");

    // Cleanup
    client1
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client2
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client3
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Test subscription operations on isolated node with local contracts
///
/// This regression test verifies that Subscribe operations complete successfully
/// when the contract exists locally but no remote peers are available.
/// Tests the fix in PR #1844 where SubscribeResponse messages were not being
/// delivered to WebSocket clients for local contracts.
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_isolated_node_local_subscription(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load test contract and state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = freenet::test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Connect first client to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream1, _) = connect_async(&url).await?;
    let mut client1 = WebApi::start(ws_stream1);

    // Connect second client to test that subscriptions work for multiple clients
    let (ws_stream2, _) = connect_async(&url).await?;
    let mut client2 = WebApi::start(ws_stream2);

    println!("Step 1: Performing PUT operation to cache contract locally");

    // Perform PUT operation - this should cache the contract locally
    make_put(&mut client1, wrapped_state.clone(), contract.clone(), false).await?;

    // Wait for PUT response
    let put_result = timeout(Duration::from_secs(30), client1.recv()).await;

    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
            println!("PUT operation successful");
        }
        Ok(Ok(other)) => {
            panic!("Unexpected PUT response: {:?}", other);
        }
        Ok(Err(e)) => {
            panic!("PUT operation failed: {}", e);
        }
        Err(_) => {
            panic!("PUT operation timed out");
        }
    }

    println!("Step 2: Testing SUBSCRIBE operation on locally cached contract");

    // Subscribe first client to the contract - should work with local contract
    let subscribe_start = std::time::Instant::now();
    make_subscribe(&mut client1, contract_key).await?;

    // Wait for SUBSCRIBE response - THIS IS THE KEY TEST
    // The fix ensures this completes successfully for local contracts
    let subscribe_result = timeout(Duration::from_secs(10), client1.recv()).await;
    let subscribe_elapsed = subscribe_start.elapsed();

    match subscribe_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
            key,
            subscribed,
        }))) => {
            assert_eq!(key, contract_key);
            println!(
                "Client 1: SUBSCRIBE operation successful in {:?}",
                subscribe_elapsed
            );

            // Verify we got the subscribed confirmation (contract exists locally)
            assert!(
                subscribed,
                "Should receive subscribed=true when subscribing to local contract"
            );
        }
        Ok(Ok(other)) => {
            panic!("Unexpected SUBSCRIBE response: {:?}", other);
        }
        Ok(Err(e)) => {
            panic!("SUBSCRIBE operation failed: {}", e);
        }
        Err(_) => {
            panic!(
                "SUBSCRIBE operation timed out - SubscribeResponse not delivered! \
                 This indicates the bug from PR #1844 has regressed."
            );
        }
    }

    println!("Step 3: Testing second client subscription");

    // Subscribe second client - verifies multiple clients can subscribe locally
    make_subscribe(&mut client2, contract_key).await?;

    let subscribe2_result = timeout(Duration::from_secs(10), client2.recv()).await;

    match subscribe2_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
            key,
            subscribed,
        }))) => {
            assert_eq!(key, contract_key);
            println!("Client 2: SUBSCRIBE operation successful");
            assert!(subscribed);
        }
        _ => {
            panic!("Client 2: SUBSCRIBE operation failed or timed out");
        }
    }

    // NOTE: Update/notification testing is skipped because UPDATE operations
    // timeout on isolated nodes (see issue #1884). The core Subscribe functionality
    // has been validated - both clients successfully receive SubscribeResponse.
    // Update notification delivery can be tested once UPDATE is fixed for isolated nodes.

    println!(
        "Local subscription test completed successfully - both clients received SubscribeResponse"
    );

    // Properly close clients
    client1
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client2
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Test UPDATE operation on isolated node
///
/// This regression test verifies UPDATE operations on isolated nodes:
/// - PUT operation caches contract locally
/// - UPDATE operation updates the contract state
/// - UPDATE returns UpdateResponse without timeout (issue #1884)
/// - GET operation retrieves updated state
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread"
)]
async fn test_isolated_node_update_operation(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let ws_port = gateway.ws_port;

    // Load test contract and state
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = freenet::test_utils::create_empty_todo_list();
    let wrapped_initial_state = WrappedState::from(initial_state);

    // Connect to the node
    let url = format!(
        "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (ws_stream, _) = connect_async(&url).await?;
    let mut client = WebApi::start(ws_stream);

    println!("Step 1: Performing PUT operation to cache contract locally");

    // Perform PUT operation - this caches the contract locally
    let put_start = std::time::Instant::now();
    make_put(
        &mut client,
        wrapped_initial_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for PUT response
    let put_result = timeout(Duration::from_secs(30), client.recv()).await;
    let put_elapsed = put_start.elapsed();

    match put_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
            println!("PUT operation successful in {:?}", put_elapsed);
        }
        Ok(Ok(other)) => {
            panic!("Unexpected PUT response: {:?}", other);
        }
        Ok(Err(e)) => {
            panic!("PUT operation failed: {}", e);
        }
        Err(_) => {
            panic!("PUT operation timed out");
        }
    }

    println!("Step 2: Performing UPDATE operation with new state");

    // Create updated state (add a todo item)
    let updated_state = freenet::test_utils::create_todo_list_with_item("Test task");
    let wrapped_updated_state = WrappedState::from(updated_state);

    // Perform UPDATE operation
    let update_start = std::time::Instant::now();
    make_update(&mut client, contract_key, wrapped_updated_state.clone()).await?;

    // Wait for UPDATE response
    let update_result = timeout(Duration::from_secs(15), client.recv()).await;
    let update_elapsed = update_start.elapsed();

    // REGRESSION TEST: Verify UPDATE completed quickly without timeout (issue #1884)
    // The bug causes UPDATE to timeout after 10 seconds, so we check it completes in < 10 seconds
    assert!(
        update_elapsed < Duration::from_secs(10),
        "UPDATE should complete quickly on isolated node, not timeout. Elapsed: {:?}",
        update_elapsed
    );

    // Verify UPDATE succeeded
    match update_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key, ..
        }))) => {
            assert_eq!(key, contract_key);
            println!("UPDATE operation successful in {:?}", update_elapsed);
        }
        Ok(Ok(other)) => {
            panic!("Unexpected UPDATE response: {:?}", other);
        }
        Ok(Err(e)) => {
            panic!("UPDATE operation failed: {}", e);
        }
        Err(_) => {
            panic!("UPDATE operation timed out (this is the bug in issue #1884)");
        }
    }

    println!("Step 3: Performing GET operation to verify updated state");

    // Verify the state was updated by performing a GET
    let get_start = std::time::Instant::now();
    make_get(&mut client, contract_key, true, false).await?;

    let get_result = timeout(Duration::from_secs(10), client.recv()).await;
    let get_elapsed = get_start.elapsed();

    match get_result {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            state: recv_state,
            ..
        }))) => {
            // Parse both states to verify the tasks were updated correctly
            // Note: UPDATE operations may modify the version number, so we check the tasks array
            let recv_str = String::from_utf8_lossy(recv_state.as_ref());
            println!("Received state after UPDATE: {}", recv_str);

            // Verify the state contains the expected task
            assert!(
                recv_str.contains("\"title\":\"Test task\""),
                "State should contain the updated task 'Test task'"
            );
            assert!(
                recv_str.contains("\"tasks\":["),
                "State should have tasks array"
            );

            // Verify it's not the empty state
            assert!(
                !recv_str.contains("\"tasks\":[]"),
                "Tasks array should not be empty after update"
            );

            println!(
                "GET operation successful, state correctly updated in {:?}",
                get_elapsed
            );
        }
        Ok(Ok(other)) => {
            panic!("Unexpected GET response: {:?}", other);
        }
        Ok(Err(e)) => {
            panic!("GET operation failed: {}", e);
        }
        Err(_) => {
            panic!("GET operation timed out");
        }
    }

    println!("PUT-UPDATE-GET workflow completed successfully on isolated node");

    // Properly close the client
    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}
