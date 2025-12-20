use anyhow::{anyhow, bail, ensure};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{
        self, load_delegate, make_get, make_node_diagnostics, make_put, make_subscribe,
        make_update, verify_contract_exists, TestContext,
    },
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{
        ClientRequest, ContractResponse, HostResponse, NodeDiagnosticsConfig, QueryResponse, WebApi,
    },
    prelude::*,
};
use futures::FutureExt;
use rand::{random, Rng, SeedableRng};
use serde::Deserialize;
use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    sync::{LazyLock, Mutex},
    time::Duration,
};
use testresult::TestResult;
use tokio::select;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"0102030405060708090a0b0c0d0e0f10",
    ))
});

struct PresetConfig {
    temp_dir: tempfile::TempDir,
}

async fn base_node_test_config(
    is_gateway: bool,
    gateways: Vec<String>,
    public_port: Option<u16>,
    ws_api_port: u16,
) -> anyhow::Result<(ConfigArgs, PresetConfig)> {
    const _DEFAULT_RATE_LIMIT: usize = 1024 * 1024 * 10; // 10 MB/s

    if is_gateway {
        assert!(public_port.is_some());
    }

    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new();
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;
    key.public().save(temp_dir.path().join("public.pem"))?;
    let config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_api_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port,
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(gateways),
            location: Some(RNG.lock().unwrap().random()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: public_port,
            min_connections: None,
            max_connections: None,
            bandwidth_limit: None,
            blocked_addresses: None,
            transient_budget: None,
            transient_ttl_secs: None,
        },
        config_paths: {
            freenet::config::ConfigPathsArgs {
                config_dir: Some(temp_dir.path().to_path_buf()),
                data_dir: Some(temp_dir.path().to_path_buf()),
            }
        },
        secrets: SecretArgs {
            transport_keypair: Some(transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };
    Ok((config, PresetConfig { temp_dir }))
}

fn gw_config(port: u16, path: &Path) -> anyhow::Result<InlineGwConfig> {
    Ok(InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, port).into(),
        location: Some(random()),
        public_key_path: path.join("public.pem"),
    })
}

async fn get_contract(
    client: &mut WebApi,
    key: ContractKey,
    temp_dir: impl AsRef<Path>,
) -> anyhow::Result<(ContractContainer, WrappedState)> {
    make_get(client, key, true, false).await?;
    // Limit iterations to avoid infinite loop on unexpected responses
    const MAX_RESPONSES: usize = 20;
    for _ in 0..MAX_RESPONSES {
        let resp = tokio::time::timeout(Duration::from_secs(45), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(contract),
                state,
            }))) => {
                verify_contract_exists(temp_dir.as_ref(), key).await?;
                return Ok((contract, state));
            }
            Ok(Ok(other)) => {
                tracing::warn!("unexpected response while waiting for get: {:?}", other);
                // Continue to next iteration to drain pending messages
            }
            Ok(Err(e)) => {
                bail!("Error receiving get response: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for get response");
            }
        }
    }
    bail!("Exceeded max responses ({MAX_RESPONSES}) without receiving expected GetResponse");
}

async fn send_put_with_retry(
    client: &mut WebApi,
    state: WrappedState,
    contract: ContractContainer,
    description: &str,
    expected_key: Option<ContractKey>,
) -> anyhow::Result<()> {
    const MAX_ATTEMPTS: usize = 3;
    const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
    for attempt in 1..=MAX_ATTEMPTS {
        tracing::info!("Sending {} (attempt {attempt}/{MAX_ATTEMPTS})", description);

        make_put(client, state.clone(), contract.clone(), false).await?;

        match tokio::time::timeout(ATTEMPT_TIMEOUT, client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                if let Some(expected) = expected_key {
                    ensure!(
                        key == expected,
                        "{} returned unexpected contract key (expected {}, got {})",
                        description,
                        expected,
                        key
                    );
                }
                tracing::info!("{description} succeeded on attempt {attempt}");
                return Ok(());
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "{} attempt {attempt} returned unexpected response: {:?}",
                    description,
                    other
                );
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    "{} attempt {attempt} failed while receiving response: {}",
                    description,
                    e
                );
            }
            Err(_) => {
                tracing::warn!(
                    "{} attempt {attempt} timed out waiting for response",
                    description
                );
            }
        }

        if attempt == MAX_ATTEMPTS {
            bail!("{description} failed after {MAX_ATTEMPTS} attempts");
        }

        // Drain any stray responses/errors before retrying to keep the client state clean.
        loop {
            match tokio::time::timeout(Duration::from_millis(200), client.recv()).await {
                Ok(Ok(resp)) => {
                    tracing::warn!(
                        "Discarding stray response prior to retrying {}: {:?}",
                        description,
                        resp
                    );
                }
                Ok(Err(err)) => {
                    tracing::warn!(
                        "Discarding stray error prior to retrying {}: {}",
                        description,
                        err
                    );
                }
                Err(_) => break,
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    unreachable!("send_put_with_retry loop should always return or bail");
}

/// Test PUT operation across two peers (gateway and peer)
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 180,
    startup_wait_secs = 15,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
    aggregate_events = "always"
)]
async fn test_put_contract(ctx: &mut TestContext) -> TestResult {
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_peer_a = peer_a.ws_port;
    let ws_api_port_peer_b = gateway.ws_port;

    tracing::info!("Node A (peer-a) ws_port: {}", ws_api_port_peer_a);
    tracing::info!("Node B (gateway) ws_port: {}", ws_api_port_peer_b);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to node A's websocket API
    let uri =
        format!("ws://127.0.0.1:{ws_api_port_peer_a}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_a = WebApi::start(stream);

    make_put(
        &mut client_api_a,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for put response (increased timeout for CI environments)
    tracing::info!("Waiting for PUT response...");
    let resp = tokio::time::timeout(Duration::from_secs(120), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            tracing::info!("PUT successful for contract: {}", key);
            assert_eq!(key, contract_key);
        }
        Ok(Ok(other)) => {
            tracing::warn!("unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for put response after 120 seconds");
        }
    }

    {
        // Wait for get response from node A
        tracing::info!("getting contract from A");
        let (response_contract, response_state) =
            get_contract(&mut client_api_a, contract_key, &gateway.temp_dir_path).await?;
        let response_key = response_contract.key();

        // Verify the responses
        assert_eq!(response_key, contract_key);
        assert_eq!(response_contract, contract);
        assert_eq!(response_state, wrapped_state);
    }

    {
        // Connect to node B's websocket API
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_peer_b}/v1/contract/command?encodingProtocol=native"
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api_b = WebApi::start(stream);

        // Wait for get response from node B
        let (response_contract, response_state) =
            get_contract(&mut client_api_b, contract_key, &gateway.temp_dir_path).await?;
        let response_key = response_contract.key();

        // Verify the responses
        assert_eq!(response_key, contract_key);
        assert_eq!(response_contract, contract);
        assert_eq!(response_state, wrapped_state);

        // Properly close the client
        client_api_b
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Close the first client as well
    client_api_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 180,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_update_contract(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port = peer_a.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A (peer-a) data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Node B (gw) data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to node A websocket API
    let uri = format!("ws://127.0.0.1:{ws_api_port}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_a = WebApi::start(stream);

    // Put contract with initial state
    make_put(
        &mut client_api_a,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for put response (increased timeout for CI environments)
    tracing::info!("Waiting for PUT response...");
    let resp = tokio::time::timeout(Duration::from_secs(120), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            tracing::info!("PUT successful for contract: {}", key);
            assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
        }
        Ok(Ok(other)) => {
            tracing::warn!("unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for put response after 120 seconds");
        }
    }

    // Create a new to-do list by deserializing the current state, adding a task, and serializing it back
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())
        .unwrap_or_else(|_| test_utils::TodoList {
            tasks: Vec::new(),
            version: 0,
        });

    // Add a task directly to the list
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Implement contract".to_string(),
        description: "Create a smart contract for the todo list".to_string(),
        completed: false,
        priority: 3,
    });

    // Serialize the updated list back to bytes
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let updated_state = WrappedState::from(updated_bytes);

    let expected_version_after_update = todo_list.version + 1;

    make_update(&mut client_api_a, contract_key, updated_state.clone()).await?;

    // Wait for update response
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key,
            summary: _,
        }))) => {
            assert_eq!(
                key, contract_key,
                "Contract key mismatch in UPDATE response"
            );
        }
        Ok(Ok(other)) => {
            bail!("unexpected response while waiting for update: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Client A: Error receiving update response: {}", e);
        }
        Err(_) => {
            bail!("Client A: Timeout waiting for update response");
        }
    }

    // Verify the updated state with GET
    {
        // Wait for get response from node A
        let (response_contract, response_state) =
            get_contract(&mut client_api_a, contract_key, &gateway.temp_dir_path).await?;

        assert_eq!(
            response_contract.key(),
            contract_key,
            "Contract key mismatch in GET response"
        );
        assert_eq!(
            response_contract, contract,
            "Contract content mismatch in GET response"
        );

        // Compare the deserialized updated content
        let response_todo_list: test_utils::TodoList =
            serde_json::from_slice(response_state.as_ref())
                .expect("Failed to deserialize response state");

        let expected_todo_list: test_utils::TodoList =
            serde_json::from_slice(updated_state.as_ref())
                .expect("Failed to deserialize expected state");

        assert_eq!(
            response_todo_list.version, expected_version_after_update,
            "Version should match"
        );

        assert_eq!(
            response_todo_list.tasks.len(),
            expected_todo_list.tasks.len(),
            "Number of tasks should match"
        );

        // Verify that the task exists and has the correct values
        assert_eq!(response_todo_list.tasks.len(), 1, "Should have one task");
        assert_eq!(response_todo_list.tasks[0].id, 1, "Task ID should be 1");
        assert_eq!(
            response_todo_list.tasks[0].title, "Implement contract",
            "Task title should match"
        );

        tracing::info!(
            "Successfully verified updated state for contract {}",
            contract_key
        );

        // Print states for debugging
        tracing::debug!(
            "Response state: {:?}, Expected state: {:?}",
            response_todo_list,
            expected_todo_list
        );
    }

    Ok(())
}

/// Test that a second PUT to an already cached contract persists the merged state.
/// This is a regression test for issue #1995.
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 180,
    startup_wait_secs = 15,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_merge_persists_state(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let initial_wrapped_state = WrappedState::from(initial_state);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_peer_a = peer_a.ws_port;
    let ws_api_port_peer_b = gateway.ws_port;

    tracing::info!("Node A data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Node B (gw) data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to node A's websocket API
    let uri =
        format!("ws://127.0.0.1:{ws_api_port_peer_a}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_a = WebApi::start(stream);

    send_put_with_retry(
        &mut client_api_a,
        initial_wrapped_state.clone(),
        contract.clone(),
        "first PUT (cache seed)",
        Some(contract_key),
    )
    .await?;

    // Wait a bit to ensure state is fully cached
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create updated state with more data (simulating a state merge)
    let mut updated_todo_list: test_utils::TodoList =
        serde_json::from_slice(initial_wrapped_state.as_ref()).unwrap();

    // Add multiple tasks to make the state larger
    for i in 1..=5 {
        updated_todo_list.tasks.push(test_utils::Task {
            id: i,
            title: format!("Task {}", i),
            description: format!("Description for task {}", i),
            completed: false,
            priority: i as u8,
        });
    }

    let updated_bytes = serde_json::to_vec(&updated_todo_list).unwrap();
    let updated_wrapped_state = WrappedState::from(updated_bytes);

    tracing::info!(
        "Initial state size: {} bytes, Updated state size: {} bytes",
        initial_wrapped_state.as_ref().len(),
        updated_wrapped_state.as_ref().len()
    );

    send_put_with_retry(
        &mut client_api_a,
        updated_wrapped_state.clone(),
        contract.clone(),
        "second PUT (merge)",
        Some(contract_key),
    )
    .await?;

    // Wait a bit to ensure the merge and persistence completes
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The key test: GET from gateway to verify it persisted the merged state
    // This is the bug from issue #1995 - gateway receives PUT for already-cached
    // contract, merges state, but doesn't persist it
    let uri =
        format!("ws://127.0.0.1:{ws_api_port_peer_b}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_gateway = WebApi::start(stream);

    tracing::info!("Getting contract from gateway to verify merged state was persisted...");
    let (response_contract_gw, response_state_gw) = get_contract(
        &mut client_api_gateway,
        contract_key,
        &gateway.temp_dir_path,
    )
    .await?;

    assert_eq!(response_contract_gw.key(), contract_key);

    let response_todo_list_gw: test_utils::TodoList =
        serde_json::from_slice(response_state_gw.as_ref())
            .expect("Failed to deserialize state from gateway");

    tracing::info!(
        "Gateway returned state with {} tasks, size {} bytes",
        response_todo_list_gw.tasks.len(),
        response_state_gw.as_ref().len()
    );

    // This is the key assertion for issue #1995:
    // Gateway received a PUT for an already-cached contract, merged the states,
    // and should have PERSISTED the merged state (not just computed it)
    assert_eq!(
        response_todo_list_gw.tasks.len(),
        5,
        "Gateway should return merged state with 5 tasks (issue #1995: merged state must be persisted)"
    );

    // Verify the state size matches as additional confirmation
    assert_eq!(
        response_state_gw.as_ref().len(),
        updated_wrapped_state.as_ref().len(),
        "Gateway state size should match the updated state"
    );

    tracing::info!(
        "✓ Test passed: Gateway correctly persisted merged state after second PUT (issue #1995 fixed)"
    );

    // Cleanup
    client_api_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client_api_gateway
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

// This test validates the REMOTE subscription path (local subscribe → remote update):
//
// Architecture being tested:
// - Client on Node A subscribes to a contract
// - Contract is updated on Node B (or gateway)
// - UPDATE propagates through the network to Node A
// - Node A's upsert_contract_state() is called with the new state
// - This triggers send_update_notification() which delivers to the local subscriber
//
// Combined with test_get_with_subscribe_flag (which tests local subscribe → local update),
// this provides full coverage of the executor notification delivery paths.
//
// NOTE: This test is disabled due to race conditions in subscription propagation logic.
// The test expects multiple clients across different nodes to receive subscription updates,
// but the PUT caching refactor (commits 2cd337b5-0d432347) changed the subscription semantics.
// Ignored due to recurring flakiness - fails intermittently with timeout waiting for
// cross-node subscription notifications (Client 3 timeout). See issue #1798.
#[ignore]
#[freenet_test(
    nodes = ["gateway", "node-a", "node-b"],
    timeout_secs = 600,
    startup_wait_secs = 40,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_multiple_clients_subscription(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Get node information from context
    let node_a = ctx.node("node-a")?;
    let node_b = ctx.node("node-b")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_a = node_a.ws_port;
    let ws_api_port_b = node_b.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", node_a.temp_dir_path);
    tracing::info!("Gateway data dir: {:?}", gateway.temp_dir_path);
    tracing::info!("Node B data dir: {:?}", node_b.temp_dir_path);

    // Give extra time for peers to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect first client to node A's websocket API
    tracing::info!("Starting WebSocket connections after 40s startup wait");
    let start_time = std::time::Instant::now();
    let uri_a =
        format!("ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native");
    let (stream1, _) = connect_async(&uri_a).await?;
    let mut client_api1_node_a = WebApi::start(stream1);

    // Connect second client to node A's websocket API
    let (stream2, _) = connect_async(&uri_a).await?;
    let mut client_api2_node_a = WebApi::start(stream2);

    // Connect third client to node C's websocket API (different node)
    let uri_c =
        format!("ws://127.0.0.1:{ws_api_port_b}/v1/contract/command?encodingProtocol=native");
    let (stream3, _) = connect_async(&uri_c).await?;
    let mut client_api_node_b = WebApi::start(stream3);

    // First client puts contract with initial state (without subscribing)
    tracing::info!(
        "Client 1: Starting PUT operation (elapsed: {:?})",
        start_time.elapsed()
    );
    make_put(
        &mut client_api1_node_a,
        wrapped_state.clone(),
        contract.clone(),
        false, // subscribe=false - no automatic subscription
    )
    .await?;

    // Wait for put response
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(120), client_api1_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
                tracing::info!(
                    "Client 1: PUT completed successfully (elapsed: {:?})",
                    start_time.elapsed()
                );
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!("unexpected response while waiting for put: {:?}", other);
            }
            Ok(Err(e)) => {
                bail!("Error receiving put response: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for put response");
            }
        }
    }

    // Explicitly subscribe client 1 to the contract using make_subscribe
    make_subscribe(&mut client_api1_node_a, contract_key).await?;

    // Wait for subscribe response
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(30), client_api1_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in SUBSCRIBE response"
                );
                assert!(subscribed, "Failed to subscribe to contract");
                tracing::info!("Client 1: Successfully subscribed to contract {}", key);
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Client 1: unexpected response while waiting for subscribe: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Client 1: Error receiving subscribe response: {}", e);
            }
            Err(_) => {
                bail!("Client 1: Timeout waiting for subscribe response");
            }
        }
    }

    // Second client gets the contract (without subscribing)
    make_get(&mut client_api2_node_a, contract_key, true, false).await?;

    // Wait for get response on second client
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(_),
                state: _,
            }))) => {
                assert_eq!(key, contract_key, "Contract key mismatch in GET response");
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!("unexpected response while waiting for get: {:?}", other);
            }
            Ok(Err(e)) => {
                bail!("Error receiving get response: {}", e);
            }
            Err(_) => {
                bail!("Timeout waiting for get response");
            }
        }
    }

    // Explicitly subscribe client 2 to the contract using make_subscribe
    make_subscribe(&mut client_api2_node_a, contract_key).await?;

    // Wait for subscribe response
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in SUBSCRIBE response"
                );
                assert!(subscribed, "Failed to subscribe to contract");
                tracing::info!("Client 2: Successfully subscribed to contract {}", key);
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Client 2: unexpected response while waiting for subscribe: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Client 2: Error receiving subscribe response: {}", e);
            }
            Err(_) => {
                bail!("Client 2: Timeout waiting for subscribe response");
            }
        }
    }

    // Third client gets the contract from node C (without subscribing)
    // Add delay to allow contract to propagate from Node A to Node B/C
    tracing::info!("Waiting 5 seconds for contract to propagate across nodes...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    tracing::info!(
        "Client 3: Sending GET request for contract {} to Node B",
        contract_key
    );
    let get_start = std::time::Instant::now();
    make_get(&mut client_api_node_b, contract_key, true, false).await?;

    // Wait for get response on third client
    // Note: Contract propagation from Node A to Node B can take 5-10s locally, longer in CI
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(60), client_api_node_b.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(_),
                state: _,
            }))) => {
                let elapsed = get_start.elapsed();
                tracing::info!("Client 3: Received GET response after {:?}", elapsed);
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in GET response for client 3"
                );
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Client 3: unexpected response while waiting for get: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Client 3: Error receiving get response: {}", e);
            }
            Err(_) => {
                let elapsed = get_start.elapsed();
                bail!("Client 3: Timeout waiting for get response after {:?}. Contract may not have propagated from Node A to Node B", elapsed);
            }
        }
    }

    // Explicitly subscribe client 3 to the contract using make_subscribe
    make_subscribe(&mut client_api_node_b, contract_key).await?;

    // Wait for subscribe response
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(60), client_api_node_b.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in SUBSCRIBE response for client 3"
                );
                assert!(subscribed, "Failed to subscribe to contract for client 3");
                tracing::info!("Client 3: Successfully subscribed to contract {}", key);
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!(
                    "Client 3: unexpected response while waiting for subscribe: {:?}",
                    other
                );
            }
            Ok(Err(e)) => {
                bail!("Client 3: Error receiving subscribe response: {}", e);
            }
            Err(_) => {
                bail!("Client 3: Timeout waiting for subscribe response");
            }
        }
    }

    tracing::info!("All clients subscribed, proceeding with UPDATE operation");

    // Create a new to-do list by deserializing the current state, adding a task, and serializing it back
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())
        .unwrap_or_else(|_| test_utils::TodoList {
            tasks: Vec::new(),
            version: 0,
        });

    // Add a task directly to the list
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Test multiple clients".to_string(),
        description: "Verify that update notifications are received by multiple clients"
            .to_string(),
        completed: false,
        priority: 5,
    });

    // Serialize the updated list back to bytes
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let updated_state = WrappedState::from(updated_bytes);

    // First client updates the contract
    make_update(&mut client_api1_node_a, contract_key, updated_state.clone()).await?;

    // Wait for update response and notifications on all clients
    let mut client1_received_notification = false;
    let mut client2_received_notification = false;
    let mut client_node_b_received_notification = false;
    let mut received_update_response = false;

    // Expected task after update
    let expected_task = test_utils::Task {
        id: 1,
        title: "Test multiple clients".to_string(),
        description: "Verify that update notifications are received by multiple clients"
            .to_string(),
        completed: false,
        priority: 5,
    };

    let start_time = std::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(90)
        && (!received_update_response
            || !client1_received_notification
            || !client2_received_notification
            || !client_node_b_received_notification)
    {
        // Check for messages on client 1
        if !received_update_response || !client1_received_notification {
            let resp =
                tokio::time::timeout(Duration::from_secs(1), client_api1_node_a.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                    key,
                    summary: _,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE response"
                    );
                    tracing::info!("Client 1: Received update response for contract {}", key);
                    received_update_response = true;
                }
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                    key,
                    update,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE notification for client 1"
                    );

                    // Verify update content
                    match update {
                        UpdateData::State(state) => {
                            let received_todo_list: test_utils::TodoList =
                                serde_json::from_slice(state.as_ref())
                                    .expect("Failed to deserialize state from update notification");

                            assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                            assert_eq!(
                                received_todo_list.tasks[0].id, expected_task.id,
                                "Task ID should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].title, expected_task.title,
                                "Task title should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].description, expected_task.description,
                                "Task description should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].completed, expected_task.completed,
                                "Task completed status should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].priority, expected_task.priority,
                                "Task priority should match"
                            );

                            tracing::info!("Client 1: Successfully verified update content");
                        }
                        _ => {
                            tracing::warn!(
                                "Client 1: Received unexpected update type: {:?}",
                                update
                            );
                        }
                    }

                    tracing::info!(
                        "✅ Client 1: Successfully received update notification for contract {}",
                        key
                    );
                    client1_received_notification = true;
                }
                Ok(Ok(other)) => {
                    tracing::debug!("Client 1: Received unexpected response: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::debug!("Client 1: Error receiving response: {}", e);
                }
                Err(_) => {
                    // Timeout is expected, just continue
                }
            }
        }

        // Check for notification on client 2
        if !client2_received_notification {
            let resp =
                tokio::time::timeout(Duration::from_secs(1), client_api2_node_a.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                    key,
                    update,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE notification for client 2"
                    );

                    // Verify update content
                    match update {
                        UpdateData::State(state) => {
                            let received_todo_list: test_utils::TodoList =
                                serde_json::from_slice(state.as_ref())
                                    .expect("Failed to deserialize state from update notification");

                            assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                            assert_eq!(
                                received_todo_list.tasks[0].id, expected_task.id,
                                "Task ID should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].title, expected_task.title,
                                "Task title should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].description, expected_task.description,
                                "Task description should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].completed, expected_task.completed,
                                "Task completed status should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].priority, expected_task.priority,
                                "Task priority should match"
                            );

                            tracing::info!("Client 2: Successfully verified update content");
                        }
                        _ => {
                            tracing::warn!(
                                "Client 2: Received unexpected update type: {:?}",
                                update
                            );
                        }
                    }

                    tracing::info!(
                        "✅ Client 2: Successfully received update notification for contract {}",
                        key
                    );
                    client2_received_notification = true;
                }
                Ok(Ok(other)) => {
                    tracing::debug!("Client 2: Received unexpected response: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::debug!("Client 2: Error receiving response: {}", e);
                }
                Err(_) => {
                    // Timeout is expected, just continue
                }
            }
        }

        // Check for notification on client 3 (on different node)
        if !client_node_b_received_notification {
            let resp = tokio::time::timeout(Duration::from_secs(1), client_api_node_b.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                    key,
                    update,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE notification for client 3"
                    );

                    // Verify update content
                    match update {
                        UpdateData::State(state) => {
                            let received_todo_list: test_utils::TodoList =
                                serde_json::from_slice(state.as_ref())
                                    .expect("Failed to deserialize state from update notification");

                            assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                            assert_eq!(
                                received_todo_list.tasks[0].id, expected_task.id,
                                "Task ID should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].title, expected_task.title,
                                "Task title should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].description, expected_task.description,
                                "Task description should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].completed, expected_task.completed,
                                "Task completed status should match"
                            );
                            assert_eq!(
                                received_todo_list.tasks[0].priority, expected_task.priority,
                                "Task priority should match"
                            );

                            tracing::info!(
                                "Client 3: Successfully verified update content (cross-node)"
                            );
                        }
                        _ => {
                            tracing::warn!(
                                "Client 3: Received unexpected update type: {:?}",
                                update
                            );
                        }
                    }

                    tracing::info!(
                        "✅ Client 3: Successfully received update notification for contract {} (cross-node)",
                        key
                    );
                    client_node_b_received_notification = true;
                }
                Ok(Ok(other)) => {
                    tracing::debug!("Client 3: Received unexpected response: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::debug!("Client 3: Error receiving response: {}", e);
                }
                Err(_) => {
                    // Timeout is expected, just continue
                }
            }
        }

        // Small delay before trying again
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Assert that we received the update response and all clients received notifications
    assert!(
        received_update_response,
        "Did not receive update response within timeout period"
    );
    assert!(
        client1_received_notification,
        "Client 1 did not receive update notification within timeout period"
    );
    assert!(
        client2_received_notification,
        "Client 2 did not receive update notification within timeout period"
    );
    assert!(
        client_node_b_received_notification,
        "Client 3 did not receive update notification within timeout period (cross-node)"
    );

    // Properly close all clients
    client_api1_node_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client_api2_node_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client_api_node_b
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    Ok(())
}

#[freenet_test(
    nodes = ["gateway", "node-a"],
    timeout_secs = 120,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_get_with_subscribe_flag(ctx: &mut TestContext) -> TestResult {
    // This test validates the LOCAL subscription path (Issue #2075 decoupling):
    //
    // Architecture being tested:
    // - Both clients connect to the SAME node (node-a) via websocket
    // - Client 2 subscribes via GET with subscribe=true
    // - The subscription is registered LOCALLY via register_contract_notifier()
    //   in the executor, NOT via network peer registration in ring.seeding_manager
    // - When Client 1 updates the contract, the update notification is delivered
    //   directly through the executor's notification channels (send_update_notification)
    //
    // This test confirms that local subscriptions work independently of network
    // subscription propagation - no remote peer registration is required.

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let node_a = ctx.node("node-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_a = node_a.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", node_a.temp_dir_path);
    tracing::info!("Node B (gw) data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect first client to node A's websocket API (for putting the contract)
    let uri_a =
        format!("ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native");
    let (stream1, _) = connect_async(&uri_a).await?;
    let mut client_api1_node_a = WebApi::start(stream1);

    tracing::info!("Client 1: Put contract with initial state");

    // First client puts contract with initial state (without subscribing)
    make_put(
        &mut client_api1_node_a,
        wrapped_state.clone(),
        contract.clone(),
        false, // subscribe=false
    )
    .await?;

    // Wait for put response (increased timeout for CI environments)
    let resp = tokio::time::timeout(Duration::from_secs(45), client_api1_node_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
        }
        Ok(Ok(other)) => {
            bail!("unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Client 1: Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Client 1: Timeout waiting for put response");
        }
    }

    tracing::warn!("Client 1: Successfully put contract {}", contract_key);

    // Connect second client to node A's websocket API (for getting with auto-subscribe)
    let (stream2, _) = connect_async(&uri_a).await?;
    let mut client_api2_node_a = WebApi::start(stream2);

    // Second client gets the contract with auto-subscribe
    make_get(&mut client_api2_node_a, contract_key, true, true).await?;

    // Wait for get response on second client
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            key,
            contract: Some(_),
            state: _,
        }))) => {
            assert_eq!(key, contract_key, "Contract key mismatch in GET response");
        }
        Ok(Ok(other)) => {
            bail!("unexpected response while waiting for get: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Client 2: Error receiving get response: {}", e);
        }
        Err(_) => {
            bail!("Client 2: Timeout waiting for get response");
        }
    }

    // At this point, Client 2's subscription is registered LOCALLY in the executor
    // via register_contract_notifier(). The subscription is NOT registered in the
    // network's ring.seeding_manager.subscribers - that's only for remote peer subscriptions.
    // This validates the decoupled architecture from Issue #2075.
    tracing::info!(
        "Client 2: Local subscription registered via GET with subscribe=true - \
         notification delivery will use executor channels, not network broadcast"
    );

    // Create a new to-do list by deserializing the current state, adding a task, and serializing it back
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())
        .unwrap_or_else(|_| test_utils::TodoList {
            tasks: Vec::new(),
            version: 0,
        });

    // Add a task directly to the list
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Test auto-subscribe with GET".to_string(),
        description: "Verify that auto-subscribe works with GET operation".to_string(),
        completed: false,
        priority: 5,
    });

    // Serialize the updated list back to bytes
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let updated_state = WrappedState::from(updated_bytes);

    // First client updates the contract
    make_update(&mut client_api1_node_a, contract_key, updated_state.clone()).await?;

    // Wait for update response
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api1_node_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key,
            summary: _,
        }))) => {
            assert_eq!(
                key, contract_key,
                "Contract key mismatch in UPDATE response"
            );
        }
        Ok(Ok(other)) => {
            bail!("unexpected response while waiting for update: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Client 1: Error receiving update response: {}", e);
        }
        Err(_) => {
            bail!("Client 1: Timeout waiting for update response");
        }
    }

    // Expected task after update
    let expected_task = test_utils::Task {
        id: 1,
        title: "Test auto-subscribe with GET".to_string(),
        description: "Verify that auto-subscribe works with GET operation".to_string(),
        completed: false,
        priority: 5,
    };

    // Wait for update notification on client 2 (should be auto-subscribed)
    let mut client2_node_a_received_notification = false;

    // Try for up to 30 seconds to receive the notification
    let start_time = std::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(30) && !client2_node_a_received_notification {
        let resp = tokio::time::timeout(Duration::from_secs(1), client_api2_node_a.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                update,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in UPDATE notification for client 2"
                );

                // Verify update content
                match update {
                    UpdateData::State(state) => {
                        let received_todo_list: test_utils::TodoList =
                            serde_json::from_slice(state.as_ref())
                                .expect("Failed to deserialize state from update notification");

                        assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                        assert_eq!(
                            received_todo_list.tasks[0].id, expected_task.id,
                            "Task ID should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].title, expected_task.title,
                            "Task title should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].description, expected_task.description,
                            "Task description should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].completed, expected_task.completed,
                            "Task completed status should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].priority, expected_task.priority,
                            "Task priority should match"
                        );

                        tracing::info!("Client 1: Successfully verified update content");
                    }
                    _ => {
                        tracing::warn!("Client 1: Received unexpected update type: {:?}", update);
                    }
                }
                client2_node_a_received_notification = true;
                break;
            }
            Ok(Ok(other)) => {
                bail!("unexpected response while waiting for update: {:?}", other);
            }
            Ok(Err(e)) => {
                tracing::error!("Client 2: Timeout waiting for update: {}", e);
            }
            Err(_) => {
                tracing::error!("Client 2: Timeout waiting for update response");
            }
        }

        // Small delay before trying again
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Assert that Client 2 received the notification, proving that:
    // 1. Local subscription via GET with subscribe=true works correctly
    // 2. The executor's send_update_notification() delivers to local subscribers
    // 3. Network peer registration is NOT required for same-node subscriptions
    assert!(
        client2_node_a_received_notification,
        "Client 2 did not receive update notification - local subscription path failed. \
         This validates that executor notification channels work independently of network \
         subscription propagation (Issue #2075 decoupling)."
    );

    tracing::info!(
        "SUCCESS: Local subscription delivered update via executor channels - \
         no network registration was required"
    );

    Ok(())
}

// FIXME Update notification is not received
#[freenet_test(
    nodes = ["gateway", "node-a"],
    timeout_secs = 180,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_with_subscribe_flag(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let node_a = ctx.node("node-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_a = node_a.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", node_a.temp_dir_path);
    tracing::info!("Gateway data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect first client to node A's websocket API (for putting with auto-subscribe)
    let uri_a =
        format!("ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native");
    let (stream1, _) = connect_async(&uri_a).await?;
    let mut client_api1 = WebApi::start(stream1);

    // Connect second client to node A's websocket API (for updating the contract)
    let (stream2, _) = connect_async(&uri_a).await?;
    let mut client_api2 = WebApi::start(stream2);

    // First client puts contract with initial state and auto-subscribes
    let put_start = std::time::Instant::now();
    tracing::info!(
        contract = %contract_key,
        client = 1,
        subscribe = true,
        phase = "put_request",
        "Sending PUT request"
    );
    make_put(
        &mut client_api1,
        wrapped_state.clone(),
        contract.clone(),
        true, // subscribe=true for auto-subscribe
    )
    .await?;

    // Wait for put response
    let mut put_response_received = false;
    let start = std::time::Instant::now();
    while !put_response_received && start.elapsed() < Duration::from_secs(30) {
        let resp = tokio::time::timeout(Duration::from_secs(5), client_api1.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
                tracing::info!(
                    contract = %contract_key,
                    client = 1,
                    elapsed_ms = put_start.elapsed().as_millis(),
                    phase = "put_response",
                    "PUT response received"
                );
                put_response_received = true;
            }
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed: _,
            }))) => {
                bail!(
                    "Client 1: Received unexpected SubscribeResponse for contract {key} - \
                         sub-operations should not send client notifications"
                );
            }
            Ok(Ok(other)) => {
                tracing::debug!(
                    contract = %contract_key,
                    client = 1,
                    elapsed_ms = start.elapsed().as_millis(),
                    response = ?other,
                    "Received unexpected response while waiting for PUT"
                );
            }
            Ok(Err(e)) => {
                tracing::error!(
                    contract = %contract_key,
                    client = 1,
                    elapsed_ms = start.elapsed().as_millis(),
                    error = %e,
                    phase = "put_error",
                    "WebSocket error receiving PUT response"
                );
                bail!("WebSocket error while waiting for PUT response: {}", e);
            }
            Err(_) => {
                tracing::debug!(
                    contract = %contract_key,
                    client = 1,
                    elapsed_ms = start.elapsed().as_millis(),
                    "Waiting for PUT response (no message in 5s)"
                );
            }
        }
    }

    if !put_response_received {
        tracing::error!(
            contract = %contract_key,
            client = 1,
            elapsed_ms = start.elapsed().as_millis(),
            phase = "put_timeout",
            "PUT response timeout after 30 seconds"
        );
        bail!("Client 1: Did not receive PUT response within 30 seconds");
    }

    // Second client gets the contract (without subscribing)
    let get_start = std::time::Instant::now();
    tracing::info!(
        contract = %contract_key,
        client = 2,
        phase = "get_request",
        "Sending GET request"
    );
    make_get(&mut client_api2, contract_key, true, false).await?;

    // Wait for get response on second client
    let mut get_response_received = false;
    let start = std::time::Instant::now();
    while !get_response_received && start.elapsed() < Duration::from_secs(30) {
        let resp = tokio::time::timeout(Duration::from_secs(5), client_api2.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(_),
                state: _,
            }))) => {
                assert_eq!(key, contract_key, "Contract key mismatch in GET response");
                tracing::info!(
                    contract = %contract_key,
                    client = 2,
                    elapsed_ms = get_start.elapsed().as_millis(),
                    phase = "get_response",
                    "GET response received"
                );
                get_response_received = true;
            }
            Ok(Ok(other)) => {
                tracing::debug!(
                    contract = %contract_key,
                    client = 2,
                    elapsed_ms = start.elapsed().as_millis(),
                    response = ?other,
                    "Received unexpected response while waiting for GET"
                );
            }
            Ok(Err(e)) => {
                tracing::error!(
                    contract = %contract_key,
                    client = 2,
                    elapsed_ms = start.elapsed().as_millis(),
                    error = %e,
                    phase = "get_error",
                    "WebSocket error receiving GET response"
                );
                bail!("WebSocket error while waiting for GET response: {}", e);
            }
            Err(_) => {
                tracing::debug!(
                    contract = %contract_key,
                    client = 2,
                    elapsed_ms = start.elapsed().as_millis(),
                    "Waiting for GET response (no message in 5s)"
                );
            }
        }
    }

    if !get_response_received {
        tracing::error!(
            contract = %contract_key,
            client = 2,
            elapsed_ms = start.elapsed().as_millis(),
            phase = "get_timeout",
            "GET response timeout after 30 seconds"
        );
        bail!("Client 2: Did not receive GET response within 30 seconds");
    }

    // Create a new to-do list by deserializing the current state, adding a task, and serializing it back
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())
        .unwrap_or_else(|_| test_utils::TodoList {
            tasks: Vec::new(),
            version: 0,
        });

    // Add a task directly to the list
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Test auto-subscribe with PUT".to_string(),
        description: "Verify that auto-subscribe works with PUT operation".to_string(),
        completed: false,
        priority: 5,
    });

    // Serialize the updated list back to bytes
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let updated_state = WrappedState::from(updated_bytes);

    // Second client updates the contract
    let update_start = std::time::Instant::now();
    tracing::info!(
        contract = %contract_key,
        client = 2,
        phase = "update_request",
        "Sending UPDATE request to trigger notification"
    );
    make_update(&mut client_api2, contract_key, updated_state.clone()).await?;

    // Wait for update response
    let mut update_response_received = false;
    let start = std::time::Instant::now();
    while !update_response_received && start.elapsed() < Duration::from_secs(30) {
        let resp = tokio::time::timeout(Duration::from_secs(5), client_api2.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                key,
                summary: _,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in UPDATE response"
                );
                tracing::info!(
                    contract = %contract_key,
                    client = 2,
                    elapsed_ms = update_start.elapsed().as_millis(),
                    phase = "update_response",
                    "UPDATE response received"
                );
                update_response_received = true;
            }
            Ok(Ok(other)) => {
                tracing::debug!(
                    contract = %contract_key,
                    client = 2,
                    elapsed_ms = start.elapsed().as_millis(),
                    response = ?other,
                    "Received unexpected response while waiting for UPDATE"
                );
            }
            Ok(Err(e)) => {
                tracing::error!(
                    contract = %contract_key,
                    client = 2,
                    elapsed_ms = start.elapsed().as_millis(),
                    error = %e,
                    phase = "update_error",
                    "WebSocket error receiving UPDATE response"
                );
                bail!("WebSocket error while waiting for UPDATE response: {}", e);
            }
            Err(_) => {
                tracing::debug!(
                    contract = %contract_key,
                    client = 2,
                    elapsed_ms = start.elapsed().as_millis(),
                    "Waiting for UPDATE response (no message in 5s)"
                );
            }
        }
    }

    if !update_response_received {
        tracing::error!(
            contract = %contract_key,
            client = 2,
            elapsed_ms = start.elapsed().as_millis(),
            phase = "update_timeout",
            "UPDATE response timeout after 30 seconds"
        );
        bail!("Client 2: Did not receive UPDATE response within 30 seconds");
    }

    // Expected task after update
    let expected_task = test_utils::Task {
        id: 1,
        title: "Test auto-subscribe with PUT".to_string(),
        description: "Verify that auto-subscribe works with PUT operation".to_string(),
        completed: false,
        priority: 5,
    };

    // Wait for update notification on client 1 (should be auto-subscribed from PUT)
    let mut client1_received_notification = false;
    let notification_start = std::time::Instant::now();
    tracing::info!(
        contract = %contract_key,
        client = 1,
        phase = "notification_wait",
        "Waiting for UPDATE notification (auto-subscribed via PUT)"
    );

    // Try for up to 30 seconds to receive the notification
    while notification_start.elapsed() < Duration::from_secs(30) && !client1_received_notification {
        let resp = tokio::time::timeout(Duration::from_secs(1), client_api1.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                update,
            }))) => {
                assert_eq!(
                    key, contract_key,
                    "Contract key mismatch in UPDATE notification for client 1"
                );

                // Verify update content
                match update {
                    UpdateData::State(state) => {
                        let received_todo_list: test_utils::TodoList =
                            serde_json::from_slice(state.as_ref())
                                .expect("Failed to deserialize state from update notification");

                        assert_eq!(received_todo_list.tasks.len(), 1, "Should have one task");
                        assert_eq!(
                            received_todo_list.tasks[0].id, expected_task.id,
                            "Task ID should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].title, expected_task.title,
                            "Task title should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].description, expected_task.description,
                            "Task description should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].completed, expected_task.completed,
                            "Task completed status should match"
                        );
                        assert_eq!(
                            received_todo_list.tasks[0].priority, expected_task.priority,
                            "Task priority should match"
                        );

                        tracing::info!(
                            contract = %contract_key,
                            client = 1,
                            elapsed_ms = notification_start.elapsed().as_millis(),
                            phase = "notification_received",
                            "UPDATE notification received and verified"
                        );
                    }
                    _ => {
                        tracing::warn!(
                            contract = %contract_key,
                            client = 1,
                            update_type = ?update,
                            "Received unexpected update type in notification"
                        );
                    }
                }
                client1_received_notification = true;
                break;
            }
            Ok(Ok(other)) => {
                tracing::debug!(
                    contract = %contract_key,
                    client = 1,
                    elapsed_ms = notification_start.elapsed().as_millis(),
                    response = ?other,
                    "Received unexpected response while waiting for notification"
                );
            }
            Ok(Err(e)) => {
                tracing::error!(
                    contract = %contract_key,
                    client = 1,
                    elapsed_ms = notification_start.elapsed().as_millis(),
                    error = %e,
                    phase = "notification_error",
                    "WebSocket error receiving notification"
                );
                bail!(
                    "WebSocket error while waiting for update notification: {}",
                    e
                );
            }
            Err(_) => {
                // Timeout on recv - this is expected, just continue looping
            }
        }

        // Small delay before trying again
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Assert that client 1 received the notification (proving auto-subscribe worked)
    if !client1_received_notification {
        tracing::error!(
            contract = %contract_key,
            client = 1,
            elapsed_ms = notification_start.elapsed().as_millis(),
            phase = "notification_timeout",
            "UPDATE notification not received within 30 seconds - auto-subscribe via PUT may have failed"
        );
    }
    assert!(
        client1_received_notification,
        "Client 1 did not receive update notification within timeout period (auto-subscribe via PUT failed)"
    );

    tracing::info!(
        contract = %contract_key,
        phase = "test_complete",
        "test_put_with_subscribe_flag completed successfully"
    );
    Ok(())
}

#[freenet_test(
    nodes = ["gateway", "client-node"],
    timeout_secs = 180,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_delegate_request(ctx: &mut TestContext) -> TestResult {
    const TEST_DELEGATE: &str = "test-delegate-integration";

    // Configure environment variables for optimized release build
    std::env::set_var("CARGO_PROFILE_RELEASE_LTO", "true");
    std::env::set_var("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");
    std::env::set_var("CARGO_PROFILE_RELEASE_STRIP", "true");

    // Load delegate (moving this outside the async block)
    let params = Parameters::from(vec![]);
    let delegate = load_delegate(TEST_DELEGATE, params.clone())?;
    let delegate_key = delegate.key().clone();

    let client_node = ctx.node("client-node")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_client = client_node.ws_port;

    // Log data directories for debugging
    tracing::info!("Client node data dir: {:?}", client_node.temp_dir_path);
    tracing::info!("Gateway node data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to the client node's WebSocket API
    let uri =
        format!("ws://127.0.0.1:{ws_api_port_client}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client = WebApi::start(stream);

    // Register the delegate in the node
    client
        .send(ClientRequest::DelegateOp(
            freenet_stdlib::client_api::DelegateRequest::RegisterDelegate {
                delegate: delegate.clone(),
                cipher: freenet_stdlib::client_api::DelegateRequest::DEFAULT_CIPHER,
                nonce: freenet_stdlib::client_api::DelegateRequest::DEFAULT_NONCE,
            },
        ))
        .await?;

    // Wait for registration response
    let resp = tokio::time::timeout(Duration::from_secs(10), client.recv()).await??;
    match resp {
        HostResponse::DelegateResponse { key, values: _ } => {
            assert_eq!(
                key, delegate_key,
                "Delegate key mismatch in register response"
            );
            tracing::info!("Successfully registered delegate with key: {key}");
        }
        other => {
            bail!(
                "Unexpected response while waiting for register: {:?}",
                other
            );
        }
    }

    // Create message for the delegate
    use serde::{Deserialize, Serialize};
    #[derive(Debug, Serialize, Deserialize)]
    enum InboundAppMessage {
        TestRequest(String),
    }

    let app_id = ContractInstanceId::new([0; 32]);
    let request_data = "test-request-data".to_string();
    let payload = bincode::serialize(&InboundAppMessage::TestRequest(request_data.clone()))?;
    let app_msg = ApplicationMessage::new(app_id, payload);

    // Send request to the delegate
    client
        .send(ClientRequest::DelegateOp(
            freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                key: delegate_key.clone(),
                params: params.clone(),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
            },
        ))
        .await?;

    // Wait for delegate response
    let resp = tokio::time::timeout(Duration::from_secs(10), client.recv()).await??;

    match resp {
        HostResponse::DelegateResponse {
            key,
            values: outbound,
        } => {
            assert_eq!(key, delegate_key, "Delegate key mismatch in response");

            assert!(!outbound.is_empty(), "No output messages from delegate");

            let app_msg = match &outbound[0] {
                OutboundDelegateMsg::ApplicationMessage(msg) => msg,
                other => bail!("Expected ApplicationMessage, got {:?}", other),
            };

            assert!(app_msg.processed, "Message not marked as processed");

            #[derive(Debug, Deserialize)]
            enum OutboundAppMessage {
                TestResponse(String, Vec<u8>),
            }

            let response: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;

            match response {
                OutboundAppMessage::TestResponse(text, data) => {
                    assert_eq!(
                        text,
                        format!("Processed: {request_data}"),
                        "Response text doesn't match expected format"
                    );
                    assert_eq!(
                        data,
                        vec![4, 5, 6],
                        "Response data doesn't match expected value"
                    );

                    tracing::info!("Successfully received and verified delegate response");
                }
            }
        }
        other => {
            bail!(
                "Unexpected response while waiting for delegate response: {:?}",
                other
            );
        }
    }

    Ok(())
}

/// Ensure a client-only peer receives PutResponse when the contract is seeded on a third hop.
///
/// This test verifies that PUT responses are properly routed back through forwarding peers,
/// even when the contract is stored on a node that is multiple hops away from the client.
///
/// Network topology:
/// - peer-a (client): Far from contract location
/// - gateway: Intermediate node
/// - peer-c (target): Close to contract location
///
/// Expected flow:
/// 1. peer-a sends PUT → routes through gateway → stored on peer-c
/// 2. peer-c sends PUT response → routes back through gateway → received by peer-a
const THREE_HOP_TEST_CONTRACT: &str = "test-contract-integration";

static THREE_HOP_CONTRACT: LazyLock<(ContractContainer, freenet::dev_tool::Location)> =
    LazyLock::new(|| {
        let contract =
            test_utils::load_contract(THREE_HOP_TEST_CONTRACT, vec![].into()).expect("contract");
        let location = freenet::dev_tool::Location::from(&contract.key());
        (contract, location)
    });

fn three_hop_contract_location() -> freenet::dev_tool::Location {
    let (_, location) = &*THREE_HOP_CONTRACT;
    *location
}

fn three_hop_gateway_location() -> f64 {
    freenet::dev_tool::Location::new_rounded(three_hop_contract_location().as_f64() + 0.2).as_f64()
}

fn three_hop_peer_a_location() -> f64 {
    freenet::dev_tool::Location::new_rounded(three_hop_contract_location().as_f64() + 0.5).as_f64()
}

fn three_hop_peer_c_location() -> f64 {
    three_hop_contract_location().as_f64()
}

fn expected_three_hop_locations() -> [f64; 3] {
    [
        three_hop_gateway_location(),
        three_hop_peer_a_location(),
        three_hop_peer_c_location(),
    ]
}

#[freenet_test(
    nodes = ["gateway", "peer-a", "peer-c"],
    gateways = ["gateway"],
    node_configs = {
        "gateway": { location: three_hop_gateway_location() },
        "peer-a": { location: three_hop_peer_a_location() },
        "peer-c": { location: three_hop_peer_c_location() },
    },
    timeout_secs = 240,
    startup_wait_secs = 15,
    aggregate_events = "on_failure",
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_contract_three_hop_returns_response(ctx: &mut TestContext) -> TestResult {
    use freenet::dev_tool::Location;

    let (contract, contract_location) = {
        let (contract, location) = &*THREE_HOP_CONTRACT;
        (contract.clone(), *location)
    };
    let contract_key = contract.key();
    let node_locations = expected_three_hop_locations();

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Get node information
    let gateway = ctx.node("gateway")?;
    let peer_a = ctx.node("peer-a")?;
    let peer_c = ctx.node("peer-c")?;

    assert_eq!(gateway.location, node_locations[0]);
    assert_eq!(peer_a.location, node_locations[1]);
    assert_eq!(peer_c.location, node_locations[2]);

    tracing::info!("Node A data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Gateway node data dir: {:?}", gateway.temp_dir_path);
    tracing::info!("Node C data dir: {:?}", peer_c.temp_dir_path);
    tracing::info!("Contract location: {}", contract_location.as_f64());

    let gateway_distance = Location::new(gateway.location).distance(contract_location);
    let peer_a_distance = Location::new(peer_a.location).distance(contract_location);
    let peer_c_distance = Location::new(peer_c.location).distance(contract_location);

    // Ensure the contract should naturally route to peer-c to create the 3-hop path:
    // peer-a (client) -> gateway -> peer-c (closest to contract).
    assert!(
        peer_c_distance.as_f64() < gateway_distance.as_f64(),
        "peer-c must be closer to contract than the gateway for three-hop routing"
    );
    assert!(
        peer_c_distance.as_f64() < peer_a_distance.as_f64(),
        "peer-c must be closest node to the contract location"
    );
    tracing::info!(
        "Distances to contract - gateway: {}, peer-a: {}, peer-c: {}",
        gateway_distance.as_f64(),
        peer_a_distance.as_f64(),
        peer_c_distance.as_f64()
    );

    // Connect to peer A's WebSocket API
    let uri_a = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer_a.ws_port
    );
    let (stream_a, _) = connect_async(&uri_a).await?;
    let mut client_api_a = WebApi::start(stream_a);

    // Send PUT from peer A with retry to deflake occasional slow routing in CI.
    send_put_with_retry(
        &mut client_api_a,
        wrapped_state.clone(),
        contract.clone(),
        "three-hop put",
        Some(contract_key),
    )
    .await?;

    // Verify contract can be retrieved from peer C
    let uri_c = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer_c.ws_port
    );
    let (stream_c, _) = connect_async(&uri_c).await?;
    let mut client_api_c = WebApi::start(stream_c);
    // Allow routing to settle and retry GET a few times to deflake under CI load.
    const GET_RETRIES: usize = 3;
    let mut last_err = None;
    for attempt in 1..=GET_RETRIES {
        tracing::info!("Attempt {attempt}/{GET_RETRIES} to GET from peer C");
        tokio::time::sleep(Duration::from_secs(2)).await;
        match get_contract(&mut client_api_c, contract_key, &peer_c.temp_dir_path).await {
            Ok((response_contract, response_state)) => {
                assert_eq!(response_contract, contract);
                assert_eq!(response_state, wrapped_state);
                break;
            }
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        }
    }
    if let Some(err) = last_err {
        bail!("GET from peer C failed after retries: {err}");
    }

    client_api_c
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Clean disconnect from peer A
    client_api_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Note: Gateway may not have the contract cached if the PUT routed directly
    // from peer-a to peer-c (which can happen when peers discover each other
    // through the gateway). In this case, GET from gateway will route to peer-c.
    //
    // Known issue: When gateway forwards GET to peer-c, the ReturnGet response
    // sometimes doesn't reach the client. This appears to be a race condition
    // in the GET response routing. The retry logic here is a workaround until
    // the root cause is identified (see CI logs for transaction flow).
    let uri_b = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        gateway.ws_port
    );
    let (stream_b, _) = connect_async(&uri_b).await?;
    let mut client_api_b = WebApi::start(stream_b);

    // GET from gateway - may need to route to peer-c if not cached locally
    // Use retry logic since network routing adds latency
    const GW_GET_RETRIES: usize = 3;
    let mut gw_last_err = None;
    for attempt in 1..=GW_GET_RETRIES {
        tracing::info!("Attempt {attempt}/{GW_GET_RETRIES} to GET from gateway");
        tokio::time::sleep(Duration::from_secs(1)).await;
        match get_contract(&mut client_api_b, contract_key, &gateway.temp_dir_path).await {
            Ok((gw_contract, gw_state)) => {
                assert_eq!(gw_contract, contract);
                assert_eq!(gw_state, wrapped_state);
                gw_last_err = None;
                break;
            }
            Err(e) => {
                gw_last_err = Some(e);
                if attempt < GW_GET_RETRIES {
                    tracing::warn!("Gateway GET attempt {attempt} failed, retrying...");
                }
            }
        }
    }
    if let Some(err) = gw_last_err {
        bail!("GET from gateway failed after retries: {err}");
    }

    client_api_b
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "Long-running test (90s) - needs update for new keep-alive constants"]
async fn test_gateway_packet_size_change_after_60s() -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_gw1 = TcpListener::bind("127.0.0.1:0")?;
    let network_socket_gw2 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_client = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw1 = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw2 = TcpListener::bind("127.0.0.1:0")?;

    // Configure first gateway node
    let (config_gw1, preset_cfg_gw1, config_gw1_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_gw1.local_addr()?.port()),
            ws_api_port_socket_gw1.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure second gateway node (connects to first gateway)
    let (config_gw2, preset_cfg_gw2, config_gw2_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![serde_json::to_string(&config_gw1_info)?], // Connect to gateway 1
            Some(network_socket_gw2.local_addr()?.port()),
            ws_api_port_socket_gw2.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node (connects via gateway 2)
    let (config_client, preset_cfg_client) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw2_info)?],
        None,
        ws_api_port_socket_client.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_client = config_client.ws_api.ws_api_port.unwrap();

    // Log data directories
    tracing::info!(
        "Client node data dir: {:?}",
        preset_cfg_client.temp_dir.path()
    );
    tracing::info!(
        "Gateway 1 node data dir: {:?}",
        preset_cfg_gw1.temp_dir.path()
    );
    tracing::info!(
        "Gateway 2 node data dir: {:?}",
        preset_cfg_gw2.temp_dir.path()
    );

    // Free ports
    std::mem::drop(ws_api_port_socket_client);
    std::mem::drop(network_socket_gw1);
    std::mem::drop(network_socket_gw2);
    std::mem::drop(ws_api_port_socket_gw1);
    std::mem::drop(ws_api_port_socket_gw2);

    // Start gateway 1 node
    let node_gw1 = async {
        let config = config_gw1.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start gateway 2 node (connects to gateway 1)
    let node_gw2 = async {
        let config = config_gw2.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node
    let node_client = async move {
        let config = config_client.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start (gateways need to connect to each other)
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect to client node
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_client}/v1/contract/command?encodingProtocol=native"
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client = WebApi::start(stream);

        // Put contract
        make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

        // Wait for put response
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key);
                tracing::info!("Successfully put contract");
            }
            _ => {
                bail!("Failed to put contract");
            }
        }

        // Now keep the connection alive for 90 seconds, sending periodic GET requests
        tracing::info!("Starting packet size change test - monitoring for 75 seconds");
        let start_time = std::time::Instant::now();
        let mut get_count = 0;
        let mut error_count = 0;

        while start_time.elapsed() < Duration::from_secs(75) {
            // Send a GET request every 5 seconds for more frequent monitoring
            tokio::time::sleep(Duration::from_secs(5)).await;
            get_count += 1;

            let elapsed = start_time.elapsed();
            tracing::info!("Sending GET request #{} at {:?}", get_count, elapsed);

            // Log if we're past the 60-second mark where errors typically start
            if elapsed > Duration::from_secs(60) {
                tracing::warn!("Past 60-second mark - monitoring for packet size changes");
            }

            make_get(&mut client, contract_key, false, false).await?;

            // Try to receive response with a shorter timeout
            match tokio::time::timeout(Duration::from_secs(10), client.recv()).await {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key,
                    ..
                }))) => {
                    assert_eq!(key, contract_key);
                    tracing::info!("GET request #{} succeeded", get_count);
                }
                Ok(Ok(other)) => {
                    tracing::warn!(
                        "GET request #{} unexpected response: {:?}",
                        get_count,
                        other
                    );
                    error_count += 1;
                }
                Ok(Err(e)) => {
                    tracing::error!("GET request #{} error: {}", get_count, e);
                    error_count += 1;
                }
                Err(_) => {
                    tracing::error!("GET request #{} timed out", get_count);
                    error_count += 1;
                }
            }
        }

        tracing::info!(
            "Long-running test completed: {} GET requests, {} errors",
            get_count,
            error_count
        );

        // The test passes if we don't crash with decryption errors
        // In production, decryption errors would cause the connection to fail
        if error_count > get_count / 2 {
            bail!("Too many errors during long-running connection test");
        }

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        gw1 = node_gw1 => {
            let Err(e) = gw1;
            return Err(anyhow!("Gateway 1 node failed: {}", e).into())
        }
        gw2 = node_gw2 => {
            let Err(e) = gw2;
            return Err(anyhow!("Gateway 2 node failed: {}", e).into())
        }
        client = node_client => {
            let Err(e) = client;
            return Err(anyhow!("Client node failed: {}", e).into())
        }
        r = test => {
            r??;
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "Long-running test (75s) - run with --ignored flag"]
async fn test_production_decryption_error_scenario() -> TestResult {
    // This test attempts to reproduce the exact production scenario:
    // 1. Client connects to gateway (vega)
    // 2. Connection works fine for ~60 seconds with 48-byte packets
    // 3. After 60 seconds, 256-byte packets arrive that fail to decrypt

    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create sockets
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_client = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway (simulating vega)
    let (config_gw, preset_cfg_gw, config_gw_info) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_gw.local_addr()?.port()),
            ws_api_port_socket_gw.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node
    let (config_client, preset_cfg_client) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_gw_info)?],
        None,
        ws_api_port_socket_client.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_client = config_client.ws_api.ws_api_port.unwrap();

    tracing::info!(
        "Client node data dir: {:?}",
        preset_cfg_client.temp_dir.path()
    );
    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());

    // Free ports
    std::mem::drop(ws_api_port_socket_client);
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);

    // Start nodes
    let node_gw = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let node_client = async move {
        let config = config_client.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(90), async {
        // Wait for nodes to start
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Connect to client node
        let uri = format!(
            "ws://127.0.0.1:{ws_api_port_client}/v1/contract/command?encodingProtocol=native"
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client = WebApi::start(stream);

        // Put contract
        make_put(&mut client, wrapped_state.clone(), contract.clone(), false).await?;

        // Wait for put response
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                assert_eq!(key, contract_key);
                tracing::info!("Successfully put contract");
            }
            _ => {
                bail!("Failed to put contract");
            }
        }

        // Monitor connection for 75 seconds
        tracing::info!("Starting production scenario simulation - monitoring for 75 seconds");
        let start_time = std::time::Instant::now();
        let mut last_success_time = start_time;
        let mut error_count = 0;
        let mut success_count = 0;

        while start_time.elapsed() < Duration::from_secs(75) {
            tokio::time::sleep(Duration::from_secs(3)).await;

            let elapsed = start_time.elapsed();

            // Try a GET request
            make_get(&mut client, contract_key, false, false).await?;

            match tokio::time::timeout(Duration::from_secs(5), client.recv()).await {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key,
                    ..
                }))) => {
                    assert_eq!(key, contract_key);
                    success_count += 1;
                    last_success_time = std::time::Instant::now();
                    tracing::info!(
                        "GET succeeded at {:?} (success #{})",
                        elapsed,
                        success_count
                    );
                }
                Ok(Ok(other)) => {
                    error_count += 1;
                    tracing::error!("GET unexpected response at {:?}: {:?}", elapsed, other);
                }
                Ok(Err(e)) => {
                    error_count += 1;
                    tracing::error!("GET error at {:?}: {}", elapsed, e);
                }
                Err(_) => {
                    error_count += 1;
                    tracing::error!("GET timeout at {:?}", elapsed);
                }
            }

            // Log status around the critical 60-second mark
            if elapsed > Duration::from_secs(58) && elapsed < Duration::from_secs(65) {
                tracing::warn!(
                    "Critical period - elapsed: {:?}, errors: {}, last success: {:?} ago",
                    elapsed,
                    error_count,
                    std::time::Instant::now().duration_since(last_success_time)
                );
            }
        }

        tracing::info!(
            "Test completed: {} successes, {} errors",
            success_count,
            error_count
        );

        // In production, all requests fail after ~60 seconds
        // For now, we just log the results to see if we can reproduce the pattern

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        gw = node_gw => {
            let Err(e) = gw;
            return Err(anyhow!("Gateway node failed: {}", e).into())
        }
        client = node_client => {
            let Err(e) = client;
            return Err(anyhow!("Client node failed: {}", e).into())
        }
        r = test => {
            r??;
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

// Helper functions for future full subscription testing
#[allow(dead_code)]
async fn wait_for_put_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<ContractKey, anyhow::Error> {
    let resp = timeout(Duration::from_secs(30), client.recv()).await??;
    match resp {
        HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => {
            if &key != expected_key {
                bail!(
                    "Put response key mismatch: expected {}, got {}",
                    expected_key,
                    key
                );
            }
            Ok(key)
        }
        other => {
            bail!("Unexpected response while waiting for put: {:?}", other);
        }
    }
}

#[allow(dead_code)]
async fn wait_for_subscribe_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<(), anyhow::Error> {
    let resp = timeout(Duration::from_secs(10), client.recv()).await??;
    match resp {
        HostResponse::ContractResponse(ContractResponse::SubscribeResponse { key, .. }) => {
            if &key != expected_key {
                bail!(
                    "Subscribe response key mismatch: expected {}, got {}",
                    expected_key,
                    key
                );
            }
            Ok(())
        }
        other => {
            bail!(
                "Unexpected response while waiting for subscribe: {:?}",
                other
            );
        }
    }
}

#[freenet_test(
    nodes = ["gateway", "peer-node"],
    timeout_secs = 180,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_subscription_introspection(ctx: &mut TestContext) -> TestResult {
    // Load test contract - not used in this simplified test
    const TEST_CONTRACT: &str = "test-contract-integration";
    let _contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;

    // Create initial state - not used in this simplified test
    let _initial_state = test_utils::create_empty_todo_list();

    let gateway = ctx.node("gateway")?;
    let peer_node = ctx.node("peer-node")?;
    let ws_api_port_gw = gateway.ws_port;
    let ws_api_port_node = peer_node.ws_port;

    tracing::info!("Gateway data dir: {:?}", gateway.temp_dir_path);
    tracing::info!("Node data dir: {:?}", peer_node.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to gateway websocket API
    let uri_gw =
        format!("ws://127.0.0.1:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
    let (stream_gw, _) = connect_async(&uri_gw).await?;
    let mut client_gw = WebApi::start(stream_gw);

    // Connect to node websocket API
    let uri_node =
        format!("ws://127.0.0.1:{ws_api_port_node}/v1/contract/command?encodingProtocol=native");
    let (stream_node, _) = connect_async(&uri_node).await?;
    let _client_node = WebApi::start(stream_node);

    // First just test that we can query subscription info
    tracing::info!("Testing basic subscription query without any subscriptions");

    // Query subscription info from gateway
    tracing::info!("Querying subscription info from gateway");
    client_gw
        .send(ClientRequest::NodeQueries(
            freenet_stdlib::client_api::NodeQuery::SubscriptionInfo,
        ))
        .await?;

    // Wait for subscription info response
    let resp = timeout(Duration::from_secs(5), client_gw.recv()).await??;

    match resp {
        HostResponse::QueryResponse(QueryResponse::NetworkDebug(info)) => {
            tracing::info!("Gateway subscription info:");
            tracing::info!("  Connected peers: {:?}", info.connected_peers);
            tracing::info!("  Total subscriptions: {}", info.subscriptions.len());

            // Should be empty since we haven't subscribed to anything
            assert!(
                info.subscriptions.is_empty(),
                "Expected no subscriptions initially"
            );
            tracing::info!("Test passed - query subscription info works");
        }
        other => {
            bail!("Unexpected response: {:?}", other);
        }
    }

    Ok(())
}

#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 180,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_update_no_change_notification(ctx: &mut TestContext) -> TestResult {
    // Load test contract that properly handles NoChange
    const TEST_CONTRACT: &str = "test-contract-update-nochange";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state - a simple state that we can update
    #[derive(serde::Serialize, serde::Deserialize)]
    struct SimpleState {
        value: String,
        counter: u64,
    }

    let initial_state = SimpleState {
        value: "initial".to_string(),
        counter: 1,
    };
    let initial_state_bytes = serde_json::to_vec(&initial_state)?;
    let wrapped_state = WrappedState::from(initial_state_bytes);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port = peer_a.ws_port;

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Node B (gw) data dir: {:?}", gateway.temp_dir_path);

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to node A websocket API
    let uri = format!("ws://127.0.0.1:{ws_api_port}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api_a = WebApi::start(stream);

    // Put contract with initial state
    make_put(
        &mut client_api_a,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for put response
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
        }
        Ok(Ok(other)) => {
            tracing::warn!("unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for put response");
        }
    }

    // Now update with the EXACT SAME state (should trigger UpdateNoChange)
    tracing::info!("Sending UPDATE with identical state to trigger UpdateNoChange");
    make_update(&mut client_api_a, contract_key, wrapped_state.clone()).await?;

    // Wait for update response - THIS SHOULD NOT TIMEOUT
    let resp = tokio::time::timeout(Duration::from_secs(30), client_api_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key,
            summary: _,
        }))) => {
            assert_eq!(
                key, contract_key,
                "Contract key mismatch in UPDATE response"
            );
            tracing::info!("SUCCESS: Received UpdateResponse for no-change update");
        }
        Ok(Ok(other)) => {
            bail!("Unexpected response while waiting for update: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving update response: {}", e);
        }
        Err(_) => {
            // This is where the test will currently fail
            bail!("TIMEOUT waiting for update response - UpdateNoChange bug: client not notified when update results in no state change");
        }
    }

    Ok(())
}

/// Regression test for issue #2301: UPDATE broadcast NoChange false positive
///
/// This test verifies that when a node updates a contract, the update is properly
/// broadcast to other nodes that have cached the contract. Prior to the fix,
/// the change detection logic ran AFTER the local state was committed, causing
/// the comparison to always return NoChange and preventing any broadcast.
///
/// The test validates the fix by:
/// 1. Having peer-a PUT a contract
/// 2. Having gateway GET the contract (so it's cached on gateway)
/// 3. Having peer-a UPDATE the contract with new state
/// 4. Verifying that gateway's cached state is updated via broadcast
///
/// If the NoChange bug is present, gateway would still have the old state
/// because the UPDATE broadcast would be skipped.
// Test configuration values:
// - timeout_secs: 180s to account for slow CI environments and network propagation delays
// - startup_wait_secs: 15s to allow nodes to fully start and establish connections
// - tokio_worker_threads: 4 to provide adequate concurrency for multi-node test
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 180,
    startup_wait_secs = 15,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_update_broadcast_propagation_issue_2301(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_initial_state = WrappedState::from(initial_state);

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;
    let ws_api_port_peer_a = peer_a.ws_port;
    let ws_api_port_gateway = gateway.ws_port;

    tracing::info!("Peer A data dir: {:?}", peer_a.temp_dir_path);
    tracing::info!("Gateway data dir: {:?}", gateway.temp_dir_path);

    // Additional wait for peer-to-gateway connection to stabilize after startup_wait_secs.
    // This helps avoid race conditions in CI where connection establishment may take longer.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to peer-a's websocket API
    let uri_peer_a =
        format!("ws://127.0.0.1:{ws_api_port_peer_a}/v1/contract/command?encodingProtocol=native");
    let (stream_a, _) = connect_async(&uri_peer_a).await?;
    let mut client_peer_a = WebApi::start(stream_a);

    // Connect to gateway's websocket API
    let uri_gateway =
        format!("ws://127.0.0.1:{ws_api_port_gateway}/v1/contract/command?encodingProtocol=native");
    let (stream_gw, _) = connect_async(&uri_gateway).await?;
    let mut client_gateway = WebApi::start(stream_gw);

    // Step 1: Peer-a puts the contract with initial state
    tracing::info!("Step 1: Peer-a putting contract with initial state");
    make_put(
        &mut client_peer_a,
        wrapped_initial_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for put response from peer-a
    let resp = tokio::time::timeout(Duration::from_secs(120), client_peer_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
            tracing::info!("Peer-a: PUT successful for contract {}", key);
        }
        Ok(Ok(other)) => {
            bail!(
                "Peer-a: unexpected response while waiting for put: {:?}",
                other
            );
        }
        Ok(Err(e)) => {
            bail!("Peer-a: Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Peer-a: Timeout waiting for put response");
        }
    }

    // Step 2: Gateway gets the contract (this caches it on gateway)
    tracing::info!("Step 2: Gateway getting contract to cache it");
    make_get(&mut client_gateway, contract_key, true, false).await?;

    // Wait for get response on gateway
    let resp = tokio::time::timeout(Duration::from_secs(60), client_gateway.recv()).await;
    let initial_state_on_gateway: WrappedState = match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            key,
            state,
            ..
        }))) => {
            assert_eq!(key, contract_key, "Contract key mismatch in GET response");
            tracing::info!("Gateway: GET successful, contract cached");
            state
        }
        Ok(Ok(other)) => {
            bail!(
                "Gateway: unexpected response while waiting for get: {:?}",
                other
            );
        }
        Ok(Err(e)) => {
            bail!("Gateway: Error receiving get response: {}", e);
        }
        Err(_) => {
            bail!("Gateway: Timeout waiting for get response");
        }
    };

    // Step 3: Peer-a updates the contract with new state
    tracing::info!("Step 3: Peer-a updating contract with new state");

    // Create updated state (add a todo item)
    // Using expect() to fail fast if state is corrupted - this is a regression test,
    // so we want clear failure on any unexpected condition.
    let mut todo_list: test_utils::TodoList =
        serde_json::from_slice(wrapped_initial_state.as_ref())
            .expect("Failed to deserialize wrapped_initial_state as TodoList");
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "Issue 2301 regression test".to_string(),
        description: "This task should propagate to gateway via UPDATE broadcast".to_string(),
        completed: false,
        priority: 1,
    });
    let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
    let wrapped_updated_state = WrappedState::from(updated_bytes);

    make_update(
        &mut client_peer_a,
        contract_key,
        wrapped_updated_state.clone(),
    )
    .await?;

    // Wait for update response on peer-a
    let resp = tokio::time::timeout(Duration::from_secs(30), client_peer_a.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
            key, ..
        }))) => {
            assert_eq!(
                key, contract_key,
                "Contract key mismatch in UPDATE response"
            );
            tracing::info!("Peer-a: UPDATE successful");
        }
        Ok(Ok(other)) => {
            bail!(
                "Peer-a: unexpected response while waiting for update: {:?}",
                other
            );
        }
        Ok(Err(e)) => {
            bail!("Peer-a: Error receiving update response: {}", e);
        }
        Err(_) => {
            bail!("Peer-a: Timeout waiting for update response");
        }
    }

    // Step 4: Verify gateway received the update by polling GET
    //
    // Note: We poll GET instead of using subscription notifications because cross-node
    // subscription notifications have known reliability issues (see test_multiple_clients_subscription).
    // While polling doesn't prove the broadcast mechanism specifically, it verifies the end behavior
    // that this regression test cares about: gateway has the updated state.
    //
    // The key insight is that without the fix, the gateway would NEVER receive the update because
    // the broadcast would be skipped entirely due to the NoChange false positive.
    tracing::info!("Step 4: Verifying gateway received the update via polling GET");

    // Polling configuration
    const POLL_INTERVAL_SECS: u64 = 2; // Interval between GET requests
    const POLL_TIMEOUT_SECS: u64 = 30; // Maximum time to wait for update propagation

    let poll_start = std::time::Instant::now();
    let poll_timeout = Duration::from_secs(POLL_TIMEOUT_SECS);
    let mut gateway_received_update = false;

    while poll_start.elapsed() < poll_timeout {
        // Small delay between polls to avoid overwhelming the gateway
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;

        make_get(&mut client_gateway, contract_key, false, false).await?;

        let resp = tokio::time::timeout(Duration::from_secs(10), client_gateway.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                state,
                ..
            }))) => {
                assert_eq!(key, contract_key);

                // Check if the state has been updated
                if state.as_ref() != initial_state_on_gateway.as_ref() {
                    // State changed - verify it matches the expected updated state
                    let state_on_gateway: test_utils::TodoList =
                        serde_json::from_slice(state.as_ref())
                            .expect("Failed to deserialize gateway state");

                    if !state_on_gateway.tasks.is_empty()
                        && state_on_gateway.tasks[0].title == "Issue 2301 regression test"
                    {
                        tracing::info!(
                            "SUCCESS: Gateway received the UPDATE broadcast! State updated with {} task(s)",
                            state_on_gateway.tasks.len()
                        );
                        gateway_received_update = true;
                        break;
                    } else {
                        // State changed but doesn't match expected - log for debugging
                        tracing::warn!(
                            "Gateway state changed but did not match expected task. \
                             Tasks: {:?}, version: {}",
                            state_on_gateway.tasks,
                            state_on_gateway.version
                        );
                    }
                }
            }
            Ok(Ok(other)) => {
                tracing::warn!("Gateway poll: unexpected response: {:?}", other);
            }
            Ok(Err(e)) => {
                tracing::warn!("Gateway poll: error: {}", e);
            }
            Err(_) => {
                tracing::warn!("Gateway poll: timeout");
            }
        }

        tracing::debug!(
            "Gateway state not yet updated, polling... (elapsed: {:?})",
            poll_start.elapsed()
        );
    }

    // REGRESSION TEST: If the bug in issue #2301 is present, the UPDATE would not be
    // broadcast because change detection would return NoChange (false positive).
    // The fix ensures that in network mode, the state is not committed before the
    // network operation, allowing proper change detection and broadcasting.
    ensure!(
        gateway_received_update,
        "REGRESSION FAILURE (Issue #2301): Gateway did not receive UPDATE broadcast. \
         This indicates the NoChange false positive bug is present - the UPDATE was \
         not broadcast to other nodes because change detection incorrectly returned \
         NoChange after the local state was already committed."
    );

    tracing::info!(
        "Issue #2301 regression test passed: UPDATE broadcast propagation works correctly"
    );

    Ok(())
}

/// Regression test for issue #2326: Subscribe should complete locally when contract is cached,
/// even if remote peers exist that don't have the contract yet.
///
/// Before the fix, Subscribe would forward to a remote peer even when the contract was
/// cached locally. If that peer didn't have the contract, it would return subscribed=false,
/// causing the client to think subscription failed.
///
/// This test validates the real user scenario:
/// 1. Client does PUT (contract is cached locally)
/// 2. Client immediately does Subscribe (before propagation to other peers)
/// 3. Subscribe should succeed locally without network round-trip
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_then_immediate_subscribe_succeeds_locally_regression_2326(
    ctx: &mut TestContext,
) -> TestResult {
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let peer_a = ctx.node("peer-a")?;
    let ws_api_port = peer_a.ws_port;

    tracing::info!(
        "Regression test #2326: Testing PUT then immediate Subscribe on peer-a (ws_port: {})",
        ws_api_port
    );

    // Give time for peer to connect to gateway (so remote peers exist)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Connect to peer-a's websocket API
    let uri = format!("ws://127.0.0.1:{ws_api_port}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api = WebApi::start(stream);

    // Step 1: PUT with subscribe=false (like River room creation)
    tracing::info!("Step 1: PUT contract with subscribe=false");
    make_put(
        &mut client_api,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for PUT response
    let put_resp = tokio::time::timeout(Duration::from_secs(30), client_api.recv()).await;
    match put_resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            tracing::info!("PUT successful for contract: {}", key);
            assert_eq!(key, contract_key);
        }
        Ok(Ok(other)) => {
            bail!("Unexpected response while waiting for PUT: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving PUT response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for PUT response");
        }
    }

    // Step 2: IMMEDIATELY Subscribe (don't wait for propagation to gateway)
    // This is the critical part - the contract is local but gateway may not have it yet
    tracing::info!("Step 2: Immediately Subscribe (before propagation to gateway)");
    make_subscribe(&mut client_api, contract_key).await?;

    // Wait for Subscribe response
    let sub_resp = tokio::time::timeout(Duration::from_secs(10), client_api.recv()).await;
    match sub_resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
            key,
            subscribed,
            ..
        }))) => {
            // REGRESSION CHECK: Before fix #2326, this would return subscribed=false
            // because Subscribe forwarded to gateway (which didn't have the contract yet)
            ensure!(
                subscribed,
                "REGRESSION FAILURE (Issue #2326): Subscribe returned subscribed=false. \
                 This indicates the bug is present - Subscribe forwarded to a remote peer \
                 instead of completing locally for a cached contract."
            );
            tracing::info!(
                "Subscribe successful for contract: {} (completed locally as expected)",
                key
            );
            assert_eq!(key, contract_key);
        }
        Ok(Ok(other)) => {
            bail!(
                "Unexpected response while waiting for Subscribe: {:?}",
                other
            );
        }
        Ok(Err(e)) => {
            bail!("Error receiving Subscribe response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for Subscribe response");
        }
    }

    // Cleanup
    client_api
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    tracing::info!("Issue #2326 regression test passed: PUT then immediate Subscribe works");

    Ok(())
}

/// Test subscription tree pruning (Issue #2166).
///
/// This test validates that when a subscriber disconnects, updates are no longer
/// sent to that peer. The pruning mechanism should clean up dead subscription branches.
///
/// Test flow:
/// 1. Gateway puts a contract
/// 2. Peer-A subscribes and receives the initial state
/// 3. Gateway updates the contract → Peer-A receives the update
/// 4. Peer-A disconnects
/// 5. Gateway updates the contract again → should complete without errors
///    (no failed sends to disconnected peer)
///
/// The test verifies pruning indirectly by ensuring updates complete successfully
/// after a subscriber disconnects, without errors from trying to send to dead peers.
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 180,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_subscription_tree_pruning(ctx: &mut TestContext) -> TestResult {
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let gateway = ctx.node("gateway")?;
    let peer_a = ctx.node("peer-a")?;

    tracing::info!(
        "Nodes: gateway ws={}, peer-a ws={}",
        gateway.ws_port,
        peer_a.ws_port
    );

    tokio::time::sleep(Duration::from_secs(5)).await;

    let uri_gw = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        gateway.ws_port
    );
    let (stream_gw, _) = connect_async(&uri_gw).await?;
    let mut client_gw = WebApi::start(stream_gw);

    let uri_a = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer_a.ws_port
    );
    let (stream_a, _) = connect_async(&uri_a).await?;
    let mut client_a = WebApi::start(stream_a);

    tracing::info!("Step 1: Putting contract on gateway");
    make_put(
        &mut client_gw,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    loop {
        let resp = timeout(Duration::from_secs(30), client_gw.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => {
                assert_eq!(key, contract_key, "Contract key mismatch");
                tracing::info!("Contract put successfully: {}", key);
                break;
            }
            other => {
                tracing::warn!("Unexpected response during put: {:?}", other);
            }
        }
    }

    tracing::info!("Step 2: Subscribing peer-a to contract");
    make_subscribe(&mut client_a, contract_key).await?;

    loop {
        let resp = timeout(Duration::from_secs(60), client_a.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }) => {
                assert_eq!(key, contract_key);
                assert!(subscribed, "Peer-a failed to subscribe");
                tracing::info!("Peer-a subscribed successfully");
                break;
            }
            other => {
                tracing::warn!("Peer-a: unexpected response: {:?}", other);
            }
        }
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    tracing::info!("Step 2.5: Verifying peer-a is in gateway's subscribers");
    let peer_a_diag_config = NodeDiagnosticsConfig {
        include_node_info: true,
        include_network_info: false,
        include_subscriptions: false,
        contract_keys: vec![],
        include_system_metrics: false,
        include_detailed_peer_info: false,
        include_subscriber_peer_ids: false,
    };
    make_node_diagnostics(&mut client_a, peer_a_diag_config).await?;

    let peer_a_peer_id = loop {
        let resp = timeout(Duration::from_secs(10), client_a.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let node_info = diag
                    .node_info
                    .ok_or_else(|| anyhow!("Missing node_info in peer-a diagnostics"))?;
                tracing::info!("Peer-a peer_id: {}", node_info.peer_id);
                break node_info.peer_id;
            }
            other => {
                tracing::warn!(
                    "Peer-a: unexpected response while getting peer_id: {:?}",
                    other
                );
            }
        }
    };

    let gw_diag_config = NodeDiagnosticsConfig {
        include_node_info: false,
        include_network_info: false,
        include_subscriptions: false,
        contract_keys: vec![contract_key],
        include_system_metrics: false,
        include_detailed_peer_info: false,
        include_subscriber_peer_ids: true,
    };
    make_node_diagnostics(&mut client_gw, gw_diag_config.clone()).await?;

    let gateway_subscribers_before = loop {
        let resp = timeout(Duration::from_secs(10), client_gw.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let contract_state = diag.contract_states.get(&contract_key);
                let subscribers = contract_state
                    .map(|cs| cs.subscriber_peer_ids.clone())
                    .unwrap_or_default();
                tracing::info!(
                    "Gateway subscribers for contract (before disconnect): {:?}",
                    subscribers
                );
                break subscribers;
            }
            other => {
                tracing::warn!(
                    "Gateway: unexpected response while getting subscribers: {:?}",
                    other
                );
            }
        }
    };

    assert!(
        gateway_subscribers_before.contains(&peer_a_peer_id),
        "Peer-a should be in gateway's subscriber list. peer_id {} not in {:?}",
        peer_a_peer_id,
        gateway_subscribers_before
    );
    tracing::info!("Verified: peer-a is in gateway's subscription tree");

    tracing::info!("Step 3: Updating contract");
    let mut todo_list: test_utils::TodoList = serde_json::from_slice(wrapped_state.as_ref())?;
    todo_list.tasks.push(test_utils::Task {
        id: 1,
        title: "First update".to_string(),
        description: "Testing subscription".to_string(),
        completed: false,
        priority: 1,
    });
    let update_state = WrappedState::from(serde_json::to_vec(&todo_list)?);

    make_update(&mut client_gw, contract_key, update_state).await?;

    loop {
        let resp = timeout(Duration::from_secs(30), client_gw.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::UpdateResponse { key, .. }) => {
                assert_eq!(key, contract_key);
                tracing::info!("Gateway: Update completed successfully");
                break;
            }
            other => {
                tracing::warn!("Gateway: unexpected response: {:?}", other);
            }
        }
    }

    let mut peer_a_received_update = false;
    for _ in 0..10 {
        match timeout(Duration::from_secs(3), client_a.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                ..
            }))) => {
                assert_eq!(key, contract_key);
                tracing::info!("Peer-a received update notification");
                peer_a_received_update = true;
                break;
            }
            Ok(Ok(other)) => {
                tracing::warn!("Peer-a: unexpected response: {:?}", other);
            }
            Ok(Err(e)) => {
                tracing::warn!("Peer-a: error: {}", e);
            }
            Err(_) => {
                tracing::debug!("Peer-a: timeout waiting for update, retrying...");
            }
        }
    }

    if !peer_a_received_update {
        tracing::warn!("Peer-a did not receive update notification (may be network topology)");
    }

    tracing::info!("Step 4: Disconnecting peer-a (triggers pruning)");
    drop(client_a);

    tracing::info!("Waiting for disconnect processing...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    tracing::info!("Step 4.5: Verifying peer-a removed from gateway's subscribers");

    make_node_diagnostics(&mut client_gw, gw_diag_config).await?;

    let gateway_subscribers_after = loop {
        let resp = timeout(Duration::from_secs(10), client_gw.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let contract_state = diag.contract_states.get(&contract_key);
                let subscribers = contract_state
                    .map(|cs| cs.subscriber_peer_ids.clone())
                    .unwrap_or_default();
                tracing::info!(
                    "Gateway subscribers for contract (after disconnect): {:?}",
                    subscribers
                );
                break subscribers;
            }
            other => {
                tracing::warn!(
                    "Gateway: unexpected response while getting subscribers after disconnect: {:?}",
                    other
                );
            }
        }
    };

    assert!(
        !gateway_subscribers_after.contains(&peer_a_peer_id),
        "Peer-a should NOT be in gateway's subscriber list after disconnect. \
         peer_id {} found in {:?}",
        peer_a_peer_id,
        gateway_subscribers_after
    );

    tracing::info!("Subscription tree pruning test passed");
    Ok(())
}

/// Test that multiple client subscriptions prevent premature upstream notification (Issue #2166).
///
/// This test validates that the subscription tree is NOT pruned while ANY local client
/// remains subscribed. Only when ALL clients disconnect should the Unsubscribed message
/// be sent to the upstream peer.
///
/// Test flow:
/// 1. Gateway puts a contract
/// 2. Two clients on Peer-A subscribe to the contract
/// 3. First client disconnects → Peer-A should still be in Gateway's subscribers
///    (because second client is still subscribed)
/// 4. Second client disconnects → Peer-A should be removed from Gateway's subscribers
///    (no remaining client subscriptions)
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 180,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_multiple_clients_prevent_premature_pruning(ctx: &mut TestContext) -> TestResult {
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let gateway = ctx.node("gateway")?;
    let peer_a = ctx.node("peer-a")?;

    tracing::info!(
        "Nodes: gateway ws={}, peer-a ws={}",
        gateway.ws_port,
        peer_a.ws_port
    );

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect gateway client
    let uri_gw = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        gateway.ws_port
    );
    let (stream_gw, _) = connect_async(&uri_gw).await?;
    let mut client_gw = WebApi::start(stream_gw);

    // Step 1: Gateway puts contract
    tracing::info!("Step 1: Putting contract on gateway");
    make_put(
        &mut client_gw,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    loop {
        let resp = timeout(Duration::from_secs(30), client_gw.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => {
                assert_eq!(key, contract_key, "Contract key mismatch");
                tracing::info!("Contract put successfully: {}", key);
                break;
            }
            other => {
                tracing::warn!("Unexpected response during put: {:?}", other);
            }
        }
    }

    // Step 2: Connect two clients on peer-a and subscribe both
    let uri_a = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer_a.ws_port
    );
    let (stream_a1, _) = connect_async(&uri_a).await?;
    let mut client_a1 = WebApi::start(stream_a1);

    let (stream_a2, _) = connect_async(&uri_a).await?;
    let mut client_a2 = WebApi::start(stream_a2);

    tracing::info!("Step 2: Subscribing first client on peer-a");
    make_subscribe(&mut client_a1, contract_key).await?;

    loop {
        let resp = timeout(Duration::from_secs(60), client_a1.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }) => {
                assert_eq!(key, contract_key);
                assert!(subscribed, "First client failed to subscribe");
                tracing::info!("First client subscribed successfully");
                break;
            }
            other => {
                tracing::warn!("Client 1: unexpected response: {:?}", other);
            }
        }
    }

    tracing::info!("Step 2b: Subscribing second client on peer-a");
    make_subscribe(&mut client_a2, contract_key).await?;

    loop {
        let resp = timeout(Duration::from_secs(60), client_a2.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }) => {
                assert_eq!(key, contract_key);
                assert!(subscribed, "Second client failed to subscribe");
                tracing::info!("Second client subscribed successfully");
                break;
            }
            other => {
                tracing::warn!("Client 2: unexpected response: {:?}", other);
            }
        }
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 3: Get peer-a's peer_id for verification
    let peer_a_diag_config = NodeDiagnosticsConfig {
        include_node_info: true,
        include_network_info: false,
        include_subscriptions: false,
        contract_keys: vec![],
        include_system_metrics: false,
        include_detailed_peer_info: false,
        include_subscriber_peer_ids: false,
    };
    make_node_diagnostics(&mut client_a1, peer_a_diag_config).await?;

    let peer_a_peer_id = loop {
        let resp = timeout(Duration::from_secs(10), client_a1.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let node_info = diag
                    .node_info
                    .ok_or_else(|| anyhow!("Missing node_info in peer-a diagnostics"))?;
                tracing::info!("Peer-a peer_id: {}", node_info.peer_id);
                break node_info.peer_id;
            }
            other => {
                tracing::warn!("Peer-a: unexpected response: {:?}", other);
            }
        }
    };

    // Step 4: Verify gateway has peer-a in subscribers
    let gw_diag_config = NodeDiagnosticsConfig {
        include_node_info: false,
        include_network_info: false,
        include_subscriptions: false,
        contract_keys: vec![contract_key],
        include_system_metrics: false,
        include_detailed_peer_info: false,
        include_subscriber_peer_ids: true,
    };
    make_node_diagnostics(&mut client_gw, gw_diag_config.clone()).await?;

    let gateway_subscribers_initial = loop {
        let resp = timeout(Duration::from_secs(10), client_gw.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let contract_state = diag.contract_states.get(&contract_key);
                let subscribers = contract_state
                    .map(|cs| cs.subscriber_peer_ids.clone())
                    .unwrap_or_default();
                tracing::info!(
                    "Gateway subscribers (both clients connected): {:?}",
                    subscribers
                );
                break subscribers;
            }
            other => {
                tracing::warn!("Gateway: unexpected response: {:?}", other);
            }
        }
    };

    assert!(
        gateway_subscribers_initial.contains(&peer_a_peer_id),
        "Peer-a should be in gateway's subscriber list initially"
    );

    // Step 5: Disconnect first client - peer-a should STILL be in subscribers
    tracing::info!("Step 5: Disconnecting first client (pruning should NOT occur)");
    drop(client_a1);

    tokio::time::sleep(Duration::from_secs(5)).await;

    make_node_diagnostics(&mut client_gw, gw_diag_config.clone()).await?;

    let gateway_subscribers_after_first = loop {
        let resp = timeout(Duration::from_secs(10), client_gw.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let contract_state = diag.contract_states.get(&contract_key);
                let subscribers = contract_state
                    .map(|cs| cs.subscriber_peer_ids.clone())
                    .unwrap_or_default();
                tracing::info!(
                    "Gateway subscribers (after first client disconnect): {:?}",
                    subscribers
                );
                break subscribers;
            }
            other => {
                tracing::warn!("Gateway: unexpected response: {:?}", other);
            }
        }
    };

    assert!(
        gateway_subscribers_after_first.contains(&peer_a_peer_id),
        "Peer-a should STILL be in gateway's subscriber list (second client still connected). \
         peer_id {} not in {:?}",
        peer_a_peer_id,
        gateway_subscribers_after_first
    );
    tracing::info!("Verified: peer-a still in subscribers after first client disconnect");

    // Step 6: Disconnect second client - peer-a should be REMOVED
    tracing::info!("Step 6: Disconnecting second client (pruning SHOULD occur)");
    drop(client_a2);

    tokio::time::sleep(Duration::from_secs(10)).await;

    make_node_diagnostics(&mut client_gw, gw_diag_config).await?;

    let gateway_subscribers_final = loop {
        let resp = timeout(Duration::from_secs(10), client_gw.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let contract_state = diag.contract_states.get(&contract_key);
                let subscribers = contract_state
                    .map(|cs| cs.subscriber_peer_ids.clone())
                    .unwrap_or_default();
                tracing::info!(
                    "Gateway subscribers (after second client disconnect): {:?}",
                    subscribers
                );
                break subscribers;
            }
            other => {
                tracing::warn!("Gateway: unexpected response: {:?}", other);
            }
        }
    };

    assert!(
        !gateway_subscribers_final.contains(&peer_a_peer_id),
        "Peer-a should NOT be in gateway's subscriber list after both clients disconnect. \
         peer_id {} found in {:?}",
        peer_a_peer_id,
        gateway_subscribers_final
    );

    tracing::info!("Multiple clients prevent premature pruning test passed");
    Ok(())
}

/// Test that verifies the complete subscription tree pruning mechanism (Issue #2166).
///
/// This test validates:
/// 1. Initial state: No subscriptions exist
/// 2. After subscribe: Gateway has Peer-A as downstream, Peer-A has local client subscription
/// 3. After client disconnect: Unsubscribed is sent, Gateway removes Peer-A from subscribers
///
/// The pruning conditions are:
/// - Downstream subscriber disconnects (client WebSocket closes)
/// - No remaining downstream subscribers on Peer-A
/// - No remaining local client subscriptions on Peer-A
/// - → Peer-A sends Unsubscribed to Gateway (upstream)
#[freenet_test(
    nodes = ["gateway", "peer-a"],
    timeout_secs = 180,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_subscription_pruning_sends_unsubscribed(ctx: &mut TestContext) -> TestResult {
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let gateway = ctx.node("gateway")?;
    let peer_a = ctx.node("peer-a")?;

    tracing::info!(
        "Nodes: gateway ws={}, peer-a ws={}",
        gateway.ws_port,
        peer_a.ws_port
    );

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect clients
    let uri_gw = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        gateway.ws_port
    );
    let (stream_gw, _) = connect_async(&uri_gw).await?;
    let mut client_gw = WebApi::start(stream_gw);

    let uri_a = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer_a.ws_port
    );
    let (stream_a, _) = connect_async(&uri_a).await?;
    let mut client_a = WebApi::start(stream_a);

    // ========== STEP 1: Put contract on gateway ==========
    tracing::info!("Step 1: Putting contract on gateway");
    make_put(
        &mut client_gw,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    loop {
        let resp = timeout(Duration::from_secs(30), client_gw.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::PutResponse { key }) => {
                assert_eq!(key, contract_key);
                tracing::info!("Contract put successfully");
                break;
            }
            other => tracing::warn!("Unexpected response: {:?}", other),
        }
    }

    // ========== STEP 2: Verify initial state - no subscribers ==========
    tracing::info!("Step 2: Verifying initial state - gateway has no subscribers");

    let gw_diag_config = NodeDiagnosticsConfig {
        include_node_info: false,
        include_network_info: false,
        include_subscriptions: false,
        contract_keys: vec![contract_key],
        include_system_metrics: false,
        include_detailed_peer_info: false,
        include_subscriber_peer_ids: true,
    };
    make_node_diagnostics(&mut client_gw, gw_diag_config.clone()).await?;

    let initial_subscribers = loop {
        let resp = timeout(Duration::from_secs(10), client_gw.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let subs = diag
                    .contract_states
                    .get(&contract_key)
                    .map(|cs| cs.subscriber_peer_ids.clone())
                    .unwrap_or_default();
                tracing::info!("Initial gateway subscribers: {:?}", subs);
                break subs;
            }
            other => tracing::warn!("Unexpected: {:?}", other),
        }
    };

    assert!(
        initial_subscribers.is_empty(),
        "Gateway should have no subscribers initially"
    );

    // ========== STEP 3: Peer-A subscribes ==========
    tracing::info!("Step 3: Peer-A subscribing to contract");
    make_subscribe(&mut client_a, contract_key).await?;

    loop {
        let resp = timeout(Duration::from_secs(60), client_a.recv()).await??;
        match resp {
            HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
            }) => {
                assert_eq!(key, contract_key);
                assert!(subscribed, "Peer-A failed to subscribe");
                tracing::info!("Peer-A subscribed successfully");
                break;
            }
            other => tracing::warn!("Peer-A unexpected: {:?}", other),
        }
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // ========== STEP 4: Verify subscription state ==========
    tracing::info!("Step 4: Verifying subscription state on both nodes");

    // Get Peer-A's peer_id
    let peer_a_diag_config = NodeDiagnosticsConfig {
        include_node_info: true,
        include_network_info: false,
        include_subscriptions: true, // Check local subscriptions
        contract_keys: vec![],
        include_system_metrics: false,
        include_detailed_peer_info: false,
        include_subscriber_peer_ids: false,
    };
    make_node_diagnostics(&mut client_a, peer_a_diag_config.clone()).await?;

    let (peer_a_peer_id, peer_a_subscriptions) = loop {
        let resp = timeout(Duration::from_secs(10), client_a.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let peer_id = diag
                    .node_info
                    .ok_or_else(|| anyhow!("Missing node_info"))?
                    .peer_id;
                let subs = diag.subscriptions;
                tracing::info!("Peer-A peer_id: {}, subscriptions: {:?}", peer_id, subs);
                break (peer_id, subs);
            }
            other => tracing::warn!("Peer-A unexpected: {:?}", other),
        }
    };

    // Verify Peer-A has local subscription
    assert!(
        peer_a_subscriptions
            .iter()
            .any(|s| s.contract_key == contract_key),
        "Peer-A should have local subscription to contract. Subscriptions: {:?}",
        peer_a_subscriptions
    );
    tracing::info!("✓ Peer-A has local client subscription");

    // Verify Gateway has Peer-A as subscriber
    make_node_diagnostics(&mut client_gw, gw_diag_config.clone()).await?;

    let gateway_subscribers_after_subscribe = loop {
        let resp = timeout(Duration::from_secs(10), client_gw.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let subs = diag
                    .contract_states
                    .get(&contract_key)
                    .map(|cs| cs.subscriber_peer_ids.clone())
                    .unwrap_or_default();
                tracing::info!("Gateway subscribers after subscribe: {:?}", subs);
                break subs;
            }
            other => tracing::warn!("Unexpected: {:?}", other),
        }
    };

    assert!(
        gateway_subscribers_after_subscribe.contains(&peer_a_peer_id),
        "Gateway should have Peer-A as subscriber. peer_id={}, subscribers={:?}",
        peer_a_peer_id,
        gateway_subscribers_after_subscribe
    );
    tracing::info!("✓ Gateway has Peer-A as downstream subscriber");

    // ========== STEP 5: Disconnect Peer-A's client (triggers pruning) ==========
    tracing::info!("Step 5: Disconnecting Peer-A's client - this should trigger pruning");
    tracing::info!("  Expected: Peer-A sends Unsubscribed to Gateway");

    drop(client_a);

    // Wait for disconnect to be processed and Unsubscribed to be sent/received
    tokio::time::sleep(Duration::from_secs(10)).await;

    // ========== STEP 6: Verify pruning occurred ==========
    tracing::info!("Step 6: Verifying pruning - Gateway should have no subscribers");

    make_node_diagnostics(&mut client_gw, gw_diag_config).await?;

    let gateway_subscribers_after_disconnect = loop {
        let resp = timeout(Duration::from_secs(10), client_gw.recv()).await??;
        match resp {
            HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(diag)) => {
                let subs = diag
                    .contract_states
                    .get(&contract_key)
                    .map(|cs| cs.subscriber_peer_ids.clone())
                    .unwrap_or_default();
                tracing::info!("Gateway subscribers after disconnect: {:?}", subs);
                break subs;
            }
            other => tracing::warn!("Unexpected: {:?}", other),
        }
    };

    assert!(
        !gateway_subscribers_after_disconnect.contains(&peer_a_peer_id),
        "Pruning FAILED: Peer-A should NOT be in Gateway's subscribers after disconnect.\n\
         This means Unsubscribed was NOT sent or NOT processed correctly.\n\
         peer_id={}, subscribers={:?}",
        peer_a_peer_id,
        gateway_subscribers_after_disconnect
    );

    tracing::info!("✓ Pruning successful: Gateway removed Peer-A after receiving Unsubscribed");
    tracing::info!("Test PASSED: Subscription tree pruning works correctly");
    Ok(())
}
