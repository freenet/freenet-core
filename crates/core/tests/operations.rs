use anyhow::{anyhow, bail};
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{
        self, load_delegate, make_get, make_put, make_subscribe, make_update,
        verify_contract_exists,
    },
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{random, Rng, SeedableRng};
use serde::Deserialize;
use std::{
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::Duration,
};
use testresult::TestResult;
use tokio::select;
use tokio_tungstenite::connect_async;
use tracing::{level_filters::LevelFilter, span, Instrument, Level};

static RNG: once_cell::sync::Lazy<std::sync::Mutex<rand::rngs::StdRng>> =
    once_cell::sync::Lazy::new(|| {
        std::sync::Mutex::new(rand::rngs::StdRng::from_seed(
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
    let key = TransportKeypair::new_with_rng(&mut *RNG.lock().unwrap());
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;
    key.public().save(temp_dir.path().join("public.pem"))?;
    let config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_api_port),
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port,
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(gateways),
            location: Some(RNG.lock().unwrap().gen()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: public_port,
            bandwidth_limit: None,
            blocked_addresses: None,
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
    temp_dir: &tempfile::TempDir,
) -> anyhow::Result<(ContractContainer, WrappedState)> {
    make_get(client, key, true, false).await?;
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: Some(contract),
                state,
            }))) => {
                verify_contract_exists(temp_dir.path(), key).await?;
                return Ok((contract, state));
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
}

#[tokio::test(flavor = "multi_thread")]
async fn test_put_contract() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };
    let ws_api_port_peer_b = config_b.ws_api.ws_api_port.unwrap();

    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_peer_a = config_a.ws_api.ws_api_port.unwrap();

    tracing::info!("Node A data dir: {:?}", preset_cfg_b.temp_dir.path());
    tracing::info!("Node B data dir: {:?}", preset_cfg_a.temp_dir.path());

    std::mem::drop(ws_api_port_socket_a); // Free the port so it does not fail on initialization
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    std::mem::drop(network_socket_b); // Free the port so it does not fail on initialization
    std::mem::drop(ws_api_port_socket_b);
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(120), async {
        // Wait for nodes to start up
        tracing::info!("Waiting for nodes to start up...");
        tokio::time::sleep(Duration::from_secs(15)).await;
        tracing::info!("Nodes should be ready, proceeding with test...");

        // Connect to node A's websocket API
        let uri = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_peer_a
        );
        let (stream, _) = connect_async(&uri).await?;
        let mut client_api_a = WebApi::start(stream);

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
                assert_eq!(key, contract_key);
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

        {
            // Wait for get response from node A
            tracing::info!("getting contract from A");
            let (response_contract, response_state) =
                get_contract(&mut client_api_a, contract_key, &preset_cfg_b.temp_dir).await?;
            let response_key = response_contract.key();

            // Verify the responses
            assert_eq!(response_key, contract_key);
            assert_eq!(response_contract, contract);
            assert_eq!(response_state, wrapped_state);
        }

        {
            // Connect to node B's websocket API
            let uri = format!(
                "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
                ws_api_port_peer_b
            );
            let (stream, _) = connect_async(&uri).await?;
            let mut client_api_b = WebApi::start(stream);

            // Wait for get response from node B
            let (response_contract, response_state) =
                get_contract(&mut client_api_b, contract_key, &preset_cfg_b.temp_dir).await?;
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

        Ok::<_, anyhow::Error>(())
    });

    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!(a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!(b).into());
        }
        r = test => {
            r??;
            // Give time for cleanup before dropping nodes
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_update_contract() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node B
    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port = config_a.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);

    // Start node A (client)
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (gateway)
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(60), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(20)).await; // Increased sleep duration

        // Connect to node A websocket API
        let uri = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port
        );
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
                get_contract(&mut client_api_a, contract_key, &preset_cfg_b.temp_dir).await?;

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

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!("Node A failed: {}", a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!("Node B failed: {}", b).into());
        }
        r = test => {
            r??;
            // Keep nodes alive for pending operations to complete
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_clients_subscription() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);
    tracing::info!("Starting test_multiple_clients_subscription");
    let test_start_time = std::time::Instant::now();

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    tracing::info!("Starting node setup phase");
    let node_setup_start_time = std::time::Instant::now();

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_c = TcpListener::bind("127.0.0.1:0")?; // Socket for node C (second client)

    let gw_public_port = network_socket_b.local_addr()?.port();
    let gw_ws_port = ws_api_port_socket_b.local_addr()?.port();
    // Configure gateway node
    let (config_gw, preset_cfg_b, gw_cfg) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(gw_public_port),
            gw_ws_port,
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };
    tracing::info!("Gateway Node (Node GW) configured. Public port: {}, WS port: {}", gw_public_port, gw_ws_port);

    // Configure client node A
    let client_a_ws_port = ws_api_port_socket_a.local_addr()?.port();
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&gw_cfg)?],
        None,
        client_a_ws_port,
    )
    .await?;
    let ws_api_port_a = config_a.ws_api.ws_api_port.unwrap();
    tracing::info!("Client Node A configured. WS port: {}", ws_api_port_a);

    // Configure client node B (second client node)
    let client_b_ws_port = ws_api_port_socket_c.local_addr()?.port();
    let (config_b, preset_cfg_c) = base_node_test_config(
        false,
        vec![serde_json::to_string(&gw_cfg)?],
        None,
        client_b_ws_port,
    )
    .await?;
    let ws_api_port_b = config_b.ws_api.ws_api_port.unwrap();
    tracing::info!("Client Node B configured. WS port: {}", ws_api_port_b);

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node GW (was B) data dir: {:?}", preset_cfg_b.temp_dir.path());
    tracing::info!("Node B (was C) data dir: {:?}", preset_cfg_c.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);
    std::mem::drop(ws_api_port_socket_c);

    // Start node A (first client)
    tracing::info!("Attempting to start Client Node A");
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start GW node
    tracing::info!("Attempting to start Gateway Node (Node GW)");
    let node_gw = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (second client)
    tracing::info!("Attempting to start Client Node B");
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        tracing::info!("Node setup phase finished. Elapsed time: {:?}", node_setup_start_time.elapsed());
        // Wait for nodes' WebSocket APIs to become available
        tracing::info!("Starting dynamic wait for nodes' WebSocket APIs.");
        let mut node_a_ready = false;
        let mut node_gw_ready = false;
        let mut node_b_ready = false;
        let ws_gw_port = config_gw.ws_api.ws_api_port.unwrap();
        let overall_wait_timeout = std::time::Instant::now();
        let max_wait_duration = Duration::from_secs(90);

        while !node_a_ready || !node_gw_ready || !node_b_ready {
            if overall_wait_timeout.elapsed() > max_wait_duration {
                tracing::error!("Timeout waiting for nodes' WebSocket APIs to become available after {:?}.", overall_wait_timeout.elapsed());
                bail!("Timeout waiting for nodes' WebSocket APIs to become available.");
            }

            if !node_a_ready {
                let uri_a_check = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", ws_api_port_a);
                tracing::info!("Attempting readiness check for Node A at {}", uri_a_check);
                match tokio::time::timeout(Duration::from_secs(2), connect_async(&uri_a_check)).await {
                    Ok(Ok((stream, _))) => {
                        tracing::info!("Successfully connected to Node A API for readiness check. Closing test connection.");
                        if let Err(e) = stream.close(None).await {
                            tracing::warn!("Error closing test connection to Node A: {:?}", e);
                        }
                        node_a_ready = true;
                    }
                    Ok(Err(e)) => {
                        tracing::info!("Node A API not yet ready (connect_async error): {}", e);
                    }
                    Err(_) => {
                        tracing::info!("Node A API connection attempt timed out.");
                    }
                }
            }

            if !node_gw_ready {
                let uri_gw_check = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", ws_gw_port);
                tracing::info!("Attempting readiness check for Gateway Node (Node GW) at {}", uri_gw_check);
                match tokio::time::timeout(Duration::from_secs(2), connect_async(&uri_gw_check)).await {
                    Ok(Ok((stream, _))) => {
                        tracing::info!("Successfully connected to Gateway Node API for readiness check. Closing test connection.");
                        if let Err(e) = stream.close(None).await {
                            tracing::warn!("Error closing test connection to Gateway Node: {:?}", e);
                        }
                        node_gw_ready = true;
                    }
                    Ok(Err(e)) => {
                        tracing::info!("Gateway Node API not yet ready (connect_async error): {}", e);
                    }
                    Err(_) => {
                        tracing::info!("Gateway Node API connection attempt timed out.");
                    }
                }
            }

            if !node_b_ready {
                let uri_b_check = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", ws_api_port_b);
                tracing::info!("Attempting readiness check for Node B at {}", uri_b_check);
                match tokio::time::timeout(Duration::from_secs(2), connect_async(&uri_b_check)).await {
                    Ok(Ok((stream, _))) => {
                        tracing::info!("Successfully connected to Node B API for readiness check. Closing test connection.");
                        if let Err(e) = stream.close(None).await {
                            tracing::warn!("Error closing test connection to Node B: {:?}", e);
                        }
                        node_b_ready = true;
                    }
                    Ok(Err(e)) => {
                        tracing::info!("Node B API not yet ready (connect_async error): {}", e);
                    }
                    Err(_) => {
                        tracing::info!("Node B API connection attempt timed out.");
                    }
                }
            }

            if !node_a_ready || !node_gw_ready || !node_b_ready {
                tracing::info!("Not all nodes ready. Node A: {}, Gateway Node: {}, Node B: {}. Waiting 1s before retry. Total elapsed: {:?}", node_a_ready, node_gw_ready, node_b_ready, overall_wait_timeout.elapsed());
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        tracing::info!("All nodes' WebSocket APIs are available. Proceeding with client connections. Total time for readiness check: {:?}", overall_wait_timeout.elapsed());

        // Connect first client to node A's websocket API
        let uri_a = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_a
        );
        tracing::info!("Client 1 (on Node A): Attempting to connect to {}", uri_a);
        let (stream1, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(&uri_a))
            .await
            .map_err(|_e| {
                tracing::error!("Client 1 (on Node A): Timeout connecting to {uri_a}");
                anyhow!("Client 1 (on Node A): Timeout connecting to {uri_a}")
            })?
            .map_err(|e| {
                tracing::error!("Client 1 (on Node A): Failed to connect to {uri_a}: {e}");
                anyhow!("Client 1 (on Node A): Failed to connect to {uri_a}: {e}")
            })?;
        tracing::info!("Client 1 (on Node A): Successfully connected to {}", uri_a);
        let mut client_api1_node_a = WebApi::start(stream1);

        // Connect second client to node A's websocket API
        tracing::info!("Client 2 (on Node A): Attempting to connect to {}", uri_a);
        let (stream2, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(&uri_a))
            .await
            .map_err(|_e| {
                tracing::error!("Client 2 (on Node A): Timeout connecting to {uri_a}");
                anyhow!("Client 2 (on Node A): Timeout connecting to {uri_a}")
            })?
            .map_err(|e| {
                tracing::error!("Client 2 (on Node A): Failed to connect to {uri_a}: {e}");
                anyhow!("Client 2 (on Node A): Failed to connect to {uri_a}: {e}")
            })?;
        tracing::info!("Client 2 (on Node A): Successfully connected to {}", uri_a);
        let mut client_api2_node_a = WebApi::start(stream2);

        // Connect third client to node C's websocket API (different node)
        let uri_c = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_b // This is ws_api_port_b, which corresponds to client Node B
        );
        tracing::info!("Client 3 (on Node B): Attempting to connect to {}", uri_c);
        let (stream3, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(&uri_c))
            .await
            .map_err(|_e| {
                tracing::error!("Client 3 (on Node B): Timeout connecting to {uri_c}");
                anyhow!("Client 3 (on Node B): Timeout connecting to {uri_c}")
            })?
            .map_err(|e| {
                tracing::error!("Client 3 (on Node B): Failed to connect to {uri_c}: {e}");
                anyhow!("Client 3 (on Node B): Failed to connect to {uri_c}: {e}")
            })?;
        tracing::info!("Client 3 (on Node B): Successfully connected to {}", uri_c);
        let mut client_api_node_b = WebApi::start(stream3);

        tracing::info!("Starting PUT operation phase");
        // First client puts contract with initial state (without subscribing)
        tracing::info!("Client 1 (on Node A): Attempting to PUT contract {contract_key}");
        make_put(
            &mut client_api1_node_a,
            wrapped_state.clone(),
            contract.clone(),
            false, // subscribe=false - no automatic subscription
        )
        .await?;

        // Wait for put response
        loop {
            let resp =
                tokio::time::timeout(Duration::from_secs(60), client_api1_node_a.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                    assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
                    tracing::info!("Client 1 (on Node A): Received PUT response for contract {key}");
                    break;
                }
                Ok(Ok(other)) => {
                    tracing::warn!("Client 1 (on Node A): Unexpected response while waiting for PUT: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 1 (on Node A): Error receiving PUT response: {}", e);
                    bail!("Error receiving put response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 1 (on Node A): Timeout waiting for PUT response for contract {contract_key}");
                    bail!("Timeout waiting for put response");
                }
            }
        }
        tracing::info!("PUT operation phase finished");

        tracing::info!("Starting SUBSCRIBE operation phase");
        // Explicitly subscribe client 1 to the contract using make_subscribe
        tracing::info!("Client 1 (on Node A): Attempting to SUBSCRIBE to contract {contract_key}");
        make_subscribe(&mut client_api1_node_a, contract_key).await?;

        // Wait for subscribe response
        loop {
            let resp =
                tokio::time::timeout(Duration::from_secs(30), client_api1_node_a.recv()).await;
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
                    tracing::info!("Client 1 (on Node A): Received SUBSCRIBE response for contract {key}. Subscribed: {subscribed}");
                    break;
                }
                Ok(Ok(other)) => {
                    tracing::warn!(
                        "Client 1 (on Node A): Unexpected response while waiting for SUBSCRIBE: {:?}",
                        other
                    );
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 1 (on Node A): Error receiving SUBSCRIBE response: {}", e);
                    bail!("Client 1: Error receiving subscribe response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 1 (on Node A): Timeout waiting for SUBSCRIBE response for contract {contract_key}");
                    bail!("Client 1: Timeout waiting for subscribe response");
                }
            }
        }

        // Second client gets the contract (without subscribing)
        tracing::info!("Client 2 (on Node A): Attempting to GET contract {contract_key}");
        make_get(&mut client_api2_node_a, contract_key, true, false).await?;

        // Wait for get response on second client
        loop {
            let resp =
                tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key,
                    contract: Some(_),
                    state: _,
                }))) => {
                    assert_eq!(key, contract_key, "Contract key mismatch in GET response");
                    tracing::info!("Client 2 (on Node A): Received GET response for contract {key}");
                    break;
                }
                Ok(Ok(other)) => {
                    tracing::warn!("Client 2 (on Node A): Unexpected response while waiting for GET: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 2 (on Node A): Error receiving GET response: {}", e);
                    bail!("Error receiving get response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 2 (on Node A): Timeout waiting for GET response for contract {contract_key}");
                    bail!("Timeout waiting for get response");
                }
            }
        }

        // Explicitly subscribe client 2 to the contract using make_subscribe
        tracing::info!("Client 2 (on Node A): Attempting to SUBSCRIBE to contract {contract_key}");
        make_subscribe(&mut client_api2_node_a, contract_key).await?;

        // Wait for subscribe response
        loop {
            let resp =
                tokio::time::timeout(Duration::from_secs(30), client_api2_node_a.recv()).await;
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
                    tracing::info!("Client 2 (on Node A): Received SUBSCRIBE response for contract {key}. Subscribed: {subscribed}");
                    break;
                }
                Ok(Ok(other)) => {
                    tracing::warn!(
                        "Client 2 (on Node A): Unexpected response while waiting for SUBSCRIBE: {:?}",
                        other
                    );
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 2 (on Node A): Error receiving SUBSCRIBE response: {}", e);
                    bail!("Client 2: Error receiving subscribe response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 2 (on Node A): Timeout waiting for SUBSCRIBE response for contract {contract_key}");
                    bail!("Client 2: Timeout waiting for subscribe response");
                }
            }
        }

        // Third client gets the contract from node C (without subscribing)
        tracing::info!("Client 3 (on Node B): Attempting to GET contract {contract_key}");
        make_get(&mut client_api_node_b, contract_key, true, false).await?;

        // Wait for get response on third client
        loop {
            let resp =
                tokio::time::timeout(Duration::from_secs(30), client_api_node_b.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key,
                    contract: Some(_),
                    state: _,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in GET response for client 3"
                    );
                    tracing::info!("Client 3 (on Node B): Received GET response for contract {key}");
                    break;
                }
                Ok(Ok(other)) => {
                    tracing::warn!(
                        "Client 3 (on Node B): Unexpected response while waiting for GET: {:?}",
                        other
                    );
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 3 (on Node B): Error receiving GET response: {}", e);
                    bail!("Client 3: Error receiving get response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 3 (on Node B): Timeout waiting for GET response for contract {contract_key}");
                    bail!("Client 3: Timeout waiting for get response");
                }
            }
        }

        // Explicitly subscribe client 3 to the contract using make_subscribe
        tracing::info!("Client 3 (on Node B): Attempting to SUBSCRIBE to contract {contract_key}");
        make_subscribe(&mut client_api_node_b, contract_key).await?;

        // Wait for subscribe response
        loop {
            let resp =
                tokio::time::timeout(Duration::from_secs(30), client_api_node_b.recv()).await;
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
                    tracing::info!("Client 3 (on Node B): Received SUBSCRIBE response for contract {key}. Subscribed: {subscribed}");
                    break;
                }
                Ok(Ok(other)) => {
                    tracing::warn!(
                        "Client 3 (on Node B): Unexpected response while waiting for SUBSCRIBE: {:?}",
                        other
                    );
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 3 (on Node B): Error receiving SUBSCRIBE response: {}", e);
                    bail!("Client 3: Error receiving subscribe response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 3 (on Node B): Timeout waiting for SUBSCRIBE response for contract {contract_key}");
                    bail!("Client 3: Timeout waiting for subscribe response");
                }
            }
        }
        tracing::info!("SUBSCRIBE operation phase finished");

        tracing::info!("Preparing for UPDATE operation");
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

        tracing::info!("Starting UPDATE operation phase");
        // First client updates the contract
        tracing::info!("Client 1 (on Node A): Attempting to UPDATE contract {contract_key}");
        make_update(&mut client_api1_node_a, contract_key, updated_state.clone()).await?;

        // Wait for update response and notifications on all clients
        tracing::info!("Starting notification check phase");
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

        let expected_task = test_utils::Task {
            id: 1,
            title: "Test multiple clients".to_string(),
            description: "Verify that update notifications are received by multiple clients"
                .to_string(),
            completed: false,
            priority: 5,
        };

        let notification_loop_start_time = std::time::Instant::now();
        tracing::info!("Main event loop for notifications started. Waiting up to 90s. Started at: {:?}", notification_loop_start_time);
        while notification_loop_start_time.elapsed() < Duration::from_secs(90)
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
                        tracing::info!("Client 1 (on Node A): Received UPDATE response for contract {key}");
                        received_update_response = true;
                    }
                    Ok(Ok(HostResponse::ContractResponse(
                        ContractResponse::UpdateNotification { key, update },
                    ))) => {
                        assert_eq!(
                            key, contract_key,
                            "Contract key mismatch in UPDATE notification for client 1"
                        );
                        tracing::info!("Client 1 (on Node A): Received UpdateNotification for contract {key}");

                        // Verify update content
                        match update {
                            UpdateData::State(state) => {
                                let received_todo_list: test_utils::TodoList =
                                    serde_json::from_slice(state.as_ref()).expect(
                                        "Failed to deserialize state from update notification",
                                    );

                                assert_eq!(
                                    received_todo_list.tasks.len(),
                                    1,
                                    "Should have one task"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].id, expected_task.id,
                                    "Task ID should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].title, expected_task.title,
                                    "Task title should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].description,
                                    expected_task.description,
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

                                tracing::info!("Client 1 (on Node A): Successfully verified update content for contract {key}");
                            }
                            _ => {
                                tracing::warn!(
                                    "Client 1 (on Node A): Received unexpected update type: {:?}",
                                    update
                                );
                            }
                        }
                        client1_received_notification = true;
                    }
                    Ok(Ok(other)) => {
                        tracing::debug!("Client 1 (on Node A): Received unexpected response while waiting for notification: {:?}", other);
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Client 1 (on Node A): Error receiving response while waiting for notification: {}", e);
                    }
                    Err(_) => {
                        tracing::info!("Client 1 (on Node A): Timeout waiting for message after 1s. Total elapsed in loop: {:?}", notification_loop_start_time.elapsed());
                        // Timeout is expected, just continue
                    }
                }
            }

            // Check for notification on client 2
            if !client2_received_notification {
                let resp =
                    tokio::time::timeout(Duration::from_secs(1), client_api2_node_a.recv()).await;
                match resp {
                    Ok(Ok(HostResponse::ContractResponse(
                        ContractResponse::UpdateNotification { key, update },
                    ))) => {
                        assert_eq!(
                            key, contract_key,
                            "Contract key mismatch in UPDATE notification for client 2"
                        );
                        tracing::info!("Client 2 (on Node A): Received UpdateNotification for contract {key}");

                        // Verify update content
                        match update {
                            UpdateData::State(state) => {
                                let received_todo_list: test_utils::TodoList =
                                    serde_json::from_slice(state.as_ref()).expect(
                                        "Failed to deserialize state from update notification",
                                    );

                                assert_eq!(
                                    received_todo_list.tasks.len(),
                                    1,
                                    "Should have one task"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].id, expected_task.id,
                                    "Task ID should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].title, expected_task.title,
                                    "Task title should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].description,
                                    expected_task.description,
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

                                tracing::info!("Client 2 (on Node A): Successfully verified update content for contract {key}");
                            }
                            _ => {
                                tracing::warn!(
                                    "Client 2 (on Node A): Received unexpected update type: {:?}",
                                    update
                                );
                            }
                        }
                        client2_received_notification = true;
                    }
                    Ok(Ok(other)) => {
                        tracing::debug!("Client 2 (on Node A): Received unexpected response while waiting for notification: {:?}", other);
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Client 2 (on Node A): Error receiving response while waiting for notification: {}", e);
                    }
                    Err(_) => {
                        tracing::info!("Client 2 (on Node A): Timeout waiting for message after 1s. Total elapsed in loop: {:?}", notification_loop_start_time.elapsed());
                        // Timeout is expected, just continue
                    }
                }
            }

            // Check for notification on client 3 (on different node)
            if !client_node_b_received_notification {
                let resp =
                    tokio::time::timeout(Duration::from_secs(1), client_api_node_b.recv()).await;
                match resp {
                    Ok(Ok(HostResponse::ContractResponse(
                        ContractResponse::UpdateNotification { key, update },
                    ))) => {
                        assert_eq!(
                            key, contract_key,
                            "Contract key mismatch in UPDATE notification for client 3"
                        );
                        tracing::info!("Client 3 (on Node B): Received UpdateNotification for contract {key}");

                        // Verify update content
                        match update {
                            UpdateData::State(state) => {
                                let received_todo_list: test_utils::TodoList =
                                    serde_json::from_slice(state.as_ref()).expect(
                                        "Failed to deserialize state from update notification",
                                    );

                                assert_eq!(
                                    received_todo_list.tasks.len(),
                                    1,
                                    "Should have one task"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].id, expected_task.id,
                                    "Task ID should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].title, expected_task.title,
                                    "Task title should match"
                                );
                                assert_eq!(
                                    received_todo_list.tasks[0].description,
                                    expected_task.description,
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
                                    "Client 3 (on Node B): Successfully verified update content (cross-node) for contract {key}"
                                );
                            }
                            _ => {
                                tracing::warn!(
                                    "Client 3 (on Node B): Received unexpected update type: {:?}",
                                    update
                                );
                            }
                        }
                        client_node_b_received_notification = true;
                    }
                    Ok(Ok(other)) => {
                        tracing::debug!("Client 3 (on Node B): Received unexpected response while waiting for notification: {:?}", other);
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Client 3 (on Node B): Error receiving response while waiting for notification: {}", e);
                    }
                    Err(_) => {
                        tracing::info!("Client 3 (on Node B): Timeout waiting for message after 1s. Total elapsed in loop: {:?}", notification_loop_start_time.elapsed());
                        // Timeout is expected, just continue
                    }
                }
            }

            // Small delay before trying again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tracing::info!("Notification check phase finished. Elapsed time: {:?}", notification_loop_start_time.elapsed());

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
        tracing::info!("Client 1 (on Node A): Attempting to disconnect");
        client_api1_node_a
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tracing::info!("Client 1 (on Node A): Disconnected");

        tracing::info!("Client 2 (on Node A): Attempting to disconnect");
        client_api2_node_a
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tracing::info!("Client 2 (on Node A): Disconnected");

        tracing::info!("Client 3 (on Node B): Attempting to disconnect");
        client_api_node_b
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tracing::info!("Client 3 (on Node B): Disconnected");

        tokio::time::sleep(Duration::from_millis(200)).await;
        tracing::info!("Finished test_multiple_clients_subscription. Total elapsed time: {:?}", test_start_time.elapsed());
        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        a = node_a => {
            let Err(a) = a;
            tracing::error!("Client Node A failed: {}", a);
            return Err(anyhow!("Node A failed: {}", a).into());
        }
        b = node_gw => {
            let Err(b) = b;
            tracing::error!("Gateway Node (Node GW) failed: {}", b);
            return Err(anyhow!("Node B failed: {}", b).into());
        }
        c = node_b => {
            let Err(c) = c;
            tracing::error!("Client Node B failed: {}", c);
            return Err(anyhow!("Node C failed: {}", c).into());
        }
        r = test => {
            match r {
                Ok(Ok(_)) => tracing::info!("Test logic completed successfully."),
                Ok(Err(e)) => {
                    tracing::error!("Test logic failed: {:?}", e);
                    return Err(e.into());
                }
                Err(e) => {
                    tracing::error!("Test timed out: {:?}", e);
                    return Err(anyhow!("Test timed out: {:?}", e).into());
                }
            }
            // Give time for cleanup before dropping nodes
            tracing::info!("Test execution finished, sleeping for 3s for cleanup.");
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

// FIXME Update notification is not received
#[ignore = "Update notification is not received"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_with_subscribe_flag() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node B
    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_a = config_a.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);

    // Start node A (client)
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (gateway)
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(60), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect first client to node A's websocket API (for putting the contract)
        let uri_a = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_a
        );
        let (stream1, _) = connect_async(&uri_a).await?;
        let mut client_api1_node_a = WebApi::start(stream1);

        // Connect second client to node A's websocket API (for getting with auto-subscribe)
        let (stream2, _) = connect_async(&uri_a).await?;
        let mut client_api2_node_a = WebApi::start(stream2);

        tracing::info!("Client 1: Put contract with initial state");


        // First client puts contract with initial state (without subscribing)
        make_put(
            &mut client_api1_node_a,
            wrapped_state.clone(),
            contract.clone(),
            false, // subscribe=false
        )
        .await?;


        // Wait for put response
        let resp = tokio::time::timeout(Duration::from_secs(30), client_api1_node_a.recv()).await;
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
                            tracing::warn!(
                                "Client 1: Received unexpected update type: {:?}",
                                update
                            );
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

        // Assert that client 1 received the notification (proving auto-subscribe worked)
        assert!(
            client2_node_a_received_notification,
            "Client 2 did not receive update notification within timeout period (auto-subscribe via GET failed)"
        );

        Ok::<_, anyhow::Error>(())
    }).instrument(span!(Level::INFO, "test_get_with_subscribe_flag"));

    // Wait for test completion or node failures
    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!("Node A failed: {}", a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!("Node B failed: {}", b).into());
        }
        r = test => {
            r??;
            // Keep nodes alive for pending operations to complete
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

// FIXME Update notification is not received
#[ignore = "Update notification is not received"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_put_with_subscribe_flag() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state with empty todo list
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Create network sockets
    let network_socket_b = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_b = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node B
    let (config_b, preset_cfg_b, config_b_gw) = {
        let (cfg, preset) = base_node_test_config(
            true,
            vec![],
            Some(network_socket_b.local_addr()?.port()),
            ws_api_port_socket_b.local_addr()?.port(),
        )
        .await?;
        let public_port = cfg.network_api.public_port.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&config_b_gw)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_a = config_a.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!("Node A data dir: {:?}", preset_cfg_a.temp_dir.path());
    tracing::info!("Node B (gw) data dir: {:?}", preset_cfg_b.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_b);
    std::mem::drop(ws_api_port_socket_b);

    // Start node A (client)
    let node_a = async move {
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start node B (gateway)
    let node_b = async {
        let config = config_b.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(60), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect first client to node A's websocket API (for putting with auto-subscribe)
        let uri_a = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_a
        );
        let (stream1, _) = connect_async(&uri_a).await?;
        let mut client_api1 = WebApi::start(stream1);

        // Connect second client to node A's websocket API (for updating the contract)
        let (stream2, _) = connect_async(&uri_a).await?;
        let mut client_api2 = WebApi::start(stream2);

        // First client puts contract with initial state and auto-subscribes
        make_put(
            &mut client_api1,
            wrapped_state.clone(),
            contract.clone(),
            true, // subscribe=true for auto-subscribe
        )
        .await?;

        // Wait for put response
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api1.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                    assert_eq!(key, contract_key, "Contract key mismatch in PUT response");
                    break;
                }
                Ok(Ok(other)) => {
                    bail!("Contract key mismatch in PUT response: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 1: Error receiving put response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 1: Error receiving put response");
                }
            }
        }

        // Second client gets the contract (without subscribing)
        make_get(&mut client_api2, contract_key, true, false).await?;

        // Wait for get response on second client
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api2.recv()).await;
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
                    bail!("unexpected response while waiting for get: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 2: Error receiving get response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 2: Error receiving get response");
                }
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
            title: "Test auto-subscribe with PUT".to_string(),
            description: "Verify that auto-subscribe works with PUT operation".to_string(),
            completed: false,
            priority: 5,
        });

        // Serialize the updated list back to bytes
        let updated_bytes = serde_json::to_vec(&todo_list).unwrap();
        let updated_state = WrappedState::from(updated_bytes);

        // Second client updates the contract
        tracing::info!("Client 2: Updating contract to trigger notification");
        make_update(&mut client_api2, contract_key, updated_state.clone()).await?;

        // Wait for update response
        loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api2.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                    key,
                    summary: _,
                }))) => {
                    assert_eq!(
                        key, contract_key,
                        "Contract key mismatch in UPDATE response"
                    );
                    break;
                }
                Ok(Ok(other)) => {
                    bail!("unexpected response while waiting for update: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 2: Error receiving update response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 2: Error receiving update response");
                }
            }
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

        // Try for up to 30 seconds to receive the notification
        let start_time = std::time::Instant::now();
        while start_time.elapsed() < Duration::from_secs(30) && !client1_received_notification {
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

                            tracing::info!("Client 1: Successfully verified update content");
                        }
                        _ => {
                            tracing::warn!(
                                "Client 1: Received unexpected update type: {:?}",
                                update
                            );
                        }
                    }
                    client1_received_notification = true;
                    break;
                }
                Ok(Ok(other)) => {
                    bail!("unexpected response while waiting for update: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::error!("Client 2: Error receiving update response: {}", e);
                }
                Err(_) => {
                    tracing::error!("Client 2: Error receiving update response");
                }
            }

            // Small delay before trying again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Assert that client 1 received the notification (proving auto-subscribe worked)
        assert!(
            client1_received_notification,
            "Client 1 did not receive update notification within timeout period (auto-subscribe via PUT failed)"
        );

        Ok::<_, anyhow::Error>(())
    });

    // Wait for test completion or node failures
    select! {
        a = node_a => {
            let Err(a) = a;
            return Err(anyhow!("Node A failed: {}", a).into());
        }
        b = node_b => {
            let Err(b) = b;
            return Err(anyhow!("Node B failed: {}", b).into());
        }
        r = test => {
            r??;
            // Keep nodes alive for pending operations to complete
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_delegate_request() -> TestResult {
    freenet::config::set_logger(Some(LevelFilter::INFO), None);
    const TEST_DELEGATE: &str = "test-delegate-integration";

    // Configure environment variables for optimized release build
    std::env::set_var("CARGO_PROFILE_RELEASE_LTO", "true");
    std::env::set_var("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");
    std::env::set_var("CARGO_PROFILE_RELEASE_STRIP", "true");

    // Load delegate (moving this outside the async block)
    let params = Parameters::from(vec![]);
    let delegate = load_delegate(TEST_DELEGATE, params.clone())?;
    let delegate_key = delegate.key().clone();

    // Create sockets for ports
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_client = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_gw = TcpListener::bind("127.0.0.1:0")?;

    // Configure gateway node
    let (config_gw, preset_cfg_gw, gw_cfg) = {
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
        vec![serde_json::to_string(&gw_cfg)?],
        None,
        ws_api_port_socket_client.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_client = config_client.ws_api.ws_api_port.unwrap();

    // Log data directories for debugging
    tracing::info!(
        "Client node data dir: {:?}",
        preset_cfg_client.temp_dir.path()
    );
    tracing::info!("Gateway node data dir: {:?}", preset_cfg_gw.temp_dir.path());

    // Free ports so they don't fail on initialization
    std::mem::drop(ws_api_port_socket_client);
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);

    // Start gateway node
    let node_gw = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start client node
    let node_client = async move {
        let config = config_client.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Wait for the nodes to start and run the test
    let test = tokio::time::timeout(Duration::from_secs(60), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(20)).await;

        // Connect to the client node's WebSocket API
        let uri = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            ws_api_port_client
        );
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
                println!("Successfully registered delegate with key: {}", key);
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
                            format!("Processed: {}", request_data),
                            "Response text doesn't match expected format"
                        );
                        assert_eq!(
                            data,
                            vec![4, 5, 6],
                            "Response data doesn't match expected value"
                        );

                        println!("Successfully received and verified delegate response");
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
            // Keep nodes alive for pending operations to complete
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    Ok(())
}
