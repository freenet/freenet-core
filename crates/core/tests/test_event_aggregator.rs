//! Integration test demonstrating event log aggregation across multiple nodes.
//!
//! This test shows how to use the EventLogAggregator to correlate transactions
//! across multiple nodes for debugging purposes.
//!
//! # Architecture
//!
//! Each node in this test:
//! - Has its own EventRegister instance
//! - Writes to its own separated AOF file (just like production)
//! - Works independently
//!
//! The aggregator reads these separated AOF files after the test completes
//! and correlates events across nodes.

use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{
        load_contract, make_put, with_peer_id, TestAggregatorBuilder, TestLogger,
    },
    tracing::EventLogAggregator,
};
use freenet_stdlib::{
    client_api::{ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use rand::{Rng, SeedableRng};
use std::{
    net::{Ipv4Addr, TcpListener},
    sync::{LazyLock, Mutex},
    time::Duration,
};
use tokio::select;
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
            bandwidth_limit: None,
            blocked_addresses: None,
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir.path().to_path_buf()),
            data_dir: Some(temp_dir.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    Ok((config, PresetConfig { temp_dir }))
}

fn gw_config(
    public_port: u16,
    path: &std::path::Path,
    location: f64,
) -> anyhow::Result<InlineGwConfig> {
    let config = InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, public_port).into(),
        location: Some(location),
        public_key_path: path.join("public.pem"),
    };
    Ok(config)
}

/// Example integration test showing event log aggregation.
///
/// This test demonstrates the AOF-based aggregation mode (Mode 1 - recommended).
///
/// Architecture:
/// - Each node has its own EventRegister writing to separated AOF files
/// - This matches production behavior exactly
/// - After test completes, we read all AOF files and aggregate them
///
/// Steps:
/// 1. Start multiple nodes (gateway + 1 client node)
/// 2. Each node writes to its own separated AOF file
/// 3. Perform a PUT operation
/// 4. After test completes, collect event logs from all nodes
/// 5. Use EventLogAggregator to analyze the transaction flow across nodes
#[test_log::test(tokio::test(flavor = "multi_thread"))]
#[ignore] // Run with --ignored flag to execute
async fn test_put_operation_with_event_aggregation() -> anyhow::Result<()> {
    // Initialize test logger
    let _logger = TestLogger::new()
        .with_json()
        .with_level("freenet=debug,info")
        .init();

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();

    // Create initial state
    let wrapped_state = WrappedState::from(vec![]);

    // Create network sockets
    let network_socket_gw = TcpListener::bind("127.0.0.1:0")?;
    let ws_api_port_socket_a = TcpListener::bind("127.0.0.1:0")?;
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
        let location = cfg.network_api.location.unwrap();
        let path = preset.temp_dir.path().to_path_buf();
        (cfg, preset, gw_config(public_port, &path, location)?)
    };

    // Configure client node A
    let (config_a, preset_cfg_a) = base_node_test_config(
        false,
        vec![serde_json::to_string(&gw_cfg)?],
        None,
        ws_api_port_socket_a.local_addr()?.port(),
    )
    .await?;
    let ws_api_port_a = config_a.ws_api.ws_api_port.unwrap();

    // Store config directories for later aggregation
    let gateway_config_dir = preset_cfg_gw.temp_dir.path().to_path_buf();
    let node_a_config_dir = preset_cfg_a.temp_dir.path().to_path_buf();

    // Free ports
    std::mem::drop(ws_api_port_socket_a);
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);

    // Start gateway node
    let node_gw = async {
        let _span = with_peer_id("gateway");
        tracing::info!("Starting gateway node");
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        tracing::info!("Gateway node running");
        node.run().await
    }
    .boxed_local();

    // Start node A (client)
    let node_a = async move {
        let _span = with_peer_id("node-a");
        tracing::info!("Starting node A");
        let config = config_a.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await)
            .await?;
        tracing::info!("Node A running");
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(180), async {
        // Wait for nodes to start up
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Connect client to node A's websocket API
        tracing::info!("Connecting to node A WebSocket API");
        let uri_a = format!("ws://127.0.0.1:{ws_api_port_a}/v1/contract/command?encodingProtocol=native");
        let (stream1, _) = connect_async(&uri_a).await?;
        let mut client_api = WebApi::start(stream1);

        // Perform PUT operation
        tracing::info!("Performing PUT operation");
        make_put(&mut client_api, wrapped_state.clone(), contract.clone(), false).await?;

        // Wait for put response
        let _put_tx = loop {
            let resp = tokio::time::timeout(Duration::from_secs(30), client_api.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                    assert_eq!(key, contract_key);
                    tracing::info!("PUT completed successfully");
                    break; // We don't have the tx ID easily accessible here
                }
                Ok(Ok(other)) => {
                    tracing::warn!("Unexpected response: {:?}", other);
                }
                Ok(Err(e)) => {
                    anyhow::bail!("Error receiving put response: {}", e);
                }
                Err(_) => {
                    anyhow::bail!("Timeout waiting for put response");
                }
            }
        };

        // Wait a bit for events to be written
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!("Test operations completed");
        Ok::<_, anyhow::Error>(())
    })
    .boxed_local();

    select! {
        res = node_gw => {
            anyhow::bail!("Gateway node exited unexpectedly: {:?}", res);
        }
        res = node_a => {
            anyhow::bail!("Node A exited unexpectedly: {:?}", res);
        }
        res = test => {
            res??;
        }
    }

    // Now the nodes are done and event logs are written
    // Let's aggregate and analyze the events
    tracing::info!("=== Aggregating Event Logs ===");

    let aggregator = TestAggregatorBuilder::new()
        .add_node("gateway", gateway_config_dir.join("_EVENT_LOG_LOCAL"))
        .add_node("node-a", node_a_config_dir.join("_EVENT_LOG_LOCAL"))
        .build()
        .await?;

    tracing::info!("Event aggregator created successfully");

    // Get all events
    let all_events = aggregator.get_all_events().await?;
    tracing::info!("Total events collected: {}", all_events.len());

    // Print summary of events
    for event in all_events.iter().take(10) {
        tracing::info!(
            "Event: tx={:?}, peer={:?}, kind={:?}, time={}",
            event.tx,
            event.peer_id,
            event.kind,
            event.datetime
        );
    }

    // The test demonstrates that we can:
    // 1. Collect events from multiple nodes
    // 2. Aggregate them in a single view
    // 3. Query and analyze cross-node transaction flows

    tracing::info!("=== Event Aggregation Test Completed Successfully ===");

    Ok(())
}
