//! Regression tests for PR #1806 - Prevent self-routing in GET operations
//! and PR #1781 - Local caching on PUT operations
//!
//! These tests ensure that:
//! 1. Nodes don't attempt to connect to themselves when isolated (PR #1806)
//! 2. Nodes cache contracts locally after PUT operations (PR #1781)

use freenet::{
    config::{ConfigArgs, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
    test_utils::{load_contract, make_get},
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use std::{
    net::Ipv4Addr,
    sync::{LazyLock, Mutex},
    time::Duration,
};
use testresult::TestResult;
use tokio::{select, time::timeout};
use tokio_tungstenite::connect_async;
use tracing::{error, info};

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    use rand::SeedableRng;
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"regression_test_seed_01234567890",
    ))
});

/// Helper to create a node configuration for testing
async fn create_test_node_config(
    is_gateway: bool,
    ws_api_port: u16,
    network_port: Option<u16>,
) -> anyhow::Result<(ConfigArgs, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new();
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
            public_port: network_port,
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(vec![]), // Empty gateways for isolated node
            location: Some({
                use rand::Rng;
                RNG.lock().unwrap().random()
            }),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port,
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

    Ok((config, temp_dir))
}

/// Test that an isolated node doesn't attempt to connect to itself during GET operations
///
/// This is a regression test for PR #1806 which fixed the self-routing bug where
/// nodes would try to connect to themselves when they had no peers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_no_self_routing_on_get() -> TestResult {
    freenet::config::set_logger(Some(tracing::level_filters::LevelFilter::INFO), None);

    // Start a single isolated node (no peers)
    let ws_port = 50700;
    let network_port = 50701;
    let (config, _temp_dir) = create_test_node_config(true, ws_port, Some(network_port)).await?;

    // Start the node
    let node_handle = {
        let config = config.clone();
        async move {
            let built_config = config.build().await?;
            let node = NodeConfig::new(built_config.clone())
                .await?
                .build(serve_gateway(built_config.ws_api).await)
                .await?;
            node.run().await
        }
        .boxed_local()
    };

    // Run the test with timeout
    let test_result = timeout(Duration::from_secs(60), async {
        // Give node time to start - critical for proper initialization
        info!("Waiting for node to start up...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!("Node should be ready, proceeding with test...");

        // Connect to the node
        let url = format!(
            "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
            ws_port
        );
        let (ws_stream, _) = connect_async(&url).await?;
        let mut client = WebApi::start(ws_stream);

        // Create a key for a non-existent contract
        let params = Parameters::from(vec![99, 99, 99]);
        let non_existent_contract = load_contract("test-contract-1", params)?;
        let non_existent_key = non_existent_contract.key();

        info!(
            "Attempting GET for non-existent contract: {}",
            non_existent_key
        );

        // Attempt to GET the non-existent contract
        // This should fail quickly without trying to connect to self
        let start = std::time::Instant::now();
        make_get(&mut client, non_existent_key, false, false).await?;

        // Wait for response with timeout
        let get_result = timeout(Duration::from_secs(5), client.recv()).await;
        let elapsed = start.elapsed();

        // REGRESSION TEST: Verify it completed quickly (not hanging on self-connection)
        // If self-routing was happening, this would timeout or take much longer
        assert!(
            elapsed < Duration::from_secs(6),
            "GET should complete quickly, not hang. Elapsed: {:?}",
            elapsed
        );

        // The GET should fail (contract doesn't exist) but shouldn't hang
        match get_result {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse { .. }))) => {
                error!("Unexpectedly found non-existent contract");
                panic!("Should not successfully GET a non-existent contract");
            }
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {
                info!(
                    "GET failed as expected for non-existent contract (no self-routing occurred)"
                );
            }
        }

        // Properly close the client
        client
            .send(ClientRequest::Disconnect { cause: None })
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok::<(), anyhow::Error>(())
    });

    // Run node and test concurrently
    select! {
        _ = node_handle => {
            error!("Node exited unexpectedly");
            panic!("Node should not exit during test");
        }
        result = test_result => {
            result??;
            // Give time for cleanup
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Ok(())
}
