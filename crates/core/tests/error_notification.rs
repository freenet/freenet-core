//! Tests for error notification delivery to WebSocket clients
//!
//! This test suite verifies that operation errors are properly delivered to clients
//! via the result router, rather than leaving clients hanging indefinitely.
//!
//! Related Issues:
//! - #1858: Clients hang when operations fail (no error notification)
//! - #2490: Notify clients when async subscription fails

use freenet::{
    local_node::NodeConfig,
    server::serve_client_api,
    test_utils::{TestContext, load_contract, make_get, make_subscribe},
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use std::{
    net::Ipv4Addr,
    sync::{LazyLock, Mutex},
    time::Duration,
};
use tokio::{select, time::timeout};
use tokio_tungstenite::connect_async;
use tracing::{error, info};

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    use rand::SeedableRng;
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"error_notification_test_seed0123",
    ))
});

/// Test that GET operation errors are delivered to WebSocket clients
///
/// This test verifies that when a GET operation fails (e.g., contract not found
/// on an isolated node), the client receives an error response rather than
/// hanging indefinitely.
///
/// Fixes: #1858
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_get_error_notification(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;

    // Connect to the node
    let (ws_stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    info!("Testing GET operation for non-existent contract (should fail with error)");

    // Create a contract to get its key, but we won't PUT it - so GET will fail
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![1u8; 32].into())?; // Random params
    let nonexistent_key = contract.key();

    // Attempt to GET a contract that doesn't exist - should fail
    make_get(&mut client, nonexistent_key, false, false).await?;

    // Wait for response - should receive SOME response (error or otherwise) within reasonable time
    // The key test is that we DON'T timeout - errors should be delivered
    let get_result = timeout(Duration::from_secs(30), client.recv()).await;

    match get_result {
        Ok(Ok(response)) => {
            // Any response is good - means we're not hanging
            info!("✓ Received response (not timing out): {:?}", response);
            info!("✓ Client properly notified instead of hanging");
        }
        Ok(Err(e)) => {
            // WebSocket error could indicate error was delivered
            info!("✓ Received error notification: {}", e);
        }
        Err(_) => {
            panic!(
                "GET operation timed out - no response received! \
                 This indicates the bug from issue #1858 has regressed. \
                 Clients should receive error responses, not hang indefinitely."
            );
        }
    }

    info!("Error notification test passed - client did not hang on operation failure");

    // Properly close the client
    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Test that PUT operation errors are delivered to WebSocket clients
///
/// This test verifies that when a PUT operation fails (e.g., invalid contract),
/// the client receives an error response rather than hanging indefinitely.
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_error_notification(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;

    // Connect to the node
    let (ws_stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    info!("Testing PUT operation with invalid contract (should fail with error)");

    // Try to PUT with malformed contract data - this should fail
    // We'll use make_put with invalid state to trigger an error
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;

    // Create invalid state that will cause PUT to fail
    let invalid_state = WrappedState::new(vec![0xFF; 1024 * 1024]); // 1MB of invalid data

    let put_request = ClientRequest::ContractOp(ContractRequest::Put {
        contract: contract.clone(),
        state: invalid_state,
        related_contracts: Default::default(),
        subscribe: false,
        blocking_subscribe: false,
    });

    client.send(put_request).await?;

    // Wait for response - should receive error response
    let put_result = timeout(Duration::from_secs(30), client.recv()).await;

    // Synthesised marker emitted by the retry loop when the bypass
    // closes early. Reject it explicitly: a response containing this
    // string means the client got an infrastructure leak rather than
    // the contract's real rejection reason.
    const FORBIDDEN_MARKER: &str = "failed notifying, channel closed";

    match put_result {
        Ok(Ok(response)) => {
            let rendered = format!("{response:?}");
            assert!(
                !rendered.contains(FORBIDDEN_MARKER),
                "PUT failure response contained the forbidden marker \
                 ({FORBIDDEN_MARKER:?}). Response was: {rendered}"
            );
            info!("✓ Received response without forbidden marker: {response:?}");
        }
        Ok(Err(e)) => {
            let rendered = e.to_string();
            assert!(
                !rendered.contains(FORBIDDEN_MARKER),
                "PUT WS error contained the forbidden marker \
                 ({FORBIDDEN_MARKER:?}). Error was: {rendered}"
            );
            info!("✓ Received error notification without forbidden marker: {rendered}");
        }
        Err(_) => {
            panic!(
                "PUT operation timed out - no response received! \
                 This indicates clients are not receiving error notifications. \
                 Clients should receive error responses, not hang indefinitely."
            );
        }
    }

    info!("PUT error notification test passed - client did not hang on operation failure");
    // Follow-up: tracked by #4147 — adds a wrapper contract that
    // emits a deterministic cause string so the assertion can
    // verify the real contract-side reason, not just the absence
    // of the FORBIDDEN_MARKER.

    // Properly close the client
    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// PR #4126 review item M1 part 2: integration coverage of the
/// multi-hop PUT-error bubble path.
///
/// Setup: 2-node ring (gateway + peer-a). The client connects to
/// `peer-a` (NOT the gateway), so the PUT enters at a non-originator
/// node and the wire path is:
///
///   peer-a (client) → peer-a (originator-loopback relay) → gateway
///
/// `run_relay_put` runs at peer-a in originator-loopback mode (because
/// the WS client is local). The PUT forward to the gateway exercises
/// `drive_relay_put`'s downstream-reply handling for `PutMsg::Error`
/// when the gateway rejects the invalid state, and the upstream-bubble
/// helper `relay_put_send_error` when the gateway's response surfaces
/// at peer-a.
///
/// What this pins behaviourally (delta vs the single-node
/// `test_put_error_notification` above):
///
///   - The client connected to a non-gateway node still receives a
///     terminal response (no `OPERATION_TTL` hang) when the PUT fails.
///   - The response does NOT carry the `FORBIDDEN_MARKER` synthesised
///     by the pre-#4111 race shape. If the multi-hop bubble regresses
///     (e.g. `node.rs::handle_pure_network_message_v1` PUT bypass stops
///     accepting `PutMsg::Error`, or `drive_relay_put`'s `PutMsg::Error`
///     reply arm falls back through to `UnexpectedOpState`), the
///     originator's `send_to_and_await` will time out and either
///     surface the marker or hang past the test deadline — both
///     trip this assertion.
///
/// Known limitation: with the available `test-contract-integration`
/// contract, invalid state is rejected at every hop (no stateful
/// version check), so the failure can land on either peer-a's local
/// validation OR the gateway's relay validation depending on routing
/// timing. The B1 fix and the multi-hop bubble arm are both
/// candidate paths exercised. A stricter test that DEFINITIVELY
/// pins "failure happens on the non-originator node only" — using
/// a wrapper contract with stateful version validation and a
/// deterministic, asserted-on cause string — is tracked as
/// follow-up #4147.
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway", "peer-a"],
    timeout_secs = 120,
    startup_wait_secs = 30,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_put_error_notification_multi_hop(ctx: &mut TestContext) -> TestResult {
    let peer = ctx.node("peer-a")?;
    let (ws_stream, _) = connect_async(&peer.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    info!("Testing multi-hop PUT error: client → peer-a → gateway (ws on peer-a, NOT gateway)");

    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![].into())?;
    // 1 MB of 0xFF — same shape as `test_put_error_notification`,
    // proven to deterministically fail the contract's validator.
    let invalid_state = WrappedState::new(vec![0xFF; 1024 * 1024]);

    let put_request = ClientRequest::ContractOp(ContractRequest::Put {
        contract: contract.clone(),
        state: invalid_state,
        related_contracts: Default::default(),
        subscribe: false,
        blocking_subscribe: false,
    });
    client.send(put_request).await?;

    // 30 s budget — must be > the cross-hop processing window but
    // well under `OPERATION_TTL` so a regression that drops the
    // bubble en route surfaces as a panic here, not as a passing
    // (slow) test.
    let put_result = timeout(Duration::from_secs(30), client.recv()).await;

    // Same FORBIDDEN_MARKER as the single-hop variant: the synthesised
    // "failed notifying, channel closed" string is the canonical
    // tell-tale that the M1/M2 race fired and the bypass→bubble chain
    // failed to deliver the real cause.
    const FORBIDDEN_MARKER: &str = "failed notifying, channel closed";

    match put_result {
        Ok(Ok(response)) => {
            let rendered = format!("{response:?}");
            assert!(
                !rendered.contains(FORBIDDEN_MARKER),
                "Multi-hop PUT failure response contained the forbidden \
                 marker ({FORBIDDEN_MARKER:?}) — the bypass→bubble chain \
                 regressed to the synthesised-error shape from issue #4111. \
                 Either the bypass at node.rs PUT branch stopped accepting \
                 PutMsg::Error, or drive_relay_put's PutMsg::Error reply arm \
                 fell back to UnexpectedOpState. Response was: {rendered}"
            );
            info!("✓ Multi-hop response received without forbidden marker: {response:?}");
        }
        Ok(Err(e)) => {
            let rendered = e.to_string();
            assert!(
                !rendered.contains(FORBIDDEN_MARKER),
                "Multi-hop PUT WS error contained the forbidden marker \
                 ({FORBIDDEN_MARKER:?}). Got: {rendered}"
            );
            info!("✓ Multi-hop error notification received without forbidden marker: {rendered}");
        }
        Err(_) => {
            panic!(
                "Multi-hop PUT timed out after 30 s — clients connected to a \
                 non-gateway node are not receiving error notifications when \
                 the failure path crosses the relay boundary. This indicates \
                 the multi-hop bubble path (PR #4126, B1 fix) regressed: \
                 either run_relay_put's non-loopback Err branch stopped \
                 emitting PutMsg::Error to upstream_addr, or the upstream \
                 relay's bypass stopped forwarding it. The single-node \
                 test_put_error_notification would still pass in this state — \
                 that's why this 2-node companion is load-bearing."
            );
        }
    }

    info!(
        "Multi-hop PUT error notification test passed — client at non-gateway \
         node received a real response without the forbidden marker"
    );

    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Test that UPDATE operation errors are delivered to WebSocket clients
///
/// This test verifies that when an UPDATE operation fails (e.g., contract doesn't exist),
/// the client receives an error response rather than hanging indefinitely.
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 10,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_update_error_notification(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;

    // Connect to the node
    let (ws_stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    info!("Testing UPDATE operation for non-existent contract (should fail with error)");

    // Create a contract key for a contract that doesn't exist
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![99u8; 32].into())?; // Random params
    let nonexistent_key = contract.key();

    // Try to UPDATE a contract that doesn't exist
    let new_state = State::from(vec![1, 2, 3, 4]);
    let update_request = ClientRequest::ContractOp(ContractRequest::Update {
        key: nonexistent_key,
        data: freenet_stdlib::prelude::UpdateData::State(new_state),
    });

    client.send(update_request).await?;

    // Wait for response - should receive error response
    let update_result = timeout(Duration::from_secs(30), client.recv()).await;

    match update_result {
        Ok(Ok(response)) => {
            // Any response is good - means we're not hanging
            info!("✓ Received response (not timing out): {:?}", response);
            info!("✓ Client properly notified instead of hanging");
        }
        Ok(Err(e)) => {
            // WebSocket error could indicate error was delivered
            info!("✓ Received error notification: {}", e);
        }
        Err(_) => {
            panic!(
                "UPDATE operation timed out - no response received! \
                 This indicates clients are not receiving error notifications. \
                 Clients should receive error responses, not hang indefinitely."
            );
        }
    }

    info!("UPDATE error notification test passed - client did not hang on operation failure");

    // Properly close the client
    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Subscribe to a non-existent contract on a 2-node network and verify the client
/// receives an error instead of silently holding a dead notification channel.
///
/// Fixes: #2490
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway", "node-a"],
    timeout_secs = 180,
    startup_wait_secs = 30,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_subscribe_failure_notifies_client(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let (ws_stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![42u8; 32].into())?;
    make_subscribe(&mut client, contract.key()).await?;

    let mut got_result_error = false;
    let mut got_notification_error = false;
    let deadline = Duration::from_secs(90);
    let start = tokio::time::Instant::now();

    while start.elapsed() < deadline {
        match timeout(Duration::from_secs(10), client.recv()).await {
            Ok(Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::SubscribeResponse {
                    subscribed, ..
                },
            ))) => {
                if !subscribed {
                    got_result_error = true;
                }
            }
            Ok(Err(e)) => {
                let err_str = format!("{e}");
                if err_str.contains("not found") || err_str.contains("Subscription failed") {
                    got_notification_error = true;
                } else {
                    got_result_error = true;
                }
            }
            Ok(Ok(_)) => {}
            Err(_) => break,
        }

        if got_result_error && got_notification_error {
            break;
        }
    }

    assert!(
        got_result_error || got_notification_error,
        "Client did not receive any error for failed subscription \
         Got result_error={}, notification_error={}",
        got_result_error,
        got_notification_error
    );

    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    Ok(())
}

/// Subscribe without prior PUT is rejected immediately with a descriptive error.
///
/// When a client subscribes to a contract that hasn't been PUT or GET'd,
/// the node lacks the WASM needed to validate updates.
/// The Subscribe must be rejected fast (not after a network timeout).
///
/// Regression test for #3601.
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway"],
    timeout_secs = 60,
    startup_wait_secs = 15,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_subscribe_without_wasm_rejected(ctx: &mut TestContext) -> TestResult {
    let gateway = ctx.gateway()?;
    let (ws_stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(ws_stream);

    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = load_contract(TEST_CONTRACT, vec![42u8; 32].into())?;

    // Subscribe WITHOUT prior PUT — should be rejected quickly
    make_subscribe(&mut client, contract.key()).await?;

    let start = tokio::time::Instant::now();
    let mut got_rejection = false;

    // The rejection should arrive fast (seconds, not minutes)
    while start.elapsed() < Duration::from_secs(15) {
        match timeout(Duration::from_secs(5), client.recv()).await {
            Ok(Err(e)) => {
                let err_str = format!("{e}");
                if err_str.contains("WASM") || err_str.contains("not cached locally") {
                    got_rejection = true;
                    info!("Got expected WASM rejection error: {err_str}");
                    break;
                } else {
                    // Any error counts as rejection (handler error, etc.)
                    got_rejection = true;
                    info!("Got error (not WASM-specific): {err_str}");
                    break;
                }
            }
            Ok(Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::SubscribeResponse {
                    subscribed: false,
                    ..
                },
            ))) => {
                got_rejection = true;
                info!("Got SubscribeResponse with subscribed=false");
                break;
            }
            Ok(Ok(other)) => {
                tracing::debug!("Skipping unrelated response: {other}");
            }
            Err(_) => break, // timeout
        }
    }

    assert!(
        got_rejection,
        "Subscribe without prior PUT should be rejected, but no error received within 15s"
    );
    // Verify it was fast (not a network timeout)
    assert!(
        start.elapsed() < Duration::from_secs(10),
        "Rejection took too long ({:?}), should be immediate",
        start.elapsed()
    );

    client
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    Ok(())
}

/// Test that errors are delivered when a peer connection drops
///
/// This test verifies that when a connection to a peer is lost during an operation,
/// the client receives an error response rather than hanging indefinitely.
///
/// Scenario: Connect 2 peers (gateway + peer1), establish connection, then forcibly
/// drop the peer to trigger connection errors.
///
/// Note: This test uses real TCP networking, so it cannot use `start_paused = true`
/// which only works with simulated/mocked I/O.
#[test_log::test(tokio::test(flavor = "current_thread"))]
async fn test_connection_drop_error_notification() -> anyhow::Result<()> {
    use std::net::TcpListener;
    // Create network sockets for gateway and peer
    let gateway_network_socket = TcpListener::bind("127.0.0.1:0")?;
    let gateway_ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let peer_ws_socket = TcpListener::bind("127.0.0.1:0")?;

    let gateway_port = gateway_network_socket.local_addr()?.port();
    let gateway_ws_port = gateway_ws_socket.local_addr()?.port();
    let peer_ws_port = peer_ws_socket.local_addr()?.port();

    // Gateway configuration
    let temp_dir_gw = tempfile::tempdir()?;
    let gateway_key = freenet::dev_tool::TransportKeypair::new();
    let gateway_transport_keypair = temp_dir_gw.path().join("private.pem");
    gateway_key.save(&gateway_transport_keypair)?;
    gateway_key
        .public()
        .save(temp_dir_gw.path().join("public.pem"))?;

    let gateway_config = freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(gateway_ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
            allowed_host: None,
            allowed_source_cidrs: None,
        },
        network_api: freenet::config::NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: Some(gateway_port),
            is_gateway: true,
            skip_load_from_network: true,
            gateways: Some(vec![]),
            location: Some({
                use rand::Rng;
                RNG.lock().unwrap().random()
            }),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(gateway_port),
            min_connections: None,
            max_connections: None,
            bandwidth_limit: None,
            blocked_addresses: None,
            transient_budget: None,
            transient_ttl_secs: None,
            total_bandwidth_limit: None,
            min_bandwidth_per_connection: None,
            ..Default::default()
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir_gw.path().to_path_buf()),
            data_dir: Some(temp_dir_gw.path().to_path_buf()),
            log_dir: Some(temp_dir_gw.path().to_path_buf()),
        },
        secrets: freenet::config::SecretArgs {
            transport_keypair: Some(gateway_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Peer configuration
    let temp_dir_peer = tempfile::tempdir()?;
    let peer_key = freenet::dev_tool::TransportKeypair::new();
    let peer_transport_keypair = temp_dir_peer.path().join("private.pem");
    peer_key.save(&peer_transport_keypair)?;

    let gateway_info = freenet::config::InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, gateway_port).into(),
        location: Some({
            use rand::Rng;
            RNG.lock().unwrap().random()
        }),
        public_key_path: temp_dir_gw.path().join("public.pem"),
    };

    let peer_config = freenet::config::ConfigArgs {
        ws_api: freenet::config::WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(peer_ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
            allowed_host: None,
            allowed_source_cidrs: None,
        },
        network_api: freenet::config::NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port: None,
            is_gateway: false,
            skip_load_from_network: true,
            gateways: Some(vec![serde_json::to_string(&gateway_info)?]),
            location: Some({
                use rand::Rng;
                RNG.lock().unwrap().random()
            }),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: None,
            min_connections: None,
            max_connections: None,
            bandwidth_limit: None,
            blocked_addresses: None,
            transient_budget: None,
            transient_ttl_secs: None,
            total_bandwidth_limit: None,
            min_bandwidth_per_connection: None,
            ..Default::default()
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir_peer.path().to_path_buf()),
            data_dir: Some(temp_dir_peer.path().to_path_buf()),
            log_dir: Some(temp_dir_peer.path().to_path_buf()),
        },
        secrets: freenet::config::SecretArgs {
            transport_keypair: Some(peer_transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    // Free the sockets before starting nodes
    std::mem::drop(gateway_network_socket);
    std::mem::drop(gateway_ws_socket);
    std::mem::drop(peer_ws_socket);

    // Start gateway node
    let gateway = async {
        let config = gateway_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_client_api(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start peer node in a way we can drop it later
    let (peer_shutdown_tx, mut peer_shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    let peer = async move {
        let config = peer_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_client_api(config.ws_api).await?)
            .await?;

        // Run node until we receive shutdown signal
        tokio::select! {
            result = node.run() => result,
            _ = peer_shutdown_rx.recv() => {
                info!("Peer received shutdown signal - simulating connection drop");
                // We can't construct Infallible, so return an error to exit cleanly
                Err(anyhow::anyhow!("Peer shutdown requested"))
            }
        }
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(90), async move {
        // Wait for nodes to start and connect
        info!("Waiting for nodes to start up and connect...");
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Connect a client to the gateway
        let url = format!(
            "ws://localhost:{}/v1/contract/command?encodingProtocol=native",
            gateway_ws_port
        );
        let (ws_stream, _) = connect_async(&url).await?;
        let mut client = WebApi::start(ws_stream);

        info!("Client connected to gateway");

        // Try to PUT a contract (this should work initially)
        const TEST_CONTRACT: &str = "test-contract-integration";
        let contract = load_contract(TEST_CONTRACT, vec![].into())?;
        let state = freenet::test_utils::create_empty_todo_list();
        let wrapped_state = WrappedState::from(state);

        // Start a PUT operation
        let put_request = ClientRequest::ContractOp(ContractRequest::Put {
            contract: contract.clone(),
            state: wrapped_state.clone(),
            related_contracts: Default::default(),
            subscribe: false,
            blocking_subscribe: false,
        });

        client.send(put_request).await?;

        // Give the PUT a moment to start processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Now forcibly drop the peer connection
        info!("Dropping peer connection to simulate network failure...");
        peer_shutdown_tx.send(()).await?;

        // Give time for the drop to be detected
        tokio::time::sleep(Duration::from_secs(2)).await;

        // The PUT may or may not succeed depending on timing, but we should get SOME response
        // The key is that we don't hang indefinitely
        info!("Waiting for response after connection drop...");
        let response_result = timeout(Duration::from_secs(30), client.recv()).await;

        match response_result {
            Ok(Ok(response)) => {
                info!("✓ Received response after connection drop: {:?}", response);
                info!("✓ Client properly handled connection drop scenario");
            }
            Ok(Err(e)) => {
                info!("✓ Received error notification after connection drop: {}", e);
                info!("✓ Client properly notified of connection issues");
            }
            Err(_) => {
                panic!(
                    "Operation timed out after connection drop - no response received! \
                     This indicates clients are not being notified of connection failures. \
                     Clients should receive error responses even when connections fail."
                );
            }
        }

        info!("Connection drop error notification test passed");

        // Try to disconnect cleanly (may fail if connection is already gone)
        let _disconnect = client.send(ClientRequest::Disconnect { cause: None }).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok::<(), anyhow::Error>(())
    });

    // Run gateway, peer, and test concurrently
    select! {
        _ = gateway => {
            error!("Gateway exited unexpectedly");
            Ok(())
        }
        _ = peer => {
            // Peer is expected to exit when we drop it
            Ok(())
        }
        result = test => {
            result??;
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        }
    }
}
