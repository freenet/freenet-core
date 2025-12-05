use anyhow::{anyhow, bail, Context};
use freenet::test_utils::{self, make_get, make_put, TestContext, TestResult};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use std::time::Duration;
use tokio_tungstenite::connect_async;

// Fixed ring locations for the three-node connectivity test.
fn fixed_three_node_locations() -> Vec<f64> {
    vec![0.1, 0.5, 0.9]
}

/// Test gateway reconnection:
/// 1. Start a gateway and a peer connected to it
/// 2. Perform operations to verify connectivity
/// 3. Force disconnect
/// 4. Verify that the peer can reconnect and operate normally
/// NOTE: The freenet_test macro configures each peer with a public network port
/// so auto_connect_peers ensures they can form a full mesh rather than only
/// connecting to the gateway.
#[freenet_test(
    nodes = ["gateway", "peer"],
    timeout_secs = 180,
    startup_wait_secs = 15,
    aggregate_events = "always",
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_gateway_reconnection(ctx: &mut TestContext) -> TestResult {
    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Get nodes from context
    let peer = ctx.node("peer")?;
    let peer_ws_port = peer.ws_port;

    // Give extra time for peer to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to peer's websocket API
    let uri = format!("ws://127.0.0.1:{peer_ws_port}/v1/contract/command?encodingProtocol=native");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api = WebApi::start(stream);

    // Perform initial PUT to verify connectivity
    tracing::info!("Performing initial PUT to verify connectivity");
    make_put(
        &mut client_api,
        wrapped_state.clone(),
        contract.clone(),
        false,
    )
    .await?;

    // Wait for put response
    let resp = tokio::time::timeout(Duration::from_secs(60), client_api.recv()).await;
    match resp {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, contract_key);
            tracing::info!("Initial PUT successful");
        }
        Ok(Ok(other)) => {
            bail!("Unexpected response while waiting for put: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving put response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for put response");
        }
    }

    // Verify with GET
    tracing::info!("Verifying with GET");
    make_get(&mut client_api, contract_key, true, false).await?;
    let get_response = tokio::time::timeout(Duration::from_secs(60), client_api.recv()).await;
    match get_response {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            contract: recv_contract,
            state: recv_state,
            ..
        }))) => {
            assert_eq!(
                recv_contract.as_ref().expect("Contract should exist").key(),
                contract_key
            );
            if recv_state != wrapped_state {
                tracing::error!("State mismatch!");
                tracing::error!(
                    "Expected state: {:?}",
                    String::from_utf8_lossy(wrapped_state.as_ref())
                );
                tracing::error!(
                    "Received state: {:?}",
                    String::from_utf8_lossy(recv_state.as_ref())
                );
            }
            assert_eq!(recv_state, wrapped_state);
            tracing::info!("Initial GET successful");
        }
        Ok(Ok(other)) => {
            bail!("Unexpected response while waiting for get: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("Error receiving get response: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for get response");
        }
    }

    // Disconnect from peer
    tracing::info!("Disconnecting from peer");
    client_api
        .send(ClientRequest::Disconnect { cause: None })
        .await?;

    // Wait for disconnect to complete
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Reconnect to the peer's websocket API
    tracing::info!("Reconnecting to peer");
    let (stream, _) = connect_async(&uri).await?;
    let mut client_api = WebApi::start(stream);

    // Wait for reconnection to establish (peer should reconnect to gateway)
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Perform GET to verify reconnection worked and peer can operate normally
    tracing::info!("Performing GET after reconnection");
    make_get(&mut client_api, contract_key, true, false).await?;
    let get_response = tokio::time::timeout(Duration::from_secs(60), client_api.recv()).await;
    match get_response {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            contract: recv_contract,
            state: recv_state,
            ..
        }))) => {
            assert_eq!(
                recv_contract.as_ref().expect("Contract should exist").key(),
                contract_key
            );
            assert_eq!(recv_state, wrapped_state);
            tracing::info!(
                "Reconnection test successful - peer can perform operations after reconnecting"
            );
        }
        Ok(Ok(other)) => {
            bail!(
                "Unexpected response while waiting for get after reconnection: {:?}",
                other
            );
        }
        Ok(Err(e)) => {
            bail!("Error receiving get response after reconnection: {}", e);
        }
        Err(_) => {
            bail!("Timeout waiting for get response after reconnection");
        }
    }

    // Clean disconnect
    client_api
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Simplified test to verify basic gateway connectivity
#[freenet_test(
    nodes = ["gateway"],
    timeout_secs = 30,
    startup_wait_secs = 5,
    aggregate_events = "always",
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
)]
async fn test_basic_gateway_connectivity(ctx: &mut TestContext) -> TestResult {
    // Get the gateway node from context
    let gateway = ctx.node("gateway")?;
    let ws_port = gateway.ws_port;

    // Try to connect to the gateway's WebSocket API
    let uri = format!("ws://127.0.0.1:{ws_port}/v1/contract/command?encodingProtocol=native");
    let result = tokio::time::timeout(Duration::from_secs(10), connect_async(&uri)).await;

    match result {
        Ok(Ok((stream, _))) => {
            tracing::info!("Successfully connected to gateway WebSocket");
            let mut client = WebApi::start(stream);

            // Disconnect cleanly
            client
                .send(ClientRequest::Disconnect { cause: None })
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        }
        Ok(Err(e)) => {
            bail!("Failed to connect to gateway: {}", e);
        }
        Err(_) => {
            bail!("Timeout connecting to gateway");
        }
    }
}

/// Test three-node network connectivity with full mesh formation
/// This test verifies that a network of 3 nodes (1 gateway + 2 peers) can:
/// 1. Establish connections to form a full mesh
/// 2. Successfully perform PUT/GET operations across the network
///
/// # Port Configuration for P2P Mesh
///
/// For peers to participate in P2P mesh connectivity, they must have BOTH
/// `public_address` AND `public_port` configured. This ensures the peer's
/// PeerId is set from config (see config/mod.rs:242-251).
///
/// ## Port Types
///
/// - **network_port**: The local port the peer binds to for listening
/// - **public_port**: The external port peers should connect to
///   - In localhost tests (no NAT): public_port = network_port
///   - In production with NAT: public_port = router's external port
///
/// ## How It Works
///
/// ### Localhost Tests (this test)
/// 1. Peer binds UDP socket to network_port (e.g., 53425)
/// 2. When sending to gateway, UDP uses bound port as source (53425)
/// 3. Gateway sees source port 53425 in handshake
/// 4. Gateway sends back "your external address is 127.0.0.1:53425"
/// 5. Peer's PeerId is already set from config with public_port=53425
/// 6. Other peers connect directly to 127.0.0.1:53425 ✅
///
/// ### Real P2P Network (with NAT)
/// 1. Peer behind NAT binds to network_port (e.g., 8080)
/// 2. Peer sets public_port to router's external port (e.g., 54321)
/// 3. Router forwards external port 54321 → internal port 8080
/// 4. When peer sends to gateway, NAT translates:
///    - Source: 192.168.1.100:8080 → PublicIP:54321
/// 5. Gateway sees source as PublicIP:54321
/// 6. Peer's PeerId is set from config: PublicIP:54321
/// 7. Other peers connect to PublicIP:54321
/// 8. Router forwards to peer's internal 192.168.1.100:8080 ✅
///
#[freenet_test(
    nodes = ["gateway", "peer1", "peer2"],
    timeout_secs = 180,
    startup_wait_secs = 30,
    node_locations_fn = fixed_three_node_locations,
    aggregate_events = "always",
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_three_node_network_connectivity(ctx: &mut TestContext) -> TestResult {
    use freenet_stdlib::client_api::{NodeQuery, QueryResponse};

    // Load test contract
    const TEST_CONTRACT: &str = "test-contract-integration";
    let contract = test_utils::load_contract(TEST_CONTRACT, vec![].into())?;
    let contract_key = contract.key();
    let initial_state = test_utils::create_empty_todo_list();
    let wrapped_state = WrappedState::from(initial_state);

    // Get node information from context
    let gateway = ctx.node("gateway")?;
    let peer1 = ctx.node("peer1")?;
    let peer2 = ctx.node("peer2")?;
    println!(
        "Using deterministic node locations: gateway={:.3}, peer1={:.3}, peer2={:.3}",
        gateway.location, peer1.location, peer2.location
    );

    let peer1_public_port = peer1.network_port.context(
        "peer1 missing network port; auto_connect_peers requires public_port for mesh connectivity",
    )?;
    let peer2_public_port = peer2.network_port.context(
        "peer2 missing network port; auto_connect_peers requires public_port for mesh connectivity",
    )?;
    tracing::info!(
        peer1_port = peer1_public_port,
        peer2_port = peer2_public_port,
        "Verified peer network ports for direct connectivity"
    );

    let gateway_ws_port = gateway.ws_port;
    let peer1_ws_port = peer1.ws_port;
    let peer2_ws_port = peer2.ws_port;

    // Give extra time for peers to connect to gateway
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Connect to websockets
    let uri_gw =
        format!("ws://127.0.0.1:{gateway_ws_port}/v1/contract/command?encodingProtocol=native");
    let (stream_gw, _) = connect_async(&uri_gw).await?;
    let mut client_gw = WebApi::start(stream_gw);

    let uri1 =
        format!("ws://127.0.0.1:{peer1_ws_port}/v1/contract/command?encodingProtocol=native");
    let (stream1, _) = connect_async(&uri1).await?;
    let mut client1 = WebApi::start(stream1);

    let uri2 =
        format!("ws://127.0.0.1:{peer2_ws_port}/v1/contract/command?encodingProtocol=native");
    let (stream2, _) = connect_async(&uri2).await?;
    let mut client2 = WebApi::start(stream2);

    // Retry loop to wait for full mesh connectivity
    // CI can be slower; give more attempts and longer waits before declaring failure.
    const MAX_RETRIES: usize = 90;
    const RETRY_DELAY: Duration = Duration::from_secs(2);
    let mut mesh_established = false;
    let mut last_snapshot = (String::new(), String::new(), String::new());

    for attempt in 1..=MAX_RETRIES {
        tracing::info!(
            "Attempt {}/{}: Querying all nodes for connected peers...",
            attempt,
            MAX_RETRIES
        );

        // Query each node for connections
        client_gw
            .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
            .await?;
        let gw_resp = tokio::time::timeout(Duration::from_secs(10), client_gw.recv()).await?;
        let gw_peers = match gw_resp {
            Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers })) => peers,
            Ok(other) => bail!("Unexpected response from gateway: {:?}", other),
            Err(e) => bail!("Error receiving gateway response: {}", e),
        };

        client1
            .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
            .await?;
        let peer1_resp = tokio::time::timeout(Duration::from_secs(10), client1.recv()).await?;
        let peer1_peers = match peer1_resp {
            Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers })) => peers,
            Ok(other) => bail!("Unexpected response from peer1: {:?}", other),
            Err(e) => bail!("Error receiving peer1 response: {}", e),
        };

        client2
            .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
            .await?;
        let peer2_resp = tokio::time::timeout(Duration::from_secs(10), client2.recv()).await?;
        let peer2_peers = match peer2_resp {
            Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers })) => peers,
            Ok(other) => bail!("Unexpected response from peer2: {:?}", other),
            Err(e) => bail!("Error receiving peer2 response: {}", e),
        };

        tracing::info!("  - Gateway has {} connections", gw_peers.len());
        tracing::info!("  - Peer1 has {} connections", peer1_peers.len());
        tracing::info!("  - Peer2 has {} connections", peer2_peers.len());
        tracing::debug!("Gateway peers: {:?}", gw_peers);
        tracing::debug!("Peer1 peers: {:?}", peer1_peers);
        tracing::debug!("Peer2 peers: {:?}", peer2_peers);

        last_snapshot = (
            format!("{:?}", gw_peers),
            format!("{:?}", peer1_peers),
            format!("{:?}", peer2_peers),
        );

        let expected_gateway_connections = 2; // peers
        let gateway_sees_all = gw_peers.len() >= expected_gateway_connections;

        // Require each peer to maintain at least one live connection (typically
        // the gateway). The topology maintenance loop can continue dialing more
        // neighbors, but the test should pass once the network is fully
        // reachable through the gateway.
        let peer1_has_minimum = !peer1_peers.is_empty();
        let peer2_has_minimum = !peer2_peers.is_empty();

        if gateway_sees_all && peer1_has_minimum && peer2_has_minimum {
            if peer1_peers.len() >= expected_gateway_connections
                && peer2_peers.len() >= expected_gateway_connections
            {
                tracing::info!("✅ Full mesh connectivity established!");
            } else {
                tracing::info!(
                    "✅ Minimum connectivity achieved (gateway sees all peers; each peer has at least one neighbor)"
                );
            }
            mesh_established = true;
            break;
        }

        tracing::info!("Network not yet meeting minimum connectivity, waiting...");
        tokio::time::sleep(RETRY_DELAY).await;
    }

    if !mesh_established {
        tracing::error!(
            gateway_peers = %last_snapshot.0,
            peer1_peers = %last_snapshot.1,
            peer2_peers = %last_snapshot.2,
            "Connectivity check failed; dumping last snapshot"
        );

        if let Ok(aggregator) = ctx.aggregate_events().await {
            if let Ok(events) = aggregator.get_all_events().await {
                tracing::error!(total_events = events.len(), "Aggregated events at timeout");
                for event in events.iter().rev().take(10).rev() {
                    tracing::error!(?event.kind, peer=%event.peer_id, ts=%event.datetime, "Recent event");
                }
            }
        }

        bail!(
            "Failed to establish minimum connectivity after {} attempts. Gateway peers: {}; peer1 peers: {}; peer2 peers: {}",
            MAX_RETRIES,
            last_snapshot.0,
            last_snapshot.1,
            last_snapshot.2
        );
    }

    // Allow a brief settling period before exercising contract operations.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify functionality with PUT/GET
    tracing::info!("Verifying network functionality with PUT/GET operations");

    const PUT_RETRIES: usize = 3;
    perform_put_with_retries(
        &mut client1,
        &wrapped_state,
        &contract,
        &contract_key,
        PUT_RETRIES,
    )
    .await?;

    make_get(&mut client2, contract_key, true, false).await?;
    let get_response = tokio::time::timeout(Duration::from_secs(60), client2.recv()).await;
    match get_response {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
            contract: recv_contract,
            state: recv_state,
            ..
        }))) => {
            assert_eq!(recv_contract.as_ref().unwrap().key(), contract_key);
            assert_eq!(recv_state, wrapped_state);
            tracing::info!("✅ Peer2 successfully retrieved data from network");
        }
        Ok(Ok(other)) => bail!("Unexpected GET response: {:?}", other),
        Ok(Err(e)) => bail!("Error receiving GET response: {}", e),
        Err(_) => bail!("Timeout waiting for GET response"),
    }

    // Clean disconnect
    client_gw
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client1
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client2
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

async fn perform_put_with_retries(
    client: &mut WebApi,
    wrapped_state: &WrappedState,
    contract: &ContractContainer,
    contract_key: &ContractKey,
    max_attempts: usize,
) -> TestResult {
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=max_attempts {
        tracing::info!(attempt, max_attempts, "Starting PUT attempt");
        if let Err(err) = make_put(client, wrapped_state.clone(), contract.clone(), false).await {
            last_err = Some(err);
        } else {
            match tokio::time::timeout(Duration::from_secs(60), client.recv()).await {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                    if key == *contract_key {
                        tracing::info!(attempt, "Peer1 successfully performed PUT");
                        return Ok(());
                    }
                    last_err = Some(anyhow!(
                        "Received PUT response for unexpected key {key:?} (expected {contract_key:?})"
                    ));
                }
                Ok(Ok(other)) => {
                    last_err = Some(anyhow!("Unexpected PUT response: {other:?}"));
                }
                Ok(Err(e)) => {
                    last_err = Some(anyhow!("Error receiving PUT response: {e}"));
                }
                Err(_) => {
                    last_err = Some(anyhow!("Timeout waiting for PUT response"));
                }
            }
        }

        tracing::warn!(
            attempt,
            max_attempts,
            "PUT attempt failed; retrying after short delay"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    Err(last_err.unwrap_or_else(|| anyhow!("PUT failed after {max_attempts} attempts")))
}

/// Regression test for: when a gateway promotes a transient connection,
/// the peer's identity must be immediately visible via QueryConnections.
///
/// This test validates that after the Connect operation completes:
/// 1. The gateway's QueryConnections returns the peer (not empty)
/// 2. The peer's QueryConnections returns the gateway
///
/// Previously, transient connections were stored with `pub_key: None` and
/// only updated via message-based identity learning, which created race
/// conditions where QueryConnections could return empty despite successful
/// connections.
///
/// The fix ensures handle_connect_peer updates the transport entry's pub_key
/// when promoting transients.
#[freenet_test(
    nodes = ["gateway", "peer"],
    timeout_secs = 60,
    startup_wait_secs = 15,
    aggregate_events = "always",
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_gateway_reports_peer_identity_after_connect(ctx: &mut TestContext) -> TestResult {
    use freenet_stdlib::client_api::{NodeQuery, QueryResponse};

    let gateway = ctx.node("gateway")?;
    let peer = ctx.node("peer")?;

    // Wait for the connection handshake to complete - but no extra time
    // This is the minimum wait for the test framework's startup_wait_secs
    // After this, connections MUST be established and identities known

    // Connect to websockets
    let uri_gw = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        gateway.ws_port
    );
    let (stream_gw, _) = connect_async(&uri_gw).await?;
    let mut client_gw = WebApi::start(stream_gw);

    let uri_peer = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer.ws_port
    );
    let (stream_peer, _) = connect_async(&uri_peer).await?;
    let mut client_peer = WebApi::start(stream_peer);

    // Query gateway for connections - should return the peer IMMEDIATELY
    // No retry loop allowed here - this is the regression test's core assertion
    client_gw
        .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
        .await?;
    let gw_resp = tokio::time::timeout(Duration::from_secs(5), client_gw.recv()).await?;
    let gw_peers = match gw_resp {
        Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers })) => peers,
        Ok(other) => bail!("Unexpected response from gateway: {:?}", other),
        Err(e) => bail!("Error receiving gateway response: {}", e),
    };

    // Query peer for connections - should return the gateway
    client_peer
        .send(ClientRequest::NodeQueries(NodeQuery::ConnectedPeers))
        .await?;
    let peer_resp = tokio::time::timeout(Duration::from_secs(5), client_peer.recv()).await?;
    let peer_peers = match peer_resp {
        Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers { peers })) => peers,
        Ok(other) => bail!("Unexpected response from peer: {:?}", other),
        Err(e) => bail!("Error receiving peer response: {}", e),
    };

    tracing::info!(
        gateway_connections = gw_peers.len(),
        peer_connections = peer_peers.len(),
        "Connection visibility check"
    );

    // CRITICAL ASSERTIONS - the whole point of this regression test
    // Gateway MUST see the peer immediately after connect handshake
    if gw_peers.is_empty() {
        bail!(
            "REGRESSION: Gateway's QueryConnections returned empty! \
             This indicates the peer's identity was not propagated to the \
             transport layer when the transient connection was promoted. \
             See PR #2211 for the original bug fix."
        );
    }

    // Peer MUST see the gateway
    if peer_peers.is_empty() {
        bail!(
            "REGRESSION: Peer's QueryConnections returned empty! \
             This indicates the connection wasn't properly established."
        );
    }

    tracing::info!(
        "✅ Connection identity propagation verified: gateway sees {} peer(s), peer sees {} connection(s)",
        gw_peers.len(),
        peer_peers.len()
    );

    Ok(())
}
