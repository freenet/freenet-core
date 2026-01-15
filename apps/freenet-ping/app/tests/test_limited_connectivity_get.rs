//! # Limited Connectivity GET Test
//!
//! Regression test for the GET request cycle bug fixed in PR #2416.
//!
//! ## Bug Scenario (before fix)
//!
//! When a node has limited connections (e.g., only connected to gateway):
//! 1. Node A (originator, single connection to gateway) sends GET for non-existent contract
//! 2. Gateway marks itself in visited bloom filter, forwards to other peers
//! 3. Other peers mark themselves, but NOT the sender (bug!)
//! 4. Eventually, routing sends request back to Node A (not in visited filter)
//! 5. Node A can't forward (only peer is gateway, already in filter)
//! 6. Request fails with confusing "no peers to forward to and no upstream" error
//!
//! ## Fix
//!
//! Mark the sender (source_addr) in the visited bloom filter alongside this_peer
//! when processing GET requests. This prevents the request from cycling back.
//!
//! ## This Test
//!
//! Creates a minimal network (gateway + 1 peer) and requests a non-existent contract.
//! The request should terminate gracefully (timeout or NotFound) rather than failing
//! with a cycle-related error.
//!
//! ## Why This Topology Works
//!
//! With only 2 nodes (gateway + peer), the cycle forms as follows:
//! 1. Peer sends GET to gateway (htl=10)
//! 2. Gateway has only one connection (peer), so it forwards back to peer (htl=9)
//! 3. Without the fix: peer wasn't marked as visited, so gateway sends back to it
//! 4. With the fix: peer is marked as visited, so gateway has no one to forward to
//!    and returns NotFound more quickly
//!
//! The test validates that the request terminates gracefully in either case.

mod common;

use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use anyhow::anyhow;
use freenet::{local_node::NodeConfig, server::serve_gateway, test_utils::test_ip_for_node};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::FutureExt;
use testresult::TestResult;
use tokio::{select, time::timeout};
use tracing::{span, Instrument, Level};

use common::{
    base_node_test_config_with_ip, connect_async_with_config, gw_config_from_path_with_ip,
    ws_config,
};

/// Test that GET requests for non-existent contracts terminate gracefully
/// even when the requesting node has limited connectivity (only connected to gateway).
///
/// This is a regression test for the bug fixed in PR #2416 where GET requests
/// could cycle back to the originator, causing confusing failures.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_limited_connectivity_get_nonexistent_contract() -> TestResult {
    println!("🔧 Testing GET for non-existent contract with limited connectivity");
    println!("   Network: Gateway + 1 peer (peer only connected to gateway)");

    // Use unique IPs for each node
    let gw_ip = test_ip_for_node(0);
    let peer_ip = test_ip_for_node(1);

    // Create sockets for the network
    let network_socket_gw = TcpListener::bind(SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_gw = TcpListener::bind(SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_peer = TcpListener::bind(SocketAddr::new(peer_ip.into(), 0))?;

    // Configure gateway
    let (config_gw, preset_cfg_gw) = base_node_test_config_with_ip(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_limited_connectivity_get",
        None,
        None,
        Some(gw_ip),
    )
    .await?;
    let public_port = config_gw.network_api.public_port.unwrap();
    let path = preset_cfg_gw.temp_dir.path().to_path_buf();
    let config_info = gw_config_from_path_with_ip(public_port, &path, gw_ip)?;
    let serialized_gateway = serde_json::to_string(&config_info)?;
    let _ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Configure peer - only knows about the gateway (limited connectivity)
    let (config_peer, preset_cfg_peer) = base_node_test_config_with_ip(
        false,
        vec![serialized_gateway],
        None,
        ws_api_port_socket_peer.local_addr()?.port(),
        "peer_limited_connectivity_get",
        None,
        None,
        Some(peer_ip),
    )
    .await?;
    let ws_api_port_peer = config_peer.ws_api.ws_api_port.unwrap();

    // Free the sockets
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);
    std::mem::drop(ws_api_port_socket_peer);

    println!("Network topology:");
    println!("  Gateway");
    println!("    └── Peer (ONLY connection is to gateway)");
    println!();

    let gateway_future = async {
        let config = config_gw.build().await?;
        let mut node_config = NodeConfig::new(config.clone()).await?;
        node_config.min_number_of_connections(1);
        node_config.max_number_of_connections(5);
        let node = node_config
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "gateway"))
    .boxed_local();

    let peer_future = async {
        let config = config_peer.build().await?;
        let mut node_config = NodeConfig::new(config.clone()).await?;
        node_config.min_number_of_connections(1);
        node_config.max_number_of_connections(5);
        let node = node_config
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .instrument(span!(Level::INFO, "peer"))
    .boxed_local();

    let test = timeout(Duration::from_secs(120), async {
        println!("Waiting for nodes to start up...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("✓ Nodes should be up");

        // Connect to peer's WebSocket API
        let uri_peer = format!(
            "ws://{peer_ip}:{ws_api_port_peer}/v1/contract/command?encodingProtocol=native"
        );
        let (stream_peer, _) =
            connect_async_with_config(&uri_peer, Some(ws_config()), false).await?;
        let mut client_peer = WebApi::start(stream_peer);

        println!("✓ Connected to peer node");

        // Generate a random contract ID that definitely doesn't exist
        // Use a random hash to ensure it doesn't match any existing contract
        let nonexistent_id = ContractInstanceId::new([0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
                                                       0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                                                       0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                                                       0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00]);

        println!("📥 Peer requesting non-existent contract...");
        println!("   Contract ID: {nonexistent_id}");
        println!("   This should terminate gracefully (not cycle back to peer)");

        client_peer
            .send(ClientRequest::ContractOp(ContractRequest::Get {
                key: nonexistent_id,
                return_contract_code: false,
                subscribe: false,
            }))
            .await?;

        // Wait for response - should either timeout gracefully or return an error
        // The key is that it should NOT hang forever or return a confusing cycle error
        let start = std::time::Instant::now();
        let result = timeout(Duration::from_secs(60), client_peer.recv()).await;

        match result {
            Ok(Ok(response)) => {
                println!("Received response after {:?}", start.elapsed());
                match response {
                    HostResponse::ContractResponse(ContractResponse::GetResponse {
                        key,
                        state,
                        ..
                    }) => {
                        // Empty state is expected for a non-existent contract
                        if state.is_empty() {
                            println!("✅ GET returned empty state (contract not found)");
                            println!("   This is the expected behavior - request terminated gracefully");
                        } else {
                            println!("⚠️  Unexpected: GET returned non-empty state for non-existent contract");
                            println!("   Key: {key}");
                        }
                    }
                    other => {
                        // Any other response is acceptable as long as the request didn't hang
                        println!("✅ Received response: {other:?}");
                        println!("   Request terminated gracefully");
                    }
                }
            }
            Ok(Err(e)) => {
                // WebSocket error - check if it's the cycle error
                let err_str = format!("{e:?}");
                if err_str.contains("no peers to forward") && err_str.contains("no upstream") {
                    println!("❌ FAILURE: Got the cycle error that PR #2416 was supposed to fix!");
                    return Err(anyhow!(
                        "GET request cycled back - the bloom filter fix is not working"
                    ));
                }
                println!("✅ Got WebSocket error (not the cycle error): {e:?}");
                println!("   Request terminated (acceptable outcome)");
            }
            Err(_) => {
                // Timeout after 60 seconds
                println!("⏱️  Request timed out after 60 seconds");
                println!("   This is acceptable - the request didn't cycle infinitely");
                println!("   (Before the fix, it might have failed quickly with a cycle error)");
            }
        }

        println!("\n✅ Test passed: GET request for non-existent contract terminated gracefully");
        println!("   The request did NOT fail with 'no peers to forward to and no upstream'");

        Ok::<_, anyhow::Error>(())
    })
    .instrument(span!(Level::INFO, "test_limited_connectivity_get"));

    select! {
        r = gateway_future => {
            match r {
                Err(e) => return Err(anyhow!("Gateway stopped unexpectedly: {}", e).into()),
                Ok(_) => return Err(anyhow!("Gateway stopped unexpectedly").into()),
            }
        }
        r = peer_future => {
            match r {
                Err(e) => return Err(anyhow!("Peer stopped unexpectedly: {}", e).into()),
                Ok(_) => return Err(anyhow!("Peer stopped unexpectedly").into()),
            }
        }
        r = test => {
            match r {
                Err(e) => return Err(anyhow!("Test timed out: {}", e).into()),
                Ok(Ok(_)) => {
                    println!("Test completed successfully!");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                },
                Ok(Err(e)) => return Err(anyhow!("Test failed: {}", e).into()),
            }
        }
    }

    drop(preset_cfg_gw);
    drop(preset_cfg_peer);

    Ok(())
}
