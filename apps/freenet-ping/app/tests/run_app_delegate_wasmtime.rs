mod common;

use std::{net::TcpListener, time::Duration};

use anyhow::anyhow;
use freenet::{local_node::NodeConfig, server::serve_gateway};
use freenet_stdlib::{
    client_api::{ClientRequest, HostResponse},
    prelude::*,
};
use futures::FutureExt;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::select;

use common::{
    allocate_test_node_block, base_node_test_config_with_rng, connect_ws_with_retry,
    test_ip_for_node,
};

/// Message types matching test-delegate-integration's InboundAppMessage
#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    TestRequest(String),
}

/// Message types matching test-delegate-integration's OutboundAppMessage
#[derive(Debug, Deserialize)]
enum OutboundAppMessage {
    TestResponse(String, Vec<u8>),
}

/// E2E test verifying delegate message passing works with the wasmtime backend.
///
/// Loads the test-delegate-integration delegate, registers it, sends two messages,
/// and verifies correct responses.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_delegate_e2e_wasmtime() -> anyhow::Result<()> {
    // Configure optimized WASM builds
    std::env::set_var("CARGO_PROFILE_RELEASE_LTO", "true");
    std::env::set_var("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");
    std::env::set_var("CARGO_PROFILE_RELEASE_STRIP", "true");

    // Load delegate before starting nodes
    let params = Parameters::from(vec![]);
    let delegate = freenet::test_utils::load_delegate("test-delegate-integration", params.clone())?;
    let delegate_key = delegate.key().clone();

    // Allocate unique IP for this test's gateway
    let base_node_idx = allocate_test_node_block(1);
    let gw_ip = test_ip_for_node(base_node_idx);

    // Reserve ports
    let network_socket_gw = TcpListener::bind(std::net::SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_gw = TcpListener::bind(std::net::SocketAddr::new(gw_ip.into(), 0))?;

    let test_seed = *b"delegate_wasmtime_e2e_test_seed!";
    let mut test_rng = rand::rngs::StdRng::from_seed(test_seed);

    // Configure gateway node
    let (config_gw, _preset_cfg_gw) = base_node_test_config_with_rng(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_delegate_wasmtime",
        None,
        None,
        Some(gw_ip),
        &mut test_rng,
    )
    .await?;
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    // Free reserved ports before node startup
    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);

    // Start gateway node
    let gateway_node = async {
        let config = config_gw.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Main test logic
    let test = tokio::time::timeout(Duration::from_secs(120), async {
        let uri_gw =
            format!("ws://{gw_ip}:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let mut client = connect_ws_with_retry(&uri_gw, "Gateway", 60).await?;

        // Register delegate
        client
            .send(ClientRequest::DelegateOp(
                freenet_stdlib::client_api::DelegateRequest::RegisterDelegate {
                    delegate: delegate.clone(),
                    cipher: freenet_stdlib::client_api::DelegateRequest::DEFAULT_CIPHER,
                    nonce: freenet_stdlib::client_api::DelegateRequest::DEFAULT_NONCE,
                },
            ))
            .await?;

        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
        match resp {
            HostResponse::DelegateResponse { key, values: _ } => {
                assert_eq!(
                    key, delegate_key,
                    "Delegate key mismatch in register response"
                );
                tracing::info!("Registered delegate: {key}");
            }
            other => {
                return Err(anyhow!(
                    "Expected DelegateResponse for register, got: {other:?}"
                ))
            }
        }

        // Helper to send a message and verify response
        async fn send_and_verify(
            client: &mut freenet_stdlib::client_api::WebApi,
            delegate_key: &freenet_stdlib::prelude::DelegateKey,
            params: &Parameters<'static>,
            request_data: &str,
        ) -> anyhow::Result<()> {
            let app_id = ContractInstanceId::new([0; 32]);
            let payload =
                bincode::serialize(&InboundAppMessage::TestRequest(request_data.to_string()))?;
            let app_msg = ApplicationMessage::new(app_id, payload);

            client
                .send(ClientRequest::DelegateOp(
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: delegate_key.clone(),
                        params: params.clone(),
                        inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
                    },
                ))
                .await?;

            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
            match resp {
                HostResponse::DelegateResponse {
                    key,
                    values: outbound,
                } => {
                    assert_eq!(&key, delegate_key, "Delegate key mismatch");
                    assert!(!outbound.is_empty(), "No output messages from delegate");

                    let app_msg = match &outbound[0] {
                        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
                        other => {
                            return Err(anyhow!("Expected ApplicationMessage, got: {other:?}"))
                        }
                    };
                    assert!(app_msg.processed, "Message not marked as processed");

                    let response: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;
                    match response {
                        OutboundAppMessage::TestResponse(text, data) => {
                            assert_eq!(text, format!("Processed: {request_data}"));
                            assert_eq!(data, vec![4, 5, 6]);
                        }
                    }
                    Ok(())
                }
                other => Err(anyhow!("Expected DelegateResponse, got: {other:?}")),
            }
        }

        // First message
        send_and_verify(&mut client, &delegate_key, &params, "hello").await?;
        tracing::info!("First delegate message verified");

        // Second message — verifies delegate stays registered across calls
        send_and_verify(&mut client, &delegate_key, &params, "world").await?;
        tracing::info!("Second delegate message verified");

        Ok::<_, anyhow::Error>(())
    });

    select! {
        gw = gateway_node => {
            let Err(gw) = gw;
            anyhow::bail!("Gateway node failed: {}", gw);
        }
        r = test => {
            r??;
        }
    }

    Ok(())
}
