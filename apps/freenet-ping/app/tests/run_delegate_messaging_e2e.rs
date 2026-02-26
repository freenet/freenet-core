mod common;

use std::{net::TcpListener, time::Duration};

use anyhow::anyhow;
use freenet::server::serve_client_api;
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
    test_ip_for_node, test_node_config,
};

/// Message types matching test-delegate-messaging's InboundAppMessage
#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    SendToDelegate {
        target_key_bytes: Vec<u8>,
        target_code_hash: Vec<u8>,
        payload: Vec<u8>,
    },
    Ping {
        data: Vec<u8>,
    },
}

/// Message types matching test-delegate-messaging's OutboundAppMessage
#[derive(Debug, Deserialize)]
enum OutboundAppMessage {
    MessageSent,
    DelegateMessageReceived {
        sender_key_bytes: Vec<u8>,
        payload: Vec<u8>,
    },
    PingResponse {
        data: Vec<u8>,
    },
}

/// E2E test for delegate-to-delegate messaging over WebSocket.
///
/// Registers two instances of test-delegate-messaging (with different params
/// to get different DelegateKeys), sends a SendToDelegate command to A targeting B,
/// and verifies the runtime delivers to B and returns B's output.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_delegate_to_delegate_messaging_e2e() -> anyhow::Result<()> {
    std::env::set_var("CARGO_PROFILE_RELEASE_LTO", "true");
    std::env::set_var("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");
    std::env::set_var("CARGO_PROFILE_RELEASE_STRIP", "true");

    // Load two instances of the same delegate with different params → different keys
    let params_a = Parameters::from(vec![1u8]);
    let delegate_a =
        freenet::test_utils::load_delegate("test-delegate-messaging", params_a.clone())?;
    let key_a = delegate_a.key().clone();

    let params_b = Parameters::from(vec![2u8]);
    let delegate_b =
        freenet::test_utils::load_delegate("test-delegate-messaging", params_b.clone())?;
    let key_b = delegate_b.key().clone();

    assert_ne!(
        key_a, key_b,
        "Different params should produce different keys"
    );

    // Allocate unique IP for this test's gateway
    let base_node_idx = allocate_test_node_block(1);
    let gw_ip = test_ip_for_node(base_node_idx);

    // Reserve ports
    let network_socket_gw = TcpListener::bind(std::net::SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_gw = TcpListener::bind(std::net::SocketAddr::new(gw_ip.into(), 0))?;

    let test_seed = *b"delegate_messaging_e2e_testseed!";
    let mut test_rng = rand::rngs::StdRng::from_seed(test_seed);

    let (config_gw, _preset_cfg_gw) = base_node_test_config_with_rng(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_delegate_messaging",
        None,
        None,
        Some(gw_ip),
        &mut test_rng,
    )
    .await?;
    let ws_api_port_gw = config_gw.ws_api.ws_api_port.unwrap();

    std::mem::drop(network_socket_gw);
    std::mem::drop(ws_api_port_socket_gw);

    let gateway_node = async {
        let config = config_gw.build().await?;
        let node = test_node_config(config.clone())
            .await?
            .build(serve_client_api(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let test = tokio::time::timeout(Duration::from_secs(120), async {
        let uri_gw =
            format!("ws://{gw_ip}:{ws_api_port_gw}/v1/contract/command?encodingProtocol=native");
        let mut client = connect_ws_with_retry(&uri_gw, "Gateway", 60).await?;

        // Register delegate A
        client
            .send(ClientRequest::DelegateOp(
                freenet_stdlib::client_api::DelegateRequest::RegisterDelegate {
                    delegate: delegate_a.clone(),
                    cipher: freenet_stdlib::client_api::DelegateRequest::DEFAULT_CIPHER,
                    nonce: freenet_stdlib::client_api::DelegateRequest::DEFAULT_NONCE,
                },
            ))
            .await?;
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
        match resp {
            HostResponse::DelegateResponse { key, .. } => {
                assert_eq!(key, key_a, "Key mismatch registering delegate A");
                tracing::info!("Registered delegate A: {key}");
            }
            other => return Err(anyhow!("Expected DelegateResponse for A, got: {other:?}")),
        }

        // Register delegate B
        client
            .send(ClientRequest::DelegateOp(
                freenet_stdlib::client_api::DelegateRequest::RegisterDelegate {
                    delegate: delegate_b.clone(),
                    cipher: freenet_stdlib::client_api::DelegateRequest::DEFAULT_CIPHER,
                    nonce: freenet_stdlib::client_api::DelegateRequest::DEFAULT_NONCE,
                },
            ))
            .await?;
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
        match resp {
            HostResponse::DelegateResponse { key, .. } => {
                assert_eq!(key, key_b, "Key mismatch registering delegate B");
                tracing::info!("Registered delegate B: {key}");
            }
            other => return Err(anyhow!("Expected DelegateResponse for B, got: {other:?}")),
        }

        // Step 1: Sanity check — ping delegate A
        {
            let app_id = ContractInstanceId::new([0; 32]);
            let payload = bincode::serialize(&InboundAppMessage::Ping {
                data: b"hello".to_vec(),
            })?;
            let app_msg = ApplicationMessage::new(app_id, payload);

            client
                .send(ClientRequest::DelegateOp(
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: key_a.clone(),
                        params: params_a.clone(),
                        inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
                    },
                ))
                .await?;

            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
            match resp {
                HostResponse::DelegateResponse { values, .. } => {
                    let app_msg = values
                        .iter()
                        .find_map(|m| match m {
                            OutboundDelegateMsg::ApplicationMessage(msg) => Some(msg),
                            _ => None,
                        })
                        .expect("Expected ApplicationMessage in ping response");
                    let response: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;
                    match response {
                        OutboundAppMessage::PingResponse { data } => {
                            assert_eq!(data, b"hello");
                            tracing::info!("Ping sanity check passed");
                        }
                        other => return Err(anyhow!("Expected PingResponse, got: {other:?}")),
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Expected DelegateResponse for ping, got: {other:?}"
                    ))
                }
            }
        }

        // Step 2: Send delegate-to-delegate message from A to B
        {
            let app_id = ContractInstanceId::new([0; 32]);
            let payload = bincode::serialize(&InboundAppMessage::SendToDelegate {
                target_key_bytes: key_b.bytes().to_vec(),
                target_code_hash: key_b.code_hash().as_ref().to_vec(),
                payload: b"inter-delegate-e2e".to_vec(),
            })?;
            let app_msg = ApplicationMessage::new(app_id, payload);

            client
                .send(ClientRequest::DelegateOp(
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: key_a.clone(),
                        params: params_a.clone(),
                        inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
                    },
                ))
                .await?;

            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
            match resp {
                HostResponse::DelegateResponse { values, .. } => {
                    tracing::info!("Got {} outbound messages", values.len());
                    for (i, v) in values.iter().enumerate() {
                        tracing::info!("  [{i}] = {v:?}");
                    }

                    // The runtime delivers SendDelegateMessage to B and
                    // accumulates B's output, so we should see:
                    // - A's ApplicationMessage(MessageSent)
                    // - B's ApplicationMessage(DelegateMessageReceived)
                    let received_msg = values
                        .iter()
                        .filter_map(|m| match m {
                            OutboundDelegateMsg::ApplicationMessage(msg) => {
                                bincode::deserialize::<OutboundAppMessage>(&msg.payload).ok()
                            }
                            _ => None,
                        })
                        .find(|m| matches!(m, OutboundAppMessage::DelegateMessageReceived { .. }));

                    match received_msg {
                        Some(OutboundAppMessage::DelegateMessageReceived {
                            sender_key_bytes,
                            payload,
                        }) => {
                            assert_eq!(
                                sender_key_bytes,
                                key_a.bytes(),
                                "Sender should be delegate A"
                            );
                            assert_eq!(payload, b"inter-delegate-e2e");
                            tracing::info!("Delegate-to-delegate message verified!");
                        }
                        _ => {
                            return Err(anyhow!(
                                "Expected DelegateMessageReceived from B in accumulated output"
                            ))
                        }
                    }

                    // Also verify A's MessageSent is present
                    let sent_msg = values
                        .iter()
                        .filter_map(|m| match m {
                            OutboundDelegateMsg::ApplicationMessage(msg) => {
                                bincode::deserialize::<OutboundAppMessage>(&msg.payload).ok()
                            }
                            _ => None,
                        })
                        .find(|m| matches!(m, OutboundAppMessage::MessageSent));

                    assert!(
                        sent_msg.is_some(),
                        "Expected MessageSent from delegate A in output"
                    );
                }
                other => {
                    return Err(anyhow!(
                        "Expected DelegateResponse for send-to-delegate, got: {other:?}"
                    ))
                }
            }
        }

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
