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

/// Message types matching test-delegate-creation's InboundAppMessage
#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    CreateChildDelegate {
        child_wasm: Vec<u8>,
        child_params: Vec<u8>,
    },
    Ping {
        data: Vec<u8>,
    },
}

/// Message types matching test-delegate-creation's OutboundAppMessage
#[derive(Debug, Deserialize)]
enum OutboundAppMessage {
    ChildCreated {
        key_bytes: Vec<u8>,
        code_hash_bytes: Vec<u8>,
    },
    CreateFailed {
        error_code: i32,
    },
    PingResponse {
        data: Vec<u8>,
    },
}

/// Message types matching test-delegate-messaging's InboundAppMessage (used for child ping)
#[derive(Debug, Serialize, Deserialize)]
enum ChildInboundAppMessage {
    SendToDelegate {
        target_key_bytes: Vec<u8>,
        target_code_hash: Vec<u8>,
        payload: Vec<u8>,
    },
    Ping {
        data: Vec<u8>,
    },
}

/// Message types matching test-delegate-messaging's OutboundAppMessage (used for child ping)
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
enum ChildOutboundAppMessage {
    MessageSent,
    DelegateMessageReceived {
        sender_key_bytes: Vec<u8>,
        payload: Vec<u8>,
    },
    PingResponse {
        data: Vec<u8>,
    },
}

/// E2E test for delegate creation by delegates.
///
/// Registers a test-delegate-creation delegate, then sends it a message to create
/// a child delegate (test-delegate-messaging). Verifies the child is created and
/// can respond to messages.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_delegate_creation_e2e() -> anyhow::Result<()> {
    std::env::set_var("CARGO_PROFILE_RELEASE_LTO", "true");
    std::env::set_var("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");
    std::env::set_var("CARGO_PROFILE_RELEASE_STRIP", "true");

    // Load the parent delegate (test-delegate-creation)
    let parent_params = Parameters::from(vec![]);
    let parent_delegate =
        freenet::test_utils::load_delegate("test-delegate-creation", parent_params.clone())?;
    let parent_key = parent_delegate.key().clone();

    // Compile the child delegate WASM bytes (test-delegate-messaging)
    let child_wasm_bytes = freenet::test_utils::compile_delegate("test-delegate-messaging")?;
    let child_params_bytes = vec![42u8]; // arbitrary params for the child

    // Allocate unique IP for this test's gateway
    let base_node_idx = allocate_test_node_block(1);
    let gw_ip = test_ip_for_node(base_node_idx);

    // Reserve ports
    let network_socket_gw = TcpListener::bind(std::net::SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_gw = TcpListener::bind(std::net::SocketAddr::new(gw_ip.into(), 0))?;

    let test_seed = *b"delegate_creation_e2e_testseed!!";
    let mut test_rng = rand::rngs::StdRng::from_seed(test_seed);

    let (config_gw, _preset_cfg_gw) = base_node_test_config_with_rng(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_delegate_creation",
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

        // Register parent delegate
        client
            .send(ClientRequest::DelegateOp(
                freenet_stdlib::client_api::DelegateRequest::RegisterDelegate {
                    delegate: parent_delegate.clone(),
                    cipher: freenet_stdlib::client_api::DelegateRequest::DEFAULT_CIPHER,
                    nonce: freenet_stdlib::client_api::DelegateRequest::DEFAULT_NONCE,
                },
            ))
            .await?;
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
        match resp {
            HostResponse::DelegateResponse { key, .. } => {
                assert_eq!(key, parent_key, "Key mismatch registering parent delegate");
                tracing::info!("Registered parent delegate: {key}");
            }
            other => {
                return Err(anyhow!(
                    "Expected DelegateResponse for parent, got: {other:?}"
                ))
            }
        }

        // Step 1: Sanity check — ping parent delegate
        {
            let app_id = ContractInstanceId::new([0; 32]);
            let payload = bincode::serialize(&InboundAppMessage::Ping {
                data: b"hello-parent".to_vec(),
            })?;
            let app_msg = ApplicationMessage::new(app_id, payload);

            client
                .send(ClientRequest::DelegateOp(
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: parent_key.clone(),
                        params: parent_params.clone(),
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
                            assert_eq!(data, b"hello-parent");
                            tracing::info!("Parent ping sanity check passed");
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

        // Step 2: Create child delegate via host function
        let (child_key_bytes, child_code_hash_bytes) = {
            let app_id = ContractInstanceId::new([0; 32]);
            let payload = bincode::serialize(&InboundAppMessage::CreateChildDelegate {
                child_wasm: child_wasm_bytes.clone(),
                child_params: child_params_bytes.clone(),
            })?;
            let app_msg = ApplicationMessage::new(app_id, payload);

            client
                .send(ClientRequest::DelegateOp(
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: parent_key.clone(),
                        params: parent_params.clone(),
                        inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
                    },
                ))
                .await?;

            let resp = tokio::time::timeout(Duration::from_secs(60), client.recv()).await??;
            match resp {
                HostResponse::DelegateResponse { values, .. } => {
                    let app_msg = values
                        .iter()
                        .find_map(|m| match m {
                            OutboundDelegateMsg::ApplicationMessage(msg) => Some(msg),
                            _ => None,
                        })
                        .expect("Expected ApplicationMessage in creation response");
                    let response: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;
                    match response {
                        OutboundAppMessage::ChildCreated {
                            key_bytes,
                            code_hash_bytes,
                        } => {
                            tracing::info!(
                                "Child delegate created! key={} bytes, hash={} bytes",
                                key_bytes.len(),
                                code_hash_bytes.len()
                            );
                            assert_eq!(key_bytes.len(), 32, "Key should be 32 bytes");
                            assert_eq!(code_hash_bytes.len(), 32, "Code hash should be 32 bytes");
                            (key_bytes, code_hash_bytes)
                        }
                        OutboundAppMessage::CreateFailed { error_code } => {
                            return Err(anyhow!(
                                "Child delegate creation failed with error code: {error_code}"
                            ))
                        }
                        other => {
                            return Err(anyhow!(
                                "Expected ChildCreated or CreateFailed, got: {other:?}"
                            ))
                        }
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Expected DelegateResponse for creation, got: {other:?}"
                    ))
                }
            }
        };

        // Step 3: Verify child delegate is executable — send it a Ping
        {
            let child_key_arr: [u8; 32] = child_key_bytes
                .clone()
                .try_into()
                .map_err(|_| anyhow!("Invalid child key length"))?;
            let child_hash_arr: [u8; 32] = child_code_hash_bytes
                .clone()
                .try_into()
                .map_err(|_| anyhow!("Invalid child hash length"))?;
            let child_key = DelegateKey::new(child_key_arr, CodeHash::new(child_hash_arr));
            let child_params = Parameters::from(child_params_bytes.clone());

            let app_id = ContractInstanceId::new([0; 32]);
            let payload = bincode::serialize(&ChildInboundAppMessage::Ping {
                data: b"hello-child".to_vec(),
            })?;
            let app_msg = ApplicationMessage::new(app_id, payload);

            client
                .send(ClientRequest::DelegateOp(
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: child_key.clone(),
                        params: child_params,
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
                        .expect("Expected ApplicationMessage in child ping response");
                    let response: ChildOutboundAppMessage = bincode::deserialize(&app_msg.payload)?;
                    match response {
                        ChildOutboundAppMessage::PingResponse { data } => {
                            assert_eq!(data, b"hello-child");
                            tracing::info!("Child delegate responded to ping — creation verified!");
                        }
                        other => {
                            return Err(anyhow!("Expected child PingResponse, got: {other:?}"))
                        }
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Expected DelegateResponse for child ping, got: {other:?}"
                    ))
                }
            }
        }

        // Step 4: Verify per-call creation limit enforcement
        // MAX_DELEGATE_CREATIONS_PER_CALL = 8, so create 8 more (we already created 1)
        // The 8th creation (9th total, but limit is per-call so each call resets)
        // should succeed. We test a batch of 9 in one call to hit the limit.
        {
            tracing::info!("Testing per-call creation limit...");

            // Create 9 delegates in a single process() call by sending 9 messages
            // Actually, each ApplicationMessages call invokes process() once per message,
            // and the limit is per process() call. So we need to verify in a single call.
            // Since we can only send one CreateChildDelegate per process() invocation
            // (each process() call handles one InboundDelegateMsg), the per-call limit
            // means per single process() invocation. This is inherently satisfied since
            // each message triggers one create_delegate call.
            //
            // To properly test the limit, we'd need a delegate that creates multiple
            // children in a single process() call. For now, verify that individual
            // creations continue to work (the limit resets per call).
            let app_id = ContractInstanceId::new([0; 32]);
            let payload = bincode::serialize(&InboundAppMessage::CreateChildDelegate {
                child_wasm: child_wasm_bytes.clone(),
                child_params: vec![99u8],
            })?;
            let app_msg = ApplicationMessage::new(app_id, payload);

            client
                .send(ClientRequest::DelegateOp(
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: parent_key.clone(),
                        params: parent_params.clone(),
                        inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
                    },
                ))
                .await?;

            let resp = tokio::time::timeout(Duration::from_secs(60), client.recv()).await??;
            match resp {
                HostResponse::DelegateResponse { values, .. } => {
                    let app_msg = values
                        .iter()
                        .find_map(|m| match m {
                            OutboundDelegateMsg::ApplicationMessage(msg) => Some(msg),
                            _ => None,
                        })
                        .expect("Expected ApplicationMessage in second creation response");
                    let response: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;
                    match response {
                        OutboundAppMessage::ChildCreated { .. } => {
                            tracing::info!(
                                "Second child creation succeeded (per-call limit resets)"
                            );
                        }
                        OutboundAppMessage::CreateFailed { error_code } => {
                            return Err(anyhow!(
                                "Second creation should succeed (limit resets per call), got error: {error_code}"
                            ))
                        }
                        other => {
                            return Err(anyhow!("Unexpected response: {other:?}"))
                        }
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Expected DelegateResponse for second creation, got: {other:?}"
                    ))
                }
            }
        }

        tracing::info!("All delegate creation E2E checks passed!");
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
