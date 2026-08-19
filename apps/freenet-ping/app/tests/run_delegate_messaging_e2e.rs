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
    TEST_DELEGATE_CIPHER, TEST_DELEGATE_NONCE, allocate_test_node_block,
    base_node_test_config_with_rng, connect_ws_with_retry, test_ip_for_node, test_node_config,
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
    /// Appended in lockstep with the fixture. Variants MUST stay in the same
    /// order in both copies: bincode encodes the discriminant positionally.
    VerifySecret {
        expected: Vec<u8>,
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
    /// Appended in lockstep with the fixture — see the note above.
    SecretStored {
        byte_count: u64,
        sender_key_bytes: Vec<u8>,
        origin_delegate_key_bytes: Option<Vec<u8>>,
    },
    SecretVerified {
        present: bool,
        matches: bool,
    },
}

/// Does `haystack` contain `needle` as a contiguous subsequence?
///
/// Used to scan RAW returned bytes rather than decoded messages, so a leak is
/// caught regardless of how the leaking delegate happened to frame it.
fn contains_subsequence(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// E2E test for delegate-to-delegate messaging over WebSocket.
///
/// Registers two instances of test-delegate-messaging (with different params
/// to get different DelegateKeys), sends a SendToDelegate command to A targeting B,
/// and verifies the runtime delivers to B and returns B's output.
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_delegate_to_delegate_messaging_e2e() -> anyhow::Result<()> {
    // SAFETY: test environment, no concurrent threads reading these env vars
    unsafe {
        std::env::set_var("CARGO_PROFILE_RELEASE_LTO", "true");
        std::env::set_var("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");
        std::env::set_var("CARGO_PROFILE_RELEASE_STRIP", "true");
    }

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
                    cipher: TEST_DELEGATE_CIPHER,
                    nonce: TEST_DELEGATE_NONCE,
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
                    cipher: TEST_DELEGATE_CIPHER,
                    nonce: TEST_DELEGATE_NONCE,
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
            let payload = bincode::serialize(&InboundAppMessage::Ping {
                data: b"hello".to_vec(),
            })?;
            let app_msg = ApplicationMessage::new(payload);

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
                    ));
                }
            }
        }

        // Step 2: Send delegate-to-delegate message from A to B
        {
            let payload = bincode::serialize(&InboundAppMessage::SendToDelegate {
                target_key_bytes: key_b.bytes().to_vec(),
                target_code_hash: key_b.code_hash().as_ref().to_vec(),
                payload: b"inter-delegate-e2e".to_vec(),
            })?;
            let app_msg = ApplicationMessage::new(payload);

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
                            ));
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

                    // NEGATIVE CONTROL for step 3, and the reason step 3 means
                    // anything. B echoed its received payload, and that payload
                    // is reachable in the RAW bytes returned to this client. If
                    // this assertion ever fails, the observer below is blind and
                    // step 3's silence proves nothing.
                    let leaked = values.iter().any(|m| match m {
                        OutboundDelegateMsg::ApplicationMessage(msg) => {
                            contains_subsequence(&msg.payload, b"inter-delegate-e2e")
                        }
                        _ => false,
                    });
                    assert!(
                        leaked,
                        "NEGATIVE CONTROL FAILED: an echoing receiver's payload did not reach \
                         the driving client, so this harness cannot observe a leak and the \
                         no-leak assertion in step 3 would pass vacuously"
                    );
                }
                other => {
                    return Err(anyhow!(
                        "Expected DelegateResponse for send-to-delegate, got: {other:?}"
                    ));
                }
            }
        }

        // Step 3: the same hop, but the receiver DEPOSITS instead of echoing.
        //
        // This is the shape delegate succession relies on: the predecessor pushes
        // secrets to its successor, the successor stores them via `set_secret`
        // and reports a count only. Nothing carrying the payload may reach the
        // client driving the sender.
        let secret = b"succession-secret-must-not-leak".to_vec();
        {
            let mut deposit = b"SECRET:".to_vec();
            deposit.extend_from_slice(&secret);

            let payload = bincode::serialize(&InboundAppMessage::SendToDelegate {
                target_key_bytes: key_b.bytes().to_vec(),
                target_code_hash: key_b.code_hash().as_ref().to_vec(),
                payload: deposit,
            })?;
            let app_msg = ApplicationMessage::new(payload);

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
                    // THE SECURITY ASSERTION. Scan raw bytes, not decoded
                    // messages, so a leak is caught however it was framed.
                    for m in values.iter() {
                        if let OutboundDelegateMsg::ApplicationMessage(msg) = m {
                            assert!(
                                !contains_subsequence(&msg.payload, &secret),
                                "LEAK: deposited secret bytes reached the driving client"
                            );
                        }
                    }

                    let stored = values
                        .iter()
                        .filter_map(|m| match m {
                            OutboundDelegateMsg::ApplicationMessage(msg) => {
                                bincode::deserialize::<OutboundAppMessage>(&msg.payload).ok()
                            }
                            _ => None,
                        })
                        .find(|m| matches!(m, OutboundAppMessage::SecretStored { .. }));

                    match stored {
                        Some(OutboundAppMessage::SecretStored {
                            byte_count,
                            sender_key_bytes,
                            origin_delegate_key_bytes,
                        }) => {
                            assert_eq!(
                                byte_count as usize,
                                secret.len(),
                                "Receiver reported the wrong deposited length"
                            );
                            assert_eq!(
                                sender_key_bytes,
                                key_a.bytes(),
                                "Deposit should be attributed to delegate A"
                            );
                            assert_eq!(
                                origin_delegate_key_bytes.as_deref(),
                                Some(key_a.bytes()),
                                "Runtime-attested origin should be delegate A"
                            );
                        }
                        _ => return Err(anyhow!("Expected SecretStored from delegate B")),
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Expected DelegateResponse for deposit, got: {other:?}"
                    ));
                }
            }
        }

        // Step 4: prove the deposit actually landed.
        //
        // Without this, step 3 would pass for a receiver that silently discarded
        // the payload. B compares internally and returns booleans only, so this
        // check cannot itself become the leak it is guarding.
        {
            let payload = bincode::serialize(&InboundAppMessage::VerifySecret {
                expected: secret.clone(),
            })?;
            let app_msg = ApplicationMessage::new(payload);

            client
                .send(ClientRequest::DelegateOp(
                    freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: key_b.clone(),
                        params: params_b.clone(),
                        inbound: vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
                    },
                ))
                .await?;

            let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
            match resp {
                HostResponse::DelegateResponse { values, .. } => {
                    let verified = values
                        .iter()
                        .filter_map(|m| match m {
                            OutboundDelegateMsg::ApplicationMessage(msg) => {
                                bincode::deserialize::<OutboundAppMessage>(&msg.payload).ok()
                            }
                            _ => None,
                        })
                        .find(|m| matches!(m, OutboundAppMessage::SecretVerified { .. }));

                    match verified {
                        Some(OutboundAppMessage::SecretVerified { present, matches }) => {
                            assert!(present, "Delegate B holds no deposited secret");
                            assert!(matches, "Delegate B's deposited secret does not match");
                        }
                        _ => return Err(anyhow!("Expected SecretVerified from delegate B")),
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Expected DelegateResponse for verify, got: {other:?}"
                    ));
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
