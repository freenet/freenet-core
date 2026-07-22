mod common;

use std::{net::TcpListener, time::Duration};

use anyhow::anyhow;
use freenet::server::serve_client_api;
use freenet_stdlib::{
    client_api::{ClientRequest, DelegateRequest, HostResponse},
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

/// Message types matching `test-delegate-2`'s `InboundAppMessage`
/// (tests/test-delegate-2/src/lib.rs). Variant ORDER must match the compiled
/// delegate's enum exactly, since bincode discriminants are index-based —
/// mirrors the same mirror-enum pattern used by
/// crates/core/tests/hosted_mode_migrate.rs. Only `StoreSecret` and
/// `GetNonExistentSecret` are exercised by this test; the rest are kept to
/// preserve variant ordering.
#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    CreateInboxRequest,
    PleaseSignMessage(Vec<u8>),
    WriteContext(Vec<u8>),
    ReadContext,
    ClearContext,
    IncrementCounter,
    HasSecret(Vec<u8>),
    GetNonExistentSecret(Vec<u8>),
    StoreSecret { key: Vec<u8>, value: Vec<u8> },
    RemoveSecret(Vec<u8>),
    WriteLargeContext(usize),
    StoreLargeSecret { key: Vec<u8>, size: usize },
}

/// Message types matching `test-delegate-2`'s `OutboundAppMessage`.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
enum OutboundAppMessage {
    CreateInboxResponse(Vec<u8>),
    MessageSigned(Vec<u8>),
    ContextData(Vec<u8>),
    CounterValue(u32),
    SecretExists(bool),
    SecretResult(Option<Vec<u8>>),
    ContextWritten,
    ContextCleared,
    SecretStored,
    SecretRemoved,
    LargeContextWritten(usize),
    LargeSecretStored(usize),
    SecretStoreFailed,
}

/// Sends `msg` as an `ApplicationMessages` request to `key`/`params` and
/// decodes the single `OutboundAppMessage` reply.
async fn delegate_app_msg(
    client: &mut freenet_stdlib::client_api::WebApi,
    key: &DelegateKey,
    params: &Parameters<'static>,
    msg: &InboundAppMessage,
) -> anyhow::Result<OutboundAppMessage> {
    let payload = bincode::serialize(msg)?;
    let app_msg = ApplicationMessage::new(payload);

    client
        .send(ClientRequest::DelegateOp(
            DelegateRequest::ApplicationMessages {
                key: key.clone(),
                params: params.clone(),
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
                .ok_or_else(|| anyhow!("expected an ApplicationMessage in the response"))?;
            Ok(bincode::deserialize::<OutboundAppMessage>(
                &app_msg.payload,
            )?)
        }
        other => Err(anyhow!("expected DelegateResponse, got: {other:?}")),
    }
}

/// E2E test for the delegate secret copy-forward feature (#4117).
///
/// A delegate's on-disk key is BLAKE3(code_hash || params), so any WASM
/// rebuild (or, as here, a param change simulating one) mints a new key and
/// a new, empty secret namespace. `DelegateRequest::RegisterDelegateWithPredecessors`
/// carries Local-scope secrets forward from a list of predecessor keys into
/// the newly-registered successor's namespace so `get_secret` still resolves
/// under the new key.
///
/// This test registers a "predecessor" delegate (test-delegate-2), stores a
/// Local-scope secret in it via `set_secret` (exercised through the
/// `StoreSecret` app message), then registers a "successor" delegate — same
/// WASM, different params, so a DIFFERENT `DelegateKey` — via
/// `RegisterDelegateWithPredecessors` naming the predecessor. It asserts the
/// successor can read the migrated secret under the SAME `SecretsId`, and
/// that the predecessor's own copy is untouched (no-delete invariant).
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_delegate_secret_copy_forward_e2e() -> anyhow::Result<()> {
    // Same WASM, different params => different DelegateKeys
    // (DelegateKey = BLAKE3(code_hash || params)), simulating a WASM rebuild
    // that mints a new successor key while keeping the same delegate logic.
    let pred_params = Parameters::from(vec![1u8]);
    let pred_delegate = freenet::test_utils::load_delegate("test-delegate-2", pred_params.clone())?;
    let pred_key = pred_delegate.key().clone();

    let succ_params = Parameters::from(vec![2u8]);
    let succ_delegate = freenet::test_utils::load_delegate("test-delegate-2", succ_params.clone())?;
    let succ_key = succ_delegate.key().clone();

    assert_ne!(
        pred_key, succ_key,
        "predecessor and successor must have different DelegateKeys"
    );

    const SECRET_KEY: &[u8] = b"delegate-secret-copy-forward-e2e";
    const SECRET_VALUE: &[u8] = b"secret-value-carried-across-rebuild";

    // Allocate unique IP for this test's gateway
    let base_node_idx = allocate_test_node_block(1);
    let gw_ip = test_ip_for_node(base_node_idx);

    // Reserve ports
    let network_socket_gw = TcpListener::bind(std::net::SocketAddr::new(gw_ip.into(), 0))?;
    let ws_api_port_socket_gw = TcpListener::bind(std::net::SocketAddr::new(gw_ip.into(), 0))?;

    let test_seed = *b"secret_copy_forward_e2e_testseed";
    let mut test_rng = rand::rngs::StdRng::from_seed(test_seed);

    let (config_gw, _preset_cfg_gw) = base_node_test_config_with_rng(
        true,
        vec![],
        Some(network_socket_gw.local_addr()?.port()),
        ws_api_port_socket_gw.local_addr()?.port(),
        "gw_delegate_secret_copy_forward",
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

        // Step 1: Register the predecessor delegate.
        client
            .send(ClientRequest::DelegateOp(
                DelegateRequest::RegisterDelegate {
                    delegate: pred_delegate.clone(),
                    cipher: TEST_DELEGATE_CIPHER,
                    nonce: TEST_DELEGATE_NONCE,
                },
            ))
            .await?;
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
        match resp {
            HostResponse::DelegateResponse { key, .. } => {
                assert_eq!(
                    key, pred_key,
                    "Key mismatch registering predecessor delegate"
                );
                tracing::info!("Registered predecessor delegate: {key}");
            }
            other => {
                return Err(anyhow!(
                    "Expected DelegateResponse for predecessor registration, got: {other:?}"
                ));
            }
        }

        // Step 2: Store a Local-scope secret in the predecessor (no user
        // token on this connection, so `set_secret` lands in `SecretScope::Local`
        // — the only scope the node-side copy-forward can re-encrypt, since
        // its DEK is derivable at rest from the delegate key + node KEK).
        match delegate_app_msg(
            &mut client,
            &pred_key,
            &pred_params,
            &InboundAppMessage::StoreSecret {
                key: SECRET_KEY.to_vec(),
                value: SECRET_VALUE.to_vec(),
            },
        )
        .await?
        {
            OutboundAppMessage::SecretStored => {
                tracing::info!("Predecessor stored the Local-scope secret");
            }
            other => return Err(anyhow!("Expected SecretStored, got: {other:?}")),
        }

        // Step 3: Read the secret back from the predecessor to confirm it's
        // actually stored before we exercise the migration.
        match delegate_app_msg(
            &mut client,
            &pred_key,
            &pred_params,
            &InboundAppMessage::GetNonExistentSecret(SECRET_KEY.to_vec()),
        )
        .await?
        {
            OutboundAppMessage::SecretResult(Some(value)) => {
                assert_eq!(
                    value, SECRET_VALUE,
                    "predecessor secret must read back as stored"
                );
                tracing::info!("Predecessor secret verified before migration");
            }
            other => return Err(anyhow!("Expected SecretResult(Some(..)), got: {other:?}")),
        }

        // Step 4: Register the successor delegate (different DelegateKey),
        // naming the predecessor so the node runs the one-shot secret
        // copy-forward as part of registration.
        client
            .send(ClientRequest::DelegateOp(
                DelegateRequest::RegisterDelegateWithPredecessors {
                    delegate: succ_delegate.clone(),
                    cipher: TEST_DELEGATE_CIPHER,
                    nonce: TEST_DELEGATE_NONCE,
                    predecessors: vec![pred_key.clone()],
                },
            ))
            .await?;
        let resp = tokio::time::timeout(Duration::from_secs(30), client.recv()).await??;
        match resp {
            HostResponse::DelegateResponse { key, .. } => {
                assert_eq!(key, succ_key, "Key mismatch registering successor delegate");
                tracing::info!("Registered successor delegate with predecessors: {key}");
            }
            other => {
                return Err(anyhow!(
                    "Expected DelegateResponse for successor registration, got: {other:?}"
                ));
            }
        }

        // Step 5: The successor must resolve the SAME SecretsId — the
        // predecessor's secret was copied forward into the successor's
        // namespace, re-encrypted under the successor's Local DEK.
        match delegate_app_msg(
            &mut client,
            &succ_key,
            &succ_params,
            &InboundAppMessage::GetNonExistentSecret(SECRET_KEY.to_vec()),
        )
        .await?
        {
            OutboundAppMessage::SecretResult(Some(value)) => {
                assert_eq!(
                    value, SECRET_VALUE,
                    "successor must read the migrated secret carried forward from the predecessor"
                );
                tracing::info!("Successor read the migrated secret — copy-forward verified!");
            }
            OutboundAppMessage::SecretResult(None) => {
                return Err(anyhow!(
                    "successor has no secret under the migrated SecretsId — copy-forward did not run"
                ));
            }
            other => return Err(anyhow!("Expected SecretResult, got: {other:?}")),
        }

        // Step 6: No-delete invariant — the predecessor's own copy must still
        // be readable; copy-forward only ever reads the predecessor, never
        // mutates or deletes it.
        match delegate_app_msg(
            &mut client,
            &pred_key,
            &pred_params,
            &InboundAppMessage::GetNonExistentSecret(SECRET_KEY.to_vec()),
        )
        .await?
        {
            OutboundAppMessage::SecretResult(Some(value)) => {
                assert_eq!(
                    value, SECRET_VALUE,
                    "predecessor secret must remain intact after copy-forward (no-delete invariant)"
                );
                tracing::info!("Predecessor secret confirmed intact after copy-forward");
            }
            other => {
                return Err(anyhow!(
                    "Expected predecessor SecretResult(Some(..)) to remain after copy-forward, got: {other:?}"
                ));
            }
        }

        tracing::info!("Delegate secret copy-forward E2E checks passed!");
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
