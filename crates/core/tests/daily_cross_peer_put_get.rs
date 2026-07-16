#![cfg(feature = "nightly_tests")]

use anyhow::bail;
use freenet::test_utils::{
    self, TestContext, create_large_todo_list, load_contract, make_get, make_put,
    verify_contract_exists,
};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use std::path::Path;
use std::time::Duration;
use tokio_tungstenite::connect_async;

const NUM_SMALL_CONTRACTS: usize = 3;
const SHORT_DELAY_SECS: u64 = 5;
const RETENTION_DELAY_SECS: u64 = 30;
const GET_TIMEOUT_SECS: u64 = 120;
const MAX_GET_ATTEMPTS: usize = 3;
const MAX_RESPONSES_PER_ATTEMPT: usize = 20;

async fn get_contract_with_retry(
    client: &mut WebApi,
    key: ContractKey,
    temp_dir: impl AsRef<Path>,
) -> anyhow::Result<(ContractContainer, WrappedState)> {
    for attempt in 1..=MAX_GET_ATTEMPTS {
        tracing::info!("GET attempt {attempt}/{MAX_GET_ATTEMPTS} for key {key}");
        make_get(client, key, true, false).await?;

        for _ in 0..MAX_RESPONSES_PER_ATTEMPT {
            let resp =
                tokio::time::timeout(Duration::from_secs(GET_TIMEOUT_SECS), client.recv()).await;
            match resp {
                Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    key: resp_key,
                    contract: Some(contract),
                    state,
                }))) => {
                    verify_contract_exists(temp_dir.as_ref(), resp_key).await?;
                    return Ok((contract, state));
                }
                Ok(Ok(other)) => {
                    tracing::warn!("unexpected response while waiting for GET: {:?}", other);
                }
                Ok(Err(e)) => {
                    tracing::warn!("GET attempt {attempt} error: {}", e);
                    break;
                }
                Err(_) => {
                    tracing::warn!("GET attempt {attempt} timed out");
                    break;
                }
            }
        }

        if attempt == MAX_GET_ATTEMPTS {
            bail!("GET contract {key} failed after {MAX_GET_ATTEMPTS} attempts");
        }

        loop {
            match tokio::time::timeout(Duration::from_millis(200), client.recv()).await {
                Ok(Ok(resp)) => {
                    tracing::warn!("Discarding stray response before GET retry: {:?}", resp);
                }
                Ok(Err(err)) => {
                    tracing::warn!("Discarding stray error before GET retry: {}", err);
                }
                Err(_) => break,
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    unreachable!("get_contract_with_retry loop should always return or bail");
}

async fn put_and_wait(
    client: &mut WebApi,
    state: WrappedState,
    contract: ContractContainer,
    description: &str,
) -> anyhow::Result<ContractKey> {
    let expected_key = contract.key();
    tracing::info!(
        "PUTting {description} (state size: {} bytes)",
        state.as_ref().len()
    );
    make_put(client, state, contract, false).await?;

    match tokio::time::timeout(Duration::from_secs(GET_TIMEOUT_SECS), client.recv()).await {
        Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
            assert_eq!(key, expected_key, "PUT key mismatch for {description}");
            tracing::info!("PUT {description} succeeded (key: {key})");
            Ok(key)
        }
        Ok(Ok(other)) => {
            bail!("PUT {description}: unexpected response: {:?}", other);
        }
        Ok(Err(e)) => {
            bail!("PUT {description}: error receiving response: {}", e);
        }
        Err(_) => {
            bail!("PUT {description}: timed out after {GET_TIMEOUT_SECS}s");
        }
    }
}

/// Automated daily network test: PUT contracts via one peer, GET them via another.
///
/// Exercises the core end-to-end invariant: one peer PUTs several contracts
/// (including a ~1 MB payload), and a DIFFERENT peer elsewhere in the network
/// GETs them, verifying they are retrievable. This catches findability/retention
/// regressions that unit and in-process sim tests can miss.
///
/// Ref: <https://github.com/freenet/freenet-core/issues/4665>
#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway", "peer-a"],
    timeout_secs = 900,
    startup_wait_secs = 30,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
    aggregate_events = "always"
)]
async fn test_cross_peer_put_get_retention(ctx: &mut TestContext) -> TestResult {
    test_utils::ensure_contract_compiled("test-contract-integration")?;

    let peer_a = ctx.node("peer-a")?;
    let gateway = ctx.node("gateway")?;

    tracing::info!("peer-a ws_port: {}", peer_a.ws_port);
    tracing::info!("gateway ws_port: {}", gateway.ws_port);

    let (stream_a, _) = connect_async(&peer_a.ws_url()).await?;
    let mut client_a = WebApi::start(stream_a);

    let mut contract_keys: Vec<(ContractKey, String)> = Vec::new();

    for i in 0..NUM_SMALL_CONTRACTS {
        let params = Parameters::from(format!("daily-test-small-{i}").into_bytes());
        let contract = load_contract("test-contract-integration", params)?;
        let state = test_utils::create_todo_list_with_item(&format!("small-contract-{i}"));
        let wrapped = WrappedState::from(state);

        let key = put_and_wait(
            &mut client_a,
            wrapped,
            contract,
            &format!("small contract {i}/{}", NUM_SMALL_CONTRACTS),
        )
        .await?;

        contract_keys.push((key, format!("small-{i}")));
    }

    let large_params = Parameters::from(b"daily-test-large-payload".to_vec());
    let large_contract = load_contract("test-contract-integration", large_params)?;
    let large_state = create_large_todo_list();
    let large_state_size = large_state.len();
    let large_wrapped = WrappedState::from(large_state);

    let large_key = put_and_wait(
        &mut client_a,
        large_wrapped,
        large_contract,
        "large (~1MB) contract",
    )
    .await?;

    contract_keys.push((large_key, "large-1MB".to_string()));

    tracing::info!(
        "All {} contracts PUT via peer-a. Waiting {SHORT_DELAY_SECS}s before cross-peer GET...",
        contract_keys.len()
    );
    tokio::time::sleep(Duration::from_secs(SHORT_DELAY_SECS)).await;

    let (stream_b, _) = connect_async(&gateway.ws_url()).await?;
    let mut client_b = WebApi::start(stream_b);

    tracing::info!("Phase 1: GET each contract via gateway (cross-peer)");
    for (key, label) in &contract_keys {
        tracing::info!("GET {label} ({key}) via gateway...");
        let (resp_contract, resp_state) =
            get_contract_with_retry(&mut client_b, *key, &gateway.temp_dir_path).await?;

        assert_eq!(
            resp_contract.key(),
            *key,
            "GET returned wrong contract key for {label}"
        );
        tracing::info!(
            "GET {label} succeeded (state size: {} bytes)",
            resp_state.as_ref().len()
        );
    }

    tracing::info!("Phase 1 complete. Waiting {RETENTION_DELAY_SECS}s for retention check...");
    tokio::time::sleep(Duration::from_secs(RETENTION_DELAY_SECS)).await;

    tracing::info!("Phase 2: GET each contract again via gateway (retention over time)");
    for (key, label) in &contract_keys {
        tracing::info!("Retention GET {label} ({key}) via gateway...");
        let (resp_contract, resp_state) =
            get_contract_with_retry(&mut client_b, *key, &gateway.temp_dir_path).await?;

        assert_eq!(
            resp_contract.key(),
            *key,
            "Retention GET returned wrong contract key for {label}"
        );

        if label == "large-1MB" {
            assert!(
                resp_state.as_ref().len() >= large_state_size / 2,
                "Retention GET for {label}: state size {} is suspiciously small (original: {large_state_size})",
                resp_state.as_ref().len()
            );
        }

        tracing::info!(
            "Retention GET {label} succeeded (state size: {} bytes)",
            resp_state.as_ref().len()
        );
    }

    tracing::info!(
        "All {} contracts retrievable cross-peer after {}s retention delay",
        contract_keys.len(),
        RETENTION_DELAY_SECS
    );

    client_a
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    client_b
        .send(ClientRequest::Disconnect { cause: None })
        .await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}
