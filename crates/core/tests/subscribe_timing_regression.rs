#![cfg(feature = "test-network")]
//! Reproduction test for subscription timeout issue (Issue #2494)
//!
//! This test demonstrates the PUT/SUBSCRIBE race condition where
//! a subscription arrives before contract propagation completes,
//! causing fallback to random peer routing and multi-second delays.
//!
//! Run with:
//!   cargo test --test subscribe_timing_regression -- --nocapture

use freenet_stdlib::client_api::{ClientRequest, ContractRequest, HostResponse, WebApi};
use freenet_stdlib::prelude::*;
use freenet_test_network::TestNetwork;
use std::time::{Duration, Instant};
use testresult::TestResult;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;

/// Load the test contract used in operations.rs
fn make_test_contract(seed: u8) -> (ContractContainer, State) {
    // Create a simple contract with unique seed to avoid caching between tests
    let code = ContractCode::from(vec![seed, 2, 3, 4, 5]);
    let params = Parameters::from(vec![seed]);
    let state = State::from(vec![seed, 6, 7, 8]);

    let contract = ContractContainer::Wasm(WasmAPIVersion::V1(WasmContractV1 {
        data: code.data().to_vec().into(),
        params: params.clone().into(),
    }));

    (contract, state)
}

#[test_log::test(tokio::test)]
async fn test_put_subscribe_race_condition_localhost() -> TestResult {
    tracing::info!("=== Testing PUT/SUBSCRIBE race condition on localhost ===");
    tracing::info!("This test uses TestNetwork without Docker to isolate the race condition");

    // Create a small network: 1 gateway + 5 peers (matching six-peer-regression)
    tracing::info!("Starting network (1 gateway + 5 peers)...");
    let network = TestNetwork::builder()
        .gateways(1)
        .peers(5)
        .binary(freenet_test_network::FreenetBinary::CurrentCrate(
            freenet_test_network::BuildProfile::Debug,
        ))
        .build()
        .await?;

    tracing::info!("Network started, connecting to gateway...");

    // Connect to gateway
    let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let (stream, _) = connect_async(&gw_url).await?;
    let mut client = WebApi::start(stream);

    // ========== SCENARIO 1: Immediate SUBSCRIBE (will race) ==========
    tracing::info!("\n=== SCENARIO 1: PUT then IMMEDIATE subscribe ===");

    let (contract1, state1) = make_test_contract(1);
    let key1 = contract1.key();

    tracing::info!("Contract 1 key: {}", key1.id());

    // PUT
    let start = Instant::now();
    let put_req = ClientRequest::ContractOp(ContractRequest::Put {
        contract: contract1.clone(),
        state: state1.clone(),
        related_contracts: RelatedContracts::default(),
    });

    client.send(put_req).await?;
    let put_response = client.recv().await?;
    let put_duration = start.elapsed();

    tracing::info!("PUT completed in {:?}", put_duration);

    match put_response {
        HostResponse::ContractResponse(response) => {
            tracing::info!("PUT response: {:?}", response);
        }
        other => {
            tracing::error!("Unexpected PUT response: {:?}", other);
        }
    }

    // IMMEDIATELY subscribe (before propagation completes)
    let sub_start = Instant::now();
    let sub_req = ClientRequest::ContractOp(ContractRequest::Subscribe {
        key: key1.clone(),
        summary: None,
    });

    tracing::info!("Sending SUBSCRIBE immediately after PUT (t={}ms)", start.elapsed().as_millis());
    client.send(sub_req).await?;
    let sub_response = client.recv().await?;
    let sub_duration = sub_start.elapsed();

    tracing::info!("SUBSCRIBE completed in {:?}", sub_duration);
    tracing::info!("Response: {:?}", sub_response);

    // ========== SCENARIO 2: Delayed SUBSCRIBE (should be fast) ==========
    tracing::info!("\n=== SCENARIO 2: PUT then DELAYED subscribe (10s wait) ===");

    let (contract2, state2) = make_test_contract(2);
    let key2 = contract2.key();

    tracing::info!("Contract 2 key: {}", key2.id());

    // PUT
    let put_req2 = ClientRequest::ContractOp(ContractRequest::Put {
        contract: contract2.clone(),
        state: state2.clone(),
        related_contracts: RelatedContracts::default(),
    });

    client.send(put_req2).await?;
    let put_response2 = client.recv().await?;

    tracing::info!("PUT 2 completed: {:?}", put_response2);

    // WAIT for propagation
    tracing::info!("Waiting 10s for PUT propagation and network stabilization...");
    sleep(Duration::from_secs(10)).await;

    // NOW subscribe - should be instant
    let sub_start2 = Instant::now();
    let sub_req2 = ClientRequest::ContractOp(ContractRequest::Subscribe {
        key: key2,
        summary: None,
    });

    tracing::info!("Sending SUBSCRIBE after 10s delay");
    client.send(sub_req2).await?;
    let sub_response2 = client.recv().await?;
    let sub_duration2 = sub_start2.elapsed();

    tracing::info!("SUBSCRIBE 2 completed in {:?}", sub_duration2);
    tracing::info!("Response: {:?}", sub_response2);

    // ========== ANALYSIS ==========
    tracing::info!("\n=== ANALYSIS ===");
    tracing::info!("Immediate subscribe: {:?}", sub_duration);
    tracing::info!("Delayed subscribe:   {:?}", sub_duration2);

    if sub_duration2.as_millis() > 0 {
        let speedup = sub_duration.as_millis() as f64 / sub_duration2.as_millis() as f64;
        tracing::info!("Speedup ratio: {:.2}x", speedup);

        if speedup > 5.0 {
            tracing::error!("\n❌ PUT/SUBSCRIBE RACE CONDITION CONFIRMED");
            tracing::error!("   Immediate subscribe is {:.1}x slower than delayed", speedup);
            tracing::error!("   Root cause: k_closest_potentially_caching returns empty");
            tracing::error!("   → Falls back to random peer");
            tracing::error!("   → Random peer doesn't have contract");
            tracing::error!("   → 2s contract wait + retries");
            tracing::error!("\n   See CODE_ANALYSIS_SUBSCRIPTION_ISSUE.md for details");
        } else if sub_duration > Duration::from_secs(5) {
            tracing::warn!("\n⚠️  Immediate subscribe took {:?} (>5s)", sub_duration);
            tracing::warn!("   But delayed subscribe also slow - may be different issue");
        } else {
            tracing::info!("\n✓ No significant race condition detected");
            tracing::info!("   Both scenarios completed quickly");
        }
    }

    Ok(())
}

/// Test with detailed logging to see routing decisions
///
/// Run with:
///   RUST_LOG=debug cargo test --test subscribe_timing_regression routing_visibility -- --ignored --nocapture
#[test_log::test(tokio::test)]
#[ignore]
async fn test_subscription_routing_visibility() -> TestResult {
    tracing::info!("=== Testing subscription routing with detailed logging ===");
    tracing::info!("Watch for 'Using fallback connection' or 'contract_wait' messages");

    let network = TestNetwork::builder()
        .gateways(1)
        .peers(5)
        .build()
        .await?;

    // Give network minimal time to start
    sleep(Duration::from_secs(2)).await;

    let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let (stream, _) = connect_async(&gw_url).await?;
    let mut client = WebApi::start(stream);

    let (contract, state) = make_test_contract(99);
    let key = contract.key();

    // PUT
    tracing::info!("Sending PUT...");
    let put_req = ClientRequest::ContractOp(ContractRequest::Put {
        contract: contract.clone(),
        state: state.clone(),
        related_contracts: RelatedContracts::default(),
    });

    client.send(put_req).await?;
    let _ = client.recv().await?;

    // IMMEDIATELY subscribe to trigger the race
    tracing::info!("Sending SUBSCRIBE immediately after PUT...");
    let sub_req = ClientRequest::ContractOp(ContractRequest::Subscribe {
        key,
        summary: None,
    });

    let start = Instant::now();
    client.send(sub_req).await?;
    let result = client.recv().await?;
    let duration = start.elapsed();

    tracing::info!("\n=== SUBSCRIBE completed in {:?} ===", duration);
    tracing::info!("Result: {:?}", result);

    tracing::info!("\n=== Check the logs above for: ===");
    tracing::info!("1. 'Using fallback connection' - indicates k_closest returned empty");
    tracing::info!("2. 'phase=\"contract_wait' - indicates 2s contract wait triggered");
    tracing::info!("3. Retry attempts with backoff delays");

    Ok(())
}
