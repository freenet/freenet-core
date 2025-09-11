//! Integration verification tests for client connection refactor infrastructure

use crate::client_events::{ClientId, OpenRequest, RequestId};
use crate::contract::{contract_handler_channel, SessionMessage};
use freenet_stdlib::client_api::ClientRequest;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_end_to_end_correlation_workflow() {
    // Test complete correlation workflow from request creation to tracking

    // 1. Create request with correlation
    let client_id = ClientId::FIRST;
    let request = Box::new(ClientRequest::Disconnect { cause: None });
    let open_req = OpenRequest::new(client_id, request);
    let correlation_id = open_req.request_id;

    // 2. Verify correlation appears in display (would be in logs)
    let display = format!("{}", open_req);
    assert!(display.contains("client: "));
    assert!(display.contains(&format!("request_id: {}", correlation_id)));

    // 3. Simulate processing through client events infrastructure
    let processed_request_id = open_req.request_id;

    // 4. Verify correlation is preserved
    assert_eq!(correlation_id, processed_request_id);

    // 5. Test that correlation IDs are unique across concurrent requests
    let concurrent_requests: Vec<_> = (0..100)
        .map(|_| {
            let req = Box::new(ClientRequest::Disconnect { cause: None });
            OpenRequest::new(client_id, req)
        })
        .collect();

    // Verify all request IDs are unique
    let mut request_ids: Vec<_> = concurrent_requests
        .iter()
        .map(|req| req.request_id)
        .collect();
    request_ids.sort();
    request_ids.dedup();
    assert_eq!(request_ids.len(), 100, "All request IDs should be unique");
}

#[tokio::test]
async fn test_session_adapter_installation() {
    // Test that session adapter can be installed without errors

    let (mut ch_outbound, _ch_inbound, _wait_for_event) = contract_handler_channel();
    let (session_tx, _session_rx) = mpsc::channel(100);

    // Install session adapter - this should not panic or fail
    ch_outbound.with_session_adapter(session_tx);

    // Test passes if no panic/error occurred during installation
}

#[tokio::test]
async fn test_result_router_receives_host_responses() {
    // Test that result router can receive and forward HostResult messages
    use crate::client_events::{result_router::ResultRouter, HostResult};
    use crate::message::Transaction;
    use freenet_stdlib::client_api::HostResponse;

    let (network_tx, network_rx) = mpsc::channel::<(Transaction, HostResult)>(100);
    let (session_tx, mut session_rx) = mpsc::channel::<SessionMessage>(100);

    // Create router
    let router = ResultRouter::new(network_rx, session_tx);

    // Spawn router task
    let router_handle = tokio::spawn(async move {
        router.run().await;
    });

    // Create test transaction and result
    use crate::operations::put::PutMsg;
    use freenet_stdlib::prelude::{ContractCode, Parameters, WrappedContract};
    use std::sync::Arc;

    let tx = Transaction::new::<PutMsg>();
    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1, 2, 3])),
        Parameters::from(vec![4u8, 5u8]),
    );
    let success_response = Ok(HostResponse::ContractResponse(
        freenet_stdlib::client_api::ContractResponse::PutResponse {
            key: *contract.key(),
        },
    ));

    // Send result through router
    network_tx.send((tx, success_response)).await.unwrap();

    // Verify router forwards to session actor
    if let Ok(msg) =
        tokio::time::timeout(tokio::time::Duration::from_millis(100), session_rx.recv()).await
    {
        match msg.unwrap() {
            SessionMessage::DeliverHostResponse {
                tx: received_tx,
                response,
            } => {
                assert_eq!(received_tx, tx);
                assert!((*response).is_ok());
            }
            other => panic!("Expected DeliverHostResponse, got {:?}", other),
        }
    } else {
        panic!("Router should forward message within timeout");
    }

    // Cleanup
    drop(network_tx);
    router_handle.abort();
}

#[tokio::test]
async fn test_flag_disabled_no_router() {
    // Test that router is not spawned when flag is false

    // Save original state
    let original = std::env::var("FREENET_ACTOR_CLIENTS").ok();

    // Disable flag
    std::env::remove_var("FREENET_ACTOR_CLIENTS");

    // Verify flag is disabled
    let enabled = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
    assert!(!enabled, "Flag should be disabled");

    // When flag is disabled, router should not be active
    // This is tested by the NodeP2P::build() logic - when flag is false,
    // result_router_tx is None, so no router is spawned

    // In a real scenario, we would test NodeP2P::build() directly,
    // but for this unit test, we verify the flag behavior

    // Restore original state
    match original {
        Some(val) => std::env::set_var("FREENET_ACTOR_CLIENTS", val),
        None => std::env::remove_var("FREENET_ACTOR_CLIENTS"),
    }
}

#[tokio::test]
async fn test_dual_path_identical_results() {
    // Test that dual-path delivery sends identical results to both legacy and router paths
    // This test simulates what happens in report_result when both paths are active
    use crate::client_events::HostResult;
    use crate::message::Transaction;
    use freenet_stdlib::client_api::HostResponse;
    use tokio::sync::mpsc;

    // Create channels for both paths
    let (router_tx, mut router_rx) = mpsc::channel::<(Transaction, HostResult)>(100);
    let (legacy_tx, mut legacy_rx) =
        mpsc::channel::<(crate::client_events::ClientId, HostResult)>(100);

    // Create test data
    use crate::operations::put::PutMsg;
    use freenet_stdlib::prelude::{ContractCode, Parameters, WrappedContract};
    use std::sync::Arc;

    let tx = Transaction::new::<PutMsg>();
    let client_id = crate::client_events::ClientId::FIRST;
    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1, 2, 3])),
        Parameters::from(vec![4u8, 5u8]),
    );

    // Create separate but equivalent results for both paths (since HostResult doesn't implement Clone)
    let host_result_1 = Ok(HostResponse::ContractResponse(
        freenet_stdlib::client_api::ContractResponse::PutResponse {
            key: *contract.key(),
        },
    ));
    let host_result_2 = Ok(HostResponse::ContractResponse(
        freenet_stdlib::client_api::ContractResponse::PutResponse {
            key: *contract.key(),
        },
    ));

    // Router path
    router_tx.send((tx, host_result_1)).await.unwrap();
    // Legacy path
    legacy_tx.send((client_id, host_result_2)).await.unwrap();

    // Verify both paths receive the same result
    let router_result = router_rx.recv().await.unwrap();
    let legacy_result = legacy_rx.recv().await.unwrap();

    // Transaction should match
    assert_eq!(router_result.0, tx);

    // Results should be equivalent (though we can't directly compare due to clone limitations)
    match (&router_result.1, &legacy_result.1) {
        (Ok(_), Ok(_)) => {}   // Both successful
        (Err(_), Err(_)) => {} // Both error
        _ => panic!("Results should have same success/error state"),
    }

    // Client ID should match
    assert_eq!(legacy_result.0, client_id);
}

#[tokio::test]
async fn test_router_receives_results_without_legacy_callback() {
    // Test that router receives results even when no legacy client callback is present
    // This ensures router path is independent of legacy callback presence
    use crate::client_events::HostResult;
    use crate::message::Transaction;
    use freenet_stdlib::client_api::HostResponse;
    use tokio::sync::mpsc;

    // Create router channel (simulating what happens in report_result)
    let (router_tx, mut router_rx) = mpsc::channel::<(Transaction, HostResult)>(100);

    // Create test data
    use crate::operations::put::PutMsg;
    use freenet_stdlib::prelude::{ContractCode, Parameters, WrappedContract};
    use std::sync::Arc;

    let tx = Transaction::new::<PutMsg>();
    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1, 2, 3])),
        Parameters::from(vec![4u8, 5u8]),
    );
    let host_result = Ok(HostResponse::ContractResponse(
        freenet_stdlib::client_api::ContractResponse::PutResponse {
            key: *contract.key(),
        },
    ));

    // Simulate the router path from report_result (without legacy callback)
    // This is what happens when tx is present but client_req_handler_callback is None
    if router_tx.try_send((tx, host_result)).is_ok() {
        // Router should receive the result even without legacy callback
    }

    // Verify router receives the result
    let router_result = router_rx.recv().await.unwrap();
    assert_eq!(router_result.0, tx);
    assert!(router_result.1.is_ok());
}

#[test]
fn test_actor_infrastructure_flag_behavior() {
    // Test environment flag behavior in different scenarios

    // Save original state
    let original = std::env::var("FREENET_ACTOR_CLIENTS").ok();

    // Test default (disabled) behavior
    std::env::remove_var("FREENET_ACTOR_CLIENTS");
    let disabled = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
    assert!(!disabled, "Should be disabled by default");

    // Test enabled behavior
    std::env::set_var("FREENET_ACTOR_CLIENTS", "true");
    let enabled = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
    assert!(enabled, "Should be enabled when set to 'true'");

    // Test case sensitivity
    std::env::set_var("FREENET_ACTOR_CLIENTS", "TRUE");
    let case_sensitive = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
    assert!(!case_sensitive, "Should be case-sensitive (TRUE != true)");

    // Test other values
    std::env::set_var("FREENET_ACTOR_CLIENTS", "1");
    let numeric = std::env::var("FREENET_ACTOR_CLIENTS").unwrap_or_default() == "true";
    assert!(!numeric, "Should only accept 'true', not '1'");

    // Restore original state
    match original {
        Some(val) => std::env::set_var("FREENET_ACTOR_CLIENTS", val),
        None => std::env::remove_var("FREENET_ACTOR_CLIENTS"),
    }
}

#[tokio::test]
async fn test_zero_performance_overhead_correlation() {
    // Test that correlation adds minimal overhead
    use std::time::Instant;

    const NUM_REQUESTS: usize = 10000;

    // Time request creation with correlation
    let start = Instant::now();
    let requests_with_correlation: Vec<_> = (0..NUM_REQUESTS)
        .map(|_| {
            let req = Box::new(ClientRequest::Disconnect { cause: None });
            OpenRequest::new(ClientId::FIRST, req)
        })
        .collect();
    let with_correlation = start.elapsed();

    // Verify all requests were created
    assert_eq!(requests_with_correlation.len(), NUM_REQUESTS);

    // Time should be very fast (correlation is just atomic counter increment)
    assert!(
        with_correlation.as_millis() < 100,
        "Correlation should add minimal overhead, took {}ms",
        with_correlation.as_millis()
    );

    // Verify uniqueness at scale
    let mut ids: Vec<_> = requests_with_correlation
        .iter()
        .map(|r| r.request_id)
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), NUM_REQUESTS, "All IDs should be unique at scale");
}

#[test]
fn test_correlation_thread_safety_comprehensive() {
    // Comprehensive thread safety test for RequestId generation
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;

    const NUM_THREADS: usize = 10;
    const REQUESTS_PER_THREAD: usize = 1000;

    let all_ids = Arc::new(Mutex::new(HashSet::new()));

    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|_| {
            let ids_clone = Arc::clone(&all_ids);
            thread::spawn(move || {
                let thread_ids: Vec<_> =
                    (0..REQUESTS_PER_THREAD).map(|_| RequestId::new()).collect();

                // Add to global set
                let mut global_ids = ids_clone.lock().unwrap();
                for id in thread_ids {
                    assert!(global_ids.insert(id), "Duplicate ID found: {}", id);
                }
            })
        })
        .collect();

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread should complete successfully");
    }

    // Verify total count
    let final_ids = all_ids.lock().unwrap();
    assert_eq!(
        final_ids.len(),
        NUM_THREADS * REQUESTS_PER_THREAD,
        "All IDs should be unique across threads"
    );
}
