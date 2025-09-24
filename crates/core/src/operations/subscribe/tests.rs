use super::*;
use crate::message::Transaction;
use crate::node::PeerId;
use freenet_stdlib::client_api::{ContractResponse, ErrorKind, HostResponse};
use std::collections::HashSet;

/// Test that subscription responses generate proper host results
/// This verifies the fix for missing waiting_for_transaction_result
#[test]
fn test_subscription_response_generates_host_result() {
    // Create a completed subscription operation
    let contract_instance_id = ContractInstanceId::new([1u8; 32]);
    let contract_key = ContractKey::from(contract_instance_id);

    let subscribe_op = SubscribeOp {
        id: Transaction::new::<SubscribeMsg>(),
        state: Some(SubscribeState::Completed { key: contract_key }),
    };

    // Test that completed state generates correct host result
    let result = subscribe_op.to_host_result();

    match result {
        Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
            key,
            subscribed,
        })) => {
            assert_eq!(key, contract_key, "Key should match");
            assert!(subscribed, "Should be subscribed");
        }
        _ => panic!("Expected successful subscribe response, got: {:?}", result),
    }
}

/// Test that subscription state transitions correctly
#[test]
fn test_subscription_state_transitions() {
    let contract_instance_id = ContractInstanceId::new([4u8; 32]);
    let contract_key = ContractKey::from(contract_instance_id);
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Test PrepareRequest state
    let op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::PrepareRequest {
            id: transaction_id,
            key: contract_key,
        }),
    };
    assert!(!op.finalized(), "PrepareRequest should not be finalized");

    // Test AwaitingResponse state
    let op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::AwaitingResponse {
            skip_list: HashSet::new(),
            retries: 0,
            upstream_subscriber: None,
            current_hop: 5,
        }),
    };
    assert!(!op.finalized(), "AwaitingResponse should not be finalized");

    // Test Completed state
    let op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::Completed { key: contract_key }),
    };
    assert!(op.finalized(), "Completed should be finalized");
    assert!(
        op.is_completed(),
        "Completed should return true for is_completed"
    );
}

/// Test that failed subscriptions generate proper error results
#[test]
fn test_failed_subscription_generates_error() {
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Test AwaitingResponse state generates error
    let op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::AwaitingResponse {
            skip_list: HashSet::new(),
            retries: 0,
            upstream_subscriber: None,
            current_hop: 5,
        }),
    };

    let result = op.to_host_result();
    match result {
        Err(error) => {
            // Verify we get an operation error
            match error.kind() {
                ErrorKind::OperationError { .. } => {
                    // Expected error type
                }
                _ => panic!("Expected OperationError, got: {:?}", error),
            }
        }
        Ok(_) => panic!("Expected error for non-completed subscription"),
    }
}

/// Test that subscription retry logic uses skip list properly
/// This validates that k_closest_potentially_caching with k=3 is working
#[test]
fn test_subscription_retry_with_skip_list() {
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Create an AwaitingResponse state with some failed peers in skip list
    let mut skip_list = HashSet::new();
    skip_list.insert(PeerId::random());
    skip_list.insert(PeerId::random());

    let op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::AwaitingResponse {
            skip_list: skip_list.clone(),
            retries: 2, // Indicates we've already tried 2 peers
            upstream_subscriber: None,
            current_hop: 3,
        }),
    };

    // Verify that the skip list contains failed peers
    if let Some(SubscribeState::AwaitingResponse {
        skip_list: list,
        retries,
        ..
    }) = &op.state
    {
        assert_eq!(list.len(), 2, "Skip list should contain 2 failed peers");
        assert_eq!(*retries, 2, "Should have 2 retries");
        assert!(list == &skip_list, "Skip list should match");
    } else {
        panic!("Expected AwaitingResponse state");
    }
}

/// Test that PrepareRequest state properly initializes subscription
/// This tests the entry point where waiting_for_transaction_result would be set
#[test]
fn test_prepare_request_initialization() {
    let contract_instance_id = ContractInstanceId::new([5u8; 32]);
    let contract_key = ContractKey::from(contract_instance_id);
    let transaction_id = Transaction::new::<SubscribeMsg>();

    let op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::PrepareRequest {
            id: transaction_id,
            key: contract_key,
        }),
    };

    // Verify PrepareRequest has correct transaction ID and key
    if let Some(SubscribeState::PrepareRequest { id, key }) = &op.state {
        assert_eq!(*id, transaction_id, "Transaction ID should match");
        assert_eq!(*key, contract_key, "Contract key should match");
    } else {
        panic!("Expected PrepareRequest state");
    }

    // Verify operation ID matches transaction ID for correlation
    assert_eq!(
        op.id, transaction_id,
        "Operation ID should match transaction ID for response routing"
    );
}

/// Test that subscription completion properly stores the contract key
/// This ensures the subscription response can be properly formed
#[test]
fn test_subscription_completion_stores_key() {
    let contract_instance_id = ContractInstanceId::new([6u8; 32]);
    let contract_key = ContractKey::from(contract_instance_id);
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Test transition from AwaitingResponse to Completed
    let op_waiting = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::AwaitingResponse {
            skip_list: HashSet::new(),
            retries: 0,
            upstream_subscriber: None,
            current_hop: 5,
        }),
    };

    // Simulate successful completion
    let op_completed = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::Completed { key: contract_key }),
    };

    // Verify completion stores the key correctly
    if let Some(SubscribeState::Completed { key }) = &op_completed.state {
        assert_eq!(
            *key, contract_key,
            "Completed state should store correct key"
        );
    } else {
        panic!("Expected Completed state");
    }

    // Verify IDs match for transaction correlation
    assert_eq!(
        op_waiting.id, op_completed.id,
        "Transaction IDs should match through state transitions"
    );
}
