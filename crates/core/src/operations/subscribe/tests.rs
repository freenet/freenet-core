use super::*;
use crate::message::Transaction;
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
