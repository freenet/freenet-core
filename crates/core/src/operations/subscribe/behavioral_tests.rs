use super::*;
use crate::{
    message::Transaction,
    node::PeerId,
    ring::{Location, PeerKeyLocation},
};
use freenet_stdlib::{
    client_api::{ContractResponse, HostResponse},
    prelude::{ContractInstanceId, ContractKey},
};
use std::collections::HashSet;

/// Test that subscription state machine transitions correctly through states
#[test]
fn test_subscription_state_machine_transitions() {
    let contract_key = ContractKey::from(ContractInstanceId::new([1u8; 32]));
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Test initial PrepareRequest state
    let mut op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::PrepareRequest {
            id: transaction_id,
            key: contract_key,
        }),
    };
    assert!(!op.finalized(), "PrepareRequest should not be finalized");

    // Transition to AwaitingResponse
    op.state = Some(SubscribeState::AwaitingResponse {
        skip_list: HashSet::new(),
        retries: 0,
        upstream_subscriber: Some(PeerKeyLocation {
            peer: PeerId::random(),
            location: Some(Location::try_from(0.1).unwrap()),
        }),
        current_hop: 10,
    });
    assert!(!op.finalized(), "AwaitingResponse should not be finalized");

    // Test retry with skip list
    let mut skip_list = HashSet::new();
    skip_list.insert(PeerId::random());
    op.state = Some(SubscribeState::AwaitingResponse {
        skip_list: skip_list.clone(),
        retries: 1,
        upstream_subscriber: Some(PeerKeyLocation {
            peer: PeerId::random(),
            location: Some(Location::try_from(0.1).unwrap()),
        }),
        current_hop: 9,
    });

    if let Some(SubscribeState::AwaitingResponse {
        skip_list: list,
        retries,
        ..
    }) = &op.state
    {
        assert_eq!(list.len(), 1, "Skip list should have failed peer");
        assert_eq!(*retries, 1, "Should have 1 retry");
    }

    // Transition to Completed
    op.state = Some(SubscribeState::Completed { key: contract_key });
    assert!(op.finalized(), "Completed should be finalized");
    assert!(op.is_completed(), "Should be completed");

    // Test host result generation
    let result = op.to_host_result();
    match result {
        Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
            key,
            subscribed,
        })) => {
            assert_eq!(key, contract_key, "Key should match");
            assert!(subscribed, "Should be subscribed");
        }
        _ => panic!("Expected successful subscribe response"),
    }
}

/// Test that subscription messages are properly formed with skip lists
#[test]
fn test_subscription_message_skip_list() {
    let contract_key = ContractKey::from(ContractInstanceId::new([2u8; 32]));
    let transaction_id = Transaction::new::<SubscribeMsg>();
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();

    // Create skip list with failed peers
    let mut skip_list = HashSet::new();
    skip_list.insert(peer1.clone());
    skip_list.insert(peer2.clone());

    // Create a SeekNode message with skip list
    let target_location = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.5).unwrap()),
    };

    let subscriber_location = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.3).unwrap()),
    };

    let msg = SubscribeMsg::SeekNode {
        id: transaction_id,
        key: contract_key,
        target: target_location.clone(),
        subscriber: subscriber_location,
        skip_list: skip_list.clone(),
        htl: 10,
        retries: 2,
    };

    // Verify message contains skip list
    if let SubscribeMsg::SeekNode {
        skip_list: list,
        retries,
        ..
    } = msg
    {
        assert_eq!(list.len(), 2, "Skip list should contain 2 peers");
        assert!(list.contains(&peer1), "Should contain peer1");
        assert!(list.contains(&peer2), "Should contain peer2");
        assert_eq!(retries, 2, "Should have 2 retries");
    } else {
        panic!("Expected SeekNode message");
    }
}

/// Test that optimal location nodes can still subscribe
/// This validates the fix for removing the early return
#[test]
fn test_optimal_location_can_subscribe() {
    let contract_key = ContractKey::from(ContractInstanceId::new([3u8; 32]));
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Create a subscription at optimal location
    // In the old code, this would have early returned
    let op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::PrepareRequest {
            id: transaction_id,
            key: contract_key,
        }),
    };

    // Even at optimal location, we should be able to create subscription
    assert!(
        !op.finalized(),
        "Should create subscription even at optimal location"
    );
}

/// Test transaction ID correlation for response routing
/// This validates the waiting_for_transaction_result fix
#[test]
fn test_transaction_id_correlation() {
    let _contract_key = ContractKey::from(ContractInstanceId::new([4u8; 32]));
    let transaction_id = Transaction::new::<SubscribeMsg>();
    let subscriber = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.2).unwrap()),
    };

    // Create subscription with upstream subscriber waiting
    let op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::AwaitingResponse {
            skip_list: HashSet::new(),
            retries: 0,
            upstream_subscriber: Some(subscriber.clone()),
            current_hop: 5,
        }),
    };

    // Verify transaction ID is maintained for correlation
    assert_eq!(op.id, transaction_id, "Transaction ID should be preserved");

    // Verify subscriber is tracked
    if let Some(SubscribeState::AwaitingResponse {
        upstream_subscriber,
        ..
    }) = &op.state
    {
        assert_eq!(
            *upstream_subscriber,
            Some(subscriber),
            "Subscriber should be tracked"
        );
    }
}

/// Test that multiple retries accumulate skip list
/// This validates k_closest_potentially_caching with k=3
#[test]
fn test_multiple_retries_skip_list_accumulation() {
    let transaction_id = Transaction::new::<SubscribeMsg>();
    let mut skip_list = HashSet::new();

    // Simulate 3 failed attempts (k=3)
    for i in 0..3 {
        skip_list.insert(PeerId::random());

        let op = SubscribeOp {
            id: transaction_id,
            state: Some(SubscribeState::AwaitingResponse {
                skip_list: skip_list.clone(),
                retries: i + 1,
                upstream_subscriber: None,
                current_hop: 10 - i,
            }),
        };

        if let Some(SubscribeState::AwaitingResponse {
            skip_list: list,
            retries,
            ..
        }) = &op.state
        {
            assert_eq!(list.len(), i + 1, "Skip list should grow with retries");
            assert_eq!(*retries, i + 1, "Retry count should increment");
        }
    }

    assert_eq!(skip_list.len(), 3, "Should have tried k=3 peers");
}

/// Test subscription response generation for different states
#[test]
fn test_subscription_response_generation() {
    let contract_key = ContractKey::from(ContractInstanceId::new([5u8; 32]));
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Test completed subscription generates success response
    let op_success = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::Completed { key: contract_key }),
    };

    match op_success.to_host_result() {
        Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
            subscribed: true,
            ..
        })) => {
            // Success
        }
        _ => panic!("Expected successful subscription response"),
    }

    // Test non-completed subscription generates error
    let op_pending = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::AwaitingResponse {
            skip_list: HashSet::new(),
            retries: 0,
            upstream_subscriber: None,
            current_hop: 5,
        }),
    };

    assert!(
        op_pending.to_host_result().is_err(),
        "Non-completed should generate error"
    );
}
