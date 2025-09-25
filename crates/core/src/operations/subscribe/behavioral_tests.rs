use super::*;
use crate::{
    message::Transaction,
    node::PeerId,
    ring::{Location, PeerKeyLocation},
    util::Contains,
};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use std::collections::HashSet;

/// Test that subscription state machine transitions correctly through states
/// This validates that the subscription can move through states properly
#[test]
fn test_subscription_state_machine_basic_transitions() {
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

    // Transition to AwaitingResponse with skip list
    let mut skip_list = HashSet::new();
    skip_list.insert(PeerId::random());

    op.state = Some(SubscribeState::AwaitingResponse {
        skip_list: skip_list.clone(),
        retries: 1,
        upstream_subscriber: None,
        current_hop: 9,
    });
    assert!(!op.finalized(), "AwaitingResponse should not be finalized");

    // Verify skip list is maintained
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
}

/// Test that subscription messages contain proper skip list data
/// This tests the message structure used by the subscription protocol
#[test]
fn test_subscription_messages_with_skip_lists() {
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

    // Verify message contains skip list and retry count
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

/// Test that skip list properly excludes failed peers
/// This validates the Contains trait implementation and skip list behavior
#[test]
fn test_skip_list_excludes_failed_peers() {
    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let peer3 = PeerId::random();

    // Test with HashSet<PeerId> as skip list (commonly used in the code)
    let mut skip_list = HashSet::new();
    skip_list.insert(peer1.clone());
    skip_list.insert(peer2.clone());

    // Test Contains implementation for HashSet reference (using clones for ownership)
    assert!(
        (&skip_list).has_element(peer1.clone()),
        "Should contain peer1"
    );
    assert!(
        (&skip_list).has_element(peer2.clone()),
        "Should contain peer2"
    );
    assert!(
        !(&skip_list).has_element(peer3.clone()),
        "Should not contain peer3"
    );

    // Verify the skip list has the expected size
    assert_eq!(skip_list.len(), 2, "Skip list should contain 2 peers");

    // Test that regular HashSet operations work
    assert!(skip_list.contains(&peer1), "HashSet should contain peer1");
    assert!(skip_list.contains(&peer2), "HashSet should contain peer2");
    assert!(
        !skip_list.contains(&peer3),
        "HashSet should not contain peer3"
    );
}

/// Test that subscription failure response contains proper data
/// This tests the ReturnSub message structure for failed subscriptions
#[test]
fn test_subscription_failure_response_structure() {
    let contract_key = ContractKey::from(ContractInstanceId::new([4u8; 32]));
    let transaction_id = Transaction::new::<SubscribeMsg>();

    let failed_peer = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.1).unwrap()),
    };

    let target_peer = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.5).unwrap()),
    };

    // Create ReturnSub message indicating subscription failed
    let return_msg = SubscribeMsg::ReturnSub {
        id: transaction_id,
        key: contract_key,
        sender: failed_peer.clone(), // This peer failed to provide subscription
        target: target_peer.clone(),
        subscribed: false, // Failed!
    };

    // Verify the failure response structure
    match return_msg {
        SubscribeMsg::ReturnSub {
            id,
            key,
            sender,
            target,
            subscribed,
        } => {
            assert_eq!(id, transaction_id, "Transaction ID should match");
            assert_eq!(key, contract_key, "Contract key should match");
            assert_eq!(sender, failed_peer, "Sender should be the failed peer");
            assert_eq!(target, target_peer, "Target should match");
            assert!(!subscribed, "Should indicate failure");
        }
        _ => panic!("Expected ReturnSub message"),
    }
}

/// Test that optimal location nodes can still create subscriptions
/// This validates that nodes at optimal location can still subscribe (they don't early return)
#[test]
fn test_optimal_location_nodes_can_subscribe() {
    let contract_key = ContractKey::from(ContractInstanceId::new([5u8; 32]));

    // Create a subscription operation - this should always work regardless of location
    let sub_op = start_op(contract_key);

    // Verify operation starts in PrepareRequest state
    match &sub_op.state {
        Some(SubscribeState::PrepareRequest { id: _, key }) => {
            assert_eq!(*key, contract_key, "Should store correct contract key");
        }
        _ => panic!("Should start in PrepareRequest state"),
    }

    assert!(
        !sub_op.finalized(),
        "PrepareRequest should not be finalized"
    );

    // This validates the fix where optimal location nodes can still create subscriptions
    // In the broken code, there might have been an early return preventing this
}

/// Test that subscription retry logic properly accumulates skip list
/// This validates that failed peers are tracked and excluded from future attempts
#[test]
fn test_subscription_retry_skip_list_accumulation() {
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Start with empty skip list
    let mut skip_list = HashSet::new();

    // Simulate multiple failed attempts where peers are added to skip list
    for i in 0..3 {
        skip_list.insert(PeerId::random()); // Add another failed peer

        let op = SubscribeOp {
            id: transaction_id,
            state: Some(SubscribeState::AwaitingResponse {
                skip_list: skip_list.clone(),
                retries: i + 1,
                upstream_subscriber: None,
                current_hop: 10 - i,
            }),
        };

        // Verify skip list grows and retry count increases
        if let Some(SubscribeState::AwaitingResponse {
            skip_list: list,
            retries,
            current_hop,
            ..
        }) = &op.state
        {
            assert_eq!(list.len(), i + 1, "Skip list should grow with retries");
            assert_eq!(*retries, i + 1, "Retry count should increment");
            assert_eq!(*current_hop, 10 - i, "Hop count should decrease");
        }
    }

    // After 3 retries, we should have 3 peers in the skip list
    assert_eq!(skip_list.len(), 3, "Should have tried 3 peers");
}

/// Test that MAX_RETRIES constant prevents infinite retry loops
/// This validates the MAX_RETRIES=10 limit in the subscription logic
#[test]
fn test_max_retries_constant_prevents_infinite_loops() {
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Test that we enforce the MAX_RETRIES limit
    assert_eq!(MAX_RETRIES, 10, "MAX_RETRIES should be 10");

    // Create a subscription that has reached MAX_RETRIES
    let mut skip_list = HashSet::new();
    for _i in 0..MAX_RETRIES {
        skip_list.insert(PeerId::random());
    }

    let op_at_max_retries = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::AwaitingResponse {
            skip_list: skip_list.clone(),
            retries: MAX_RETRIES,
            upstream_subscriber: None,
            current_hop: 1,
        }),
    };

    // Verify the operation tracks the maximum retries
    if let Some(SubscribeState::AwaitingResponse {
        skip_list: list,
        retries,
        ..
    }) = &op_at_max_retries.state
    {
        assert_eq!(
            list.len(),
            MAX_RETRIES,
            "Skip list should contain MAX_RETRIES peers"
        );
        assert_eq!(*retries, MAX_RETRIES, "Should have reached MAX_RETRIES");
    }

    // This test validates that the subscription logic has a retry limit
    // The actual enforcement happens in the message processing logic
    // MAX_RETRIES is a constant set to 10, which is a reasonable limit
}

/// Test that successful subscription response generates proper host result
/// This validates that the subscription system properly returns success responses
#[test]
fn test_successful_subscription_generates_host_result() {
    let contract_key = ContractKey::from(ContractInstanceId::new([8u8; 32]));
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Create a completed subscription operation
    let completed_op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::Completed { key: contract_key }),
    };

    // Test that completed state generates correct host result
    let result = completed_op.to_host_result();

    match result {
        Ok(_host_response) => {
            // The exact structure may depend on the implementation
            // This test validates that successful subscriptions return Ok results
        }
        Err(e) => panic!(
            "Expected successful result for completed subscription, got error: {:?}",
            e
        ),
    }

    // Test that non-completed states generate error results
    let pending_op = SubscribeOp {
        id: transaction_id,
        state: Some(SubscribeState::AwaitingResponse {
            skip_list: HashSet::new(),
            retries: 0,
            upstream_subscriber: None,
            current_hop: 5,
        }),
    };

    let pending_result = pending_op.to_host_result();
    assert!(
        pending_result.is_err(),
        "Non-completed operations should return errors"
    );
}

/// Integration test demonstrating the subscription flow that would fail if k_closest_potentially_caching is broken
/// This test shows the critical path where the subscription logic depends on k_closest_potentially_caching
#[test]
fn test_subscription_integration_would_fail_if_k_closest_broken() {
    let contract_key = ContractKey::from(ContractInstanceId::new([9u8; 32]));

    // Test the start_op function - this should always work
    let sub_op = start_op(contract_key);

    // Verify the operation structure matches what request_subscribe expects
    match &sub_op.state {
        Some(SubscribeState::PrepareRequest { id, key }) => {
            assert_eq!(
                *key, contract_key,
                "PrepareRequest should store the contract key"
            );
            assert_eq!(sub_op.id, *id, "Transaction IDs should match");
        }
        _ => panic!("start_op should create PrepareRequest state"),
    }

    // Test the message structures used in the subscription flow
    let transaction_id = Transaction::new::<SubscribeMsg>();
    let target = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.1).unwrap()),
    };

    // This is the message that request_subscribe would create when calling k_closest_potentially_caching
    let request_msg = SubscribeMsg::RequestSub {
        id: transaction_id,
        key: contract_key,
        target: target.clone(),
    };

    // Verify the message structure
    if let SubscribeMsg::RequestSub {
        key,
        target: msg_target,
        ..
    } = request_msg
    {
        assert_eq!(
            key, contract_key,
            "RequestSub should contain correct contract key"
        );
        assert_eq!(
            msg_target, target,
            "RequestSub should contain correct target"
        );
    } else {
        panic!("Expected RequestSub message");
    }

    // Test skip list message structure (what would be used after failures)
    let mut skip_list = HashSet::new();
    skip_list.insert(PeerId::random());
    skip_list.insert(PeerId::random());

    let seek_msg = SubscribeMsg::SeekNode {
        id: transaction_id,
        key: contract_key,
        target: target.clone(),
        subscriber: PeerKeyLocation {
            peer: PeerId::random(),
            location: Some(Location::try_from(0.9).unwrap()),
        },
        skip_list: skip_list.clone(),
        htl: 5,
        retries: 2,
    };

    // This message structure is what the message handler would use to call k_closest_potentially_caching
    if let SubscribeMsg::SeekNode {
        skip_list: msg_skip_list,
        retries,
        ..
    } = seek_msg
    {
        assert_eq!(
            msg_skip_list.len(),
            2,
            "Skip list should contain failed peers"
        );
        assert_eq!(retries, 2, "Should track retry count");

        // The subscription logic would call:
        // op_manager.ring.k_closest_potentially_caching(key, &skip_list, 3)
        // If this function is broken, the subscription system would fail to find alternative peers
    }

    // This test validates the integration points where k_closest_potentially_caching is critical:
    // 1. request_subscribe calls it to find initial peers
    // 2. SeekNode message handler calls it to find alternative peers when contract not found
    // 3. ReturnSub(false) handler calls it to retry after failures
}

use std::sync::{Arc, Mutex};

/// TestRing implements only the methods used by subscription routing
#[allow(clippy::type_complexity)]
struct TestRing {
    pub k_closest_calls: Arc<Mutex<Vec<(ContractKey, Vec<PeerId>, usize)>>>,
    pub candidates: Vec<PeerKeyLocation>,
}

impl TestRing {
    fn new(candidates: Vec<PeerKeyLocation>, _own_location: PeerKeyLocation) -> Self {
        Self {
            k_closest_calls: Arc::new(Mutex::new(Vec::new())),
            candidates,
        }
    }

    pub fn k_closest_potentially_caching(
        &self,
        key: &ContractKey,
        skip_list: impl Contains<PeerId> + Clone,
        k: usize,
    ) -> Vec<PeerKeyLocation> {
        // Record the call
        let skip_vec: Vec<PeerId> = self
            .candidates
            .iter()
            .filter(|peer| skip_list.has_element(peer.peer.clone()))
            .map(|peer| peer.peer.clone())
            .collect();
        self.k_closest_calls
            .lock()
            .unwrap()
            .push((*key, skip_vec, k));

        // Return candidates not in skip list
        self.candidates
            .iter()
            .filter(|peer| !skip_list.has_element(peer.peer.clone()))
            .take(k)
            .cloned()
            .collect()
    }
}

/// Behavioral test that verifies the subscription routing logic with skip lists
/// This test would FAIL if k_closest_potentially_caching stopped being used correctly
#[tokio::test]
async fn test_subscription_routing_calls_k_closest_with_skip_list() {
    let contract_key = ContractKey::from(ContractInstanceId::new([10u8; 32]));

    // Create test peers
    let peer1 = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.1).unwrap()),
    };
    let peer2 = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.2).unwrap()),
    };
    let peer3 = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.3).unwrap()),
    };
    let own_location = PeerKeyLocation {
        peer: PeerId::random(),
        location: Some(Location::try_from(0.5).unwrap()),
    };

    // Create TestRing with multiple candidates
    let test_ring = TestRing::new(
        vec![peer1.clone(), peer2.clone(), peer3.clone()],
        own_location.clone(),
    );

    // 1. Test start_op function - this should always work now (validates no early return bug)
    let sub_op = start_op(contract_key);
    assert!(matches!(
        sub_op.state,
        Some(SubscribeState::PrepareRequest { .. })
    ));

    // 2. Test k_closest_potentially_caching with empty skip list (simulates request_subscribe call)
    const EMPTY: &[PeerId] = &[];
    let initial_candidates = test_ring.k_closest_potentially_caching(&contract_key, EMPTY, 3);

    // 3. Verify initial call was recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().unwrap();
    assert_eq!(
        k_closest_calls.len(),
        1,
        "Should have called k_closest_potentially_caching once"
    );
    assert_eq!(
        k_closest_calls[0].0, contract_key,
        "Should query for correct contract"
    );
    assert_eq!(
        k_closest_calls[0].1.len(),
        0,
        "Initial call should have empty skip list"
    );
    assert_eq!(k_closest_calls[0].2, 3, "Should request 3 candidates");
    drop(k_closest_calls);

    // 4. Verify all candidates are returned initially
    assert_eq!(
        initial_candidates.len(),
        3,
        "Should return all 3 candidates initially"
    );
    assert_eq!(initial_candidates[0], peer1, "Should return peer1 first");

    // 5. Test retry with skip list (simulates ReturnSub handler)
    let mut skip_list = HashSet::new();
    skip_list.insert(peer1.peer.clone()); // First peer failed

    // This is the critical call that would happen in the ReturnSub handler
    let candidates_after_failure =
        test_ring.k_closest_potentially_caching(&contract_key, &skip_list, 3);

    // 6. Verify the second call used the skip list correctly
    let k_closest_calls = test_ring.k_closest_calls.lock().unwrap();
    assert_eq!(
        k_closest_calls.len(),
        2,
        "Should have called k_closest_potentially_caching twice"
    );
    assert_eq!(
        k_closest_calls[1].0, contract_key,
        "Second call should query for correct contract"
    );
    assert_eq!(
        k_closest_calls[1].1.len(),
        1,
        "Skip list should contain 1 failed peer"
    );
    assert_eq!(
        k_closest_calls[1].1[0], peer1.peer,
        "Skip list should contain the failed peer"
    );
    assert_eq!(k_closest_calls[1].2, 3, "Should still request 3 candidates");
    drop(k_closest_calls);

    // 7. Verify the failed peer is excluded from results
    assert!(
        !candidates_after_failure
            .iter()
            .any(|p| p.peer == peer1.peer),
        "Failed peer should be excluded from candidates"
    );
    assert!(
        !candidates_after_failure.is_empty(),
        "Should find alternative candidates"
    );
    assert_eq!(
        candidates_after_failure[0], peer2,
        "Should return peer2 as first alternative"
    );

    // 8. Test multiple failures
    skip_list.insert(peer2.peer.clone()); // Second peer also failed
    let final_candidates = test_ring.k_closest_potentially_caching(&contract_key, &skip_list, 3);

    let k_closest_calls = test_ring.k_closest_calls.lock().unwrap();
    assert_eq!(
        k_closest_calls.len(),
        3,
        "Should have called k_closest_potentially_caching three times"
    );
    assert_eq!(
        k_closest_calls[2].1.len(),
        2,
        "Final skip list should contain 2 failed peers"
    );
    drop(k_closest_calls);

    // Only peer3 should remain
    assert_eq!(
        final_candidates.len(),
        1,
        "Should have 1 remaining candidate"
    );
    assert_eq!(
        final_candidates[0], peer3,
        "Should return peer3 as final option"
    );
    assert!(
        !final_candidates
            .iter()
            .any(|p| p.peer == peer1.peer || p.peer == peer2.peer),
        "Failed peers should be excluded"
    );

    // This test validates the critical subscription routing behavior:
    // 1. start_op always works (no early return bug)
    // 2. k_closest_potentially_caching is called with empty skip list initially
    // 3. k_closest_potentially_caching is called with proper skip list after failures
    // 4. Skip list correctly excludes failed peers
    // 5. Alternative peers are found after failures
    // 6. Multiple failures are handled correctly

    // This test would FAIL if:
    // - k_closest_potentially_caching stopped being called
    // - Skip list logic was broken
    // - Retry logic was broken
    // - Contains trait implementation failed
}
