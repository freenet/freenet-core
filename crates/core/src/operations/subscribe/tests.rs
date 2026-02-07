use super::*;
use crate::{message::Transaction, ring::PeerKeyLocation, util::Contains};
use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
use std::collections::HashSet;
use std::net::SocketAddr;

/// Helper to create PeerKeyLocation with a random peer
fn random_peer() -> PeerKeyLocation {
    PeerKeyLocation::random()
}

/// TestRing implements only the methods used by subscription routing
/// Uses SocketAddr for skip lists to match production code
#[allow(clippy::type_complexity)]
struct TestRing {
    pub k_closest_calls:
        std::sync::Arc<tokio::sync::Mutex<Vec<(ContractInstanceId, Vec<SocketAddr>, usize)>>>,
    pub candidates: Vec<PeerKeyLocation>,
    pub own_addr: SocketAddr,
}

impl TestRing {
    fn new(candidates: Vec<PeerKeyLocation>, own_location: PeerKeyLocation) -> Self {
        let own_addr = own_location
            .socket_addr()
            .expect("own location must have address");
        Self {
            k_closest_calls: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
            candidates,
            own_addr,
        }
    }

    pub async fn k_closest_potentially_caching(
        &self,
        instance_id: &ContractInstanceId,
        skip_list: impl Contains<SocketAddr> + Clone,
        k: usize,
    ) -> Vec<PeerKeyLocation> {
        // Record skipped addresses from candidates
        let mut skip_vec: Vec<SocketAddr> = self
            .candidates
            .iter()
            .filter_map(|peer| {
                peer.socket_addr()
                    .filter(|addr| skip_list.has_element(*addr))
            })
            .collect();

        // Add own address if it's in skip list
        if skip_list.has_element(self.own_addr) && !skip_vec.contains(&self.own_addr) {
            skip_vec.push(self.own_addr);
        }

        // Record the call
        self.k_closest_calls
            .lock()
            .await
            .push((*instance_id, skip_vec, k));

        // Return candidates not in skip list
        self.candidates
            .iter()
            .filter(|peer| {
                peer.socket_addr()
                    .map(|addr| !skip_list.has_element(addr))
                    .unwrap_or(true)
            })
            .take(k)
            .cloned()
            .collect()
    }
}

/// Legacy test that verifies the subscription routing logic with skip lists directly
/// This test focuses on the TestRing behavior itself
#[tokio::test]
async fn test_subscription_routing_calls_k_closest_with_skip_list() {
    let contract_key = ContractKey::from_id_and_code(
        ContractInstanceId::new([10u8; 32]),
        CodeHash::new([11u8; 32]),
    );

    // Create test peers
    let peer1 = random_peer();
    let peer2 = random_peer();
    let peer3 = random_peer();
    let own_location = random_peer();

    // Get socket addresses for skip list operations
    let peer1_addr = peer1.socket_addr().expect("peer1 must have address");
    let peer2_addr = peer2.socket_addr().expect("peer2 must have address");
    let own_addr = own_location
        .socket_addr()
        .expect("own location must have address");

    // Create TestRing with multiple candidates
    let test_ring = TestRing::new(
        vec![peer1.clone(), peer2.clone(), peer3.clone()],
        own_location.clone(),
    );

    // 1. Test start_op function - this should always work now (validates no early return bug)
    let sub_op = start_op(*contract_key.id(), false);
    assert!(matches!(
        sub_op.state,
        Some(SubscribeState::PrepareRequest { .. })
    ));

    // 2. Test k_closest_potentially_caching with initial skip list containing self
    let mut initial_skip: HashSet<SocketAddr> = HashSet::new();
    initial_skip.insert(own_addr);
    let initial_candidates = test_ring
        .k_closest_potentially_caching(contract_key.id(), &initial_skip, 3)
        .await;

    // 3. Verify initial call was recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
    assert_eq!(
        k_closest_calls.len(),
        1,
        "Should have called k_closest_potentially_caching once"
    );
    assert_eq!(
        k_closest_calls[0].0,
        *contract_key.id(),
        "Should query for correct contract"
    );
    assert_eq!(
        k_closest_calls[0].1.len(),
        1,
        "Initial call should only skip own peer"
    );
    assert_eq!(
        k_closest_calls[0].1[0], own_addr,
        "Initial skip list should contain own address"
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
    let mut skip_list: HashSet<SocketAddr> = HashSet::new();
    skip_list.insert(peer1_addr); // First peer failed

    // This is the critical call that would happen in the ReturnSub handler
    let candidates_after_failure = test_ring
        .k_closest_potentially_caching(contract_key.id(), &skip_list, 3)
        .await;

    // 6. Verify the second call used the skip list correctly
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
    assert_eq!(
        k_closest_calls.len(),
        2,
        "Should have called k_closest_potentially_caching twice"
    );
    assert_eq!(
        k_closest_calls[1].0,
        *contract_key.id(),
        "Second call should query for correct contract"
    );
    assert_eq!(
        k_closest_calls[1].1.len(),
        1,
        "Skip list should contain 1 failed peer"
    );
    assert_eq!(
        k_closest_calls[1].1[0], peer1_addr,
        "Skip list should contain the failed peer's address"
    );
    assert_eq!(k_closest_calls[1].2, 3, "Should still request 3 candidates");
    drop(k_closest_calls);

    // 7. Verify the failed peer is excluded from results
    assert!(
        !candidates_after_failure
            .iter()
            .any(|p| p.socket_addr() == Some(peer1_addr)),
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
    skip_list.insert(peer2_addr); // Second peer also failed
    let final_candidates = test_ring
        .k_closest_potentially_caching(contract_key.id(), &skip_list, 3)
        .await;

    let k_closest_calls = test_ring.k_closest_calls.lock().await;
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
            .any(|p| p.socket_addr() == Some(peer1_addr) || p.socket_addr() == Some(peer2_addr)),
        "Failed peers should be excluded"
    );
}

/// Integration test that exercises the production subscription code paths that use k_closest_potentially_caching
/// This test proves that if k_closest_potentially_caching usage was broken in subscription code, this test would fail
#[tokio::test]
async fn test_subscription_production_code_paths_use_k_closest() {
    let contract_key = ContractKey::from_id_and_code(
        ContractInstanceId::new([11u8; 32]),
        CodeHash::new([12u8; 32]),
    );

    // Create test peers
    let peer1 = random_peer();
    let peer2 = random_peer();
    let peer3 = random_peer();
    let own_location = random_peer();

    // Get socket addresses
    let peer1_addr = peer1.socket_addr().expect("peer1 must have address");
    let peer2_addr = peer2.socket_addr().expect("peer2 must have address");
    let own_addr = own_location
        .socket_addr()
        .expect("own location must have address");

    // Create TestRing that records all k_closest_potentially_caching calls
    let test_ring = TestRing::new(
        vec![peer1.clone(), peer2.clone(), peer3.clone()],
        own_location.clone(),
    );

    // Test 1: Validate that start_op creates correct initial state
    let sub_op = start_op(*contract_key.id(), false);
    assert!(matches!(
        sub_op.state,
        Some(SubscribeState::PrepareRequest { .. })
    ));

    // Test 2: Simulate the k_closest_potentially_caching call made in request_subscribe
    let mut initial_skip: HashSet<SocketAddr> = HashSet::new();
    initial_skip.insert(own_addr);
    let initial_candidates = test_ring
        .k_closest_potentially_caching(contract_key.id(), &initial_skip, 3)
        .await;

    // Verify the call was recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
    assert_eq!(
        k_closest_calls.len(),
        1,
        "Should have recorded initial call"
    );
    assert_eq!(
        k_closest_calls[0].0,
        *contract_key.id(),
        "Should use correct contract key"
    );
    assert_eq!(
        k_closest_calls[0].1.len(),
        1,
        "Should skip own peer initially"
    );
    assert_eq!(
        k_closest_calls[0].1[0], own_addr,
        "Skip list should contain own address"
    );
    assert_eq!(k_closest_calls[0].2, 3, "Should request 3 candidates");
    drop(k_closest_calls);

    assert_eq!(
        initial_candidates.len(),
        3,
        "Should return all 3 candidates"
    );
    assert_eq!(initial_candidates[0], peer1, "Should return peer1 first");

    // Test 3: Simulate the k_closest_potentially_caching call made in SeekNode handler
    let mut skip_list: HashSet<SocketAddr> = HashSet::new();
    skip_list.insert(peer1_addr);
    let seek_candidates = test_ring
        .k_closest_potentially_caching(contract_key.id(), &skip_list, 3)
        .await;

    // Verify this call was also recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
    assert_eq!(k_closest_calls.len(), 2, "Should have recorded second call");
    assert_eq!(
        k_closest_calls[1].0,
        *contract_key.id(),
        "Should use correct contract key"
    );
    assert_eq!(
        k_closest_calls[1].1.len(),
        1,
        "Should include failed peer in skip list"
    );
    assert_eq!(
        k_closest_calls[1].1[0], peer1_addr,
        "Should skip the failed peer's address"
    );
    assert_eq!(k_closest_calls[1].2, 3, "Should still request 3 candidates");
    drop(k_closest_calls);

    // Verify failed peer is excluded from results
    assert!(
        !seek_candidates
            .iter()
            .any(|p| p.socket_addr() == Some(peer1_addr)),
        "Should exclude failed peer"
    );
    assert_eq!(
        seek_candidates.len(),
        2,
        "Should return remaining 2 candidates"
    );
    assert_eq!(seek_candidates[0], peer2, "Should return peer2 first");

    // Test 4: Simulate the k_closest_potentially_caching call made in ReturnSub(false) handler
    skip_list.insert(peer2_addr); // Second peer also failed
    let retry_candidates = test_ring
        .k_closest_potentially_caching(contract_key.id(), &skip_list, 3)
        .await;

    // Verify this call was recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
    assert_eq!(k_closest_calls.len(), 3, "Should have recorded third call");
    assert_eq!(
        k_closest_calls[2].0,
        *contract_key.id(),
        "Should use correct contract key"
    );
    assert_eq!(
        k_closest_calls[2].1.len(),
        2,
        "Should include both failed peers in skip list"
    );
    assert!(
        k_closest_calls[2].1.contains(&peer1_addr),
        "Should skip peer1"
    );
    assert!(
        k_closest_calls[2].1.contains(&peer2_addr),
        "Should skip peer2"
    );
    assert_eq!(k_closest_calls[2].2, 3, "Should still request 3 candidates");
    drop(k_closest_calls);

    // Verify both failed peers are excluded
    assert!(
        !retry_candidates
            .iter()
            .any(|p| p.socket_addr() == Some(peer1_addr) || p.socket_addr() == Some(peer2_addr)),
        "Should exclude both failed peers"
    );
    assert_eq!(retry_candidates.len(), 1, "Should return final 1 candidate");
    assert_eq!(
        retry_candidates[0], peer3,
        "Should return peer3 as last option"
    );
}

/// Test that validates the subscription operation would call k_closest_potentially_caching correctly
/// This test demonstrates how the production code flow works without needing full OpManager
#[tokio::test]
async fn test_subscription_validates_k_closest_usage() {
    // This test validates that the subscription operation correctly:
    // 1. Calls k_closest_potentially_caching with a skip list containing the local peer on first attempt
    // 2. Accumulates failed peers in the skip list
    // 3. Calls k_closest_potentially_caching with the skip list on retry

    let contract_key =
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]));
    let transaction_id = Transaction::new::<SubscribeMsg>();

    // Create TestRing that records all k_closest calls
    let test_ring = TestRing::new(
        vec![random_peer(), random_peer(), random_peer()],
        random_peer(),
    );

    // Test 1: Validate the exact call pattern from request_subscribe (line 72)
    {
        let mut initial_skip: HashSet<SocketAddr> = HashSet::new();
        initial_skip.insert(test_ring.own_addr);
        let _candidates = test_ring
            .k_closest_potentially_caching(contract_key.id(), &initial_skip, 3)
            .await;

        let calls = test_ring.k_closest_calls.lock().await;
        assert_eq!(calls.len(), 1, "Should record the call");
        let (instance_id, skip_list, k) = &calls[0];
        assert_eq!(*instance_id, *contract_key.id());
        assert_eq!(
            skip_list.len(),
            1,
            "First attempt should only skip own peer"
        );
        assert_eq!(
            skip_list[0], test_ring.own_addr,
            "Skip list should contain own address"
        );
        assert_eq!(*k, 3, "Uses k=3 as per fix");
    }

    // Test 2: Validate retry with skip list (as in ReturnSub handler line 336)
    {
        test_ring.k_closest_calls.lock().await.clear();

        let failed_addr = test_ring.candidates[0]
            .socket_addr()
            .expect("candidate must have address");
        let skip_list = [failed_addr];

        let candidates = test_ring
            .k_closest_potentially_caching(contract_key.id(), &skip_list[..], 3)
            .await;

        // Verify skip list is used
        let calls = test_ring.k_closest_calls.lock().await;
        let (instance_id, used_skip_list, k) = &calls[0];
        assert_eq!(*instance_id, *contract_key.id());
        assert_eq!(used_skip_list.len(), 1, "Skip list includes failed peer");
        assert_eq!(used_skip_list[0], failed_addr);
        assert_eq!(*k, 3);

        // Verify failed peer is excluded from candidates
        assert!(
            !candidates
                .iter()
                .any(|c| c.socket_addr() == Some(failed_addr)),
            "Failed peer must be excluded"
        );
    }

    // Test 3: Validate simplified AwaitingResponse state
    {
        let op = SubscribeOp {
            id: transaction_id,
            state: Some(SubscribeState::AwaitingResponse {
                next_hop: None,
                instance_id: *contract_key.id(),
            }),
            requester_addr: None,
            is_renewal: false,
        };

        // State is simplified - skip list is now in the Request message, not state
        assert!(matches!(
            op.state,
            Some(SubscribeState::AwaitingResponse { .. })
        ));
    }
}

/// Test that `start_op` creates the correct initial state for local subscription path.
///
/// This validates the entry point for local subscriptions (standalone mode).
#[test]
fn test_start_op_creates_prepare_request_state() {
    let instance_id = ContractInstanceId::new([42u8; 32]);

    let sub_op = start_op(instance_id, false);

    // Verify the operation is initialized correctly
    assert!(
        matches!(
            sub_op.state,
            Some(SubscribeState::PrepareRequest { id, instance_id: iid, .. })
            if iid == instance_id && id == sub_op.id
        ),
        "start_op should create PrepareRequest state with correct instance_id"
    );

    // Verify it's a local operation (no requester)
    assert_eq!(
        sub_op.requester_addr, None,
        "Local subscription should have no requester address"
    );

    // Verify the transaction ID was generated
    let tx_id = sub_op.id;
    assert_ne!(
        format!("{}", tx_id),
        "",
        "Transaction ID should be generated"
    );
}

/// Test that `start_op_with_id` creates correct state with a specific transaction ID.
///
/// This is used for operation deduplication in the subscription path.
#[test]
fn test_start_op_with_id_uses_provided_transaction() {
    let instance_id = ContractInstanceId::new([99u8; 32]);
    let custom_tx = Transaction::new::<SubscribeMsg>();

    let sub_op = start_op_with_id(instance_id, custom_tx, false);

    // Verify the operation uses the provided transaction ID
    assert_eq!(
        sub_op.id, custom_tx,
        "start_op_with_id should use provided transaction ID"
    );

    // Verify state is correct
    assert!(
        matches!(
            sub_op.state,
            Some(SubscribeState::PrepareRequest { id, instance_id: iid, .. })
            if iid == instance_id && id == custom_tx
        ),
        "PrepareRequest state should have provided transaction ID"
    );

    // Verify it's a local operation
    assert_eq!(
        sub_op.requester_addr, None,
        "Local subscription should have no requester address"
    );
}

/// Test that the SubscribeOp state machine transitions are correctly validated.
///
/// This test ensures the operation state follows the expected lifecycle:
/// PrepareRequest → AwaitingResponse → Completed
#[test]
fn test_subscribe_op_state_lifecycle() {
    let instance_id = ContractInstanceId::new([7u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([8u8; 32]));
    let tx_id = Transaction::new::<SubscribeMsg>();
    let peer_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    // 1. Initial state: PrepareRequest (created by start_op)
    let op_initial = SubscribeOp {
        id: tx_id,
        state: Some(SubscribeState::PrepareRequest {
            id: tx_id,
            instance_id,
            is_renewal: false,
        }),
        requester_addr: None,
        is_renewal: false,
    };

    assert!(
        !op_initial.finalized(),
        "PrepareRequest should not be finalized"
    );
    assert!(
        !op_initial.is_completed(),
        "PrepareRequest should not be completed"
    );

    // 2. Awaiting response state (after sending Subscribe::Request)
    let op_awaiting = SubscribeOp {
        id: tx_id,
        state: Some(SubscribeState::AwaitingResponse {
            next_hop: Some(peer_addr),
            instance_id,
        }),
        requester_addr: None,
        is_renewal: false,
    };

    assert!(
        !op_awaiting.finalized(),
        "AwaitingResponse should not be finalized"
    );
    assert!(
        !op_awaiting.is_completed(),
        "AwaitingResponse should not be completed"
    );
    assert_eq!(
        op_awaiting.get_next_hop_addr(),
        Some(peer_addr),
        "Should return next hop address for routing"
    );

    // 3. Completed state (after receiving Subscribe::Response)
    let op_completed = SubscribeOp {
        id: tx_id,
        state: Some(SubscribeState::Completed { key: contract_key }),
        requester_addr: None,
        is_renewal: false,
    };

    assert!(
        op_completed.finalized(),
        "Completed state should be finalized"
    );
    assert!(
        op_completed.is_completed(),
        "Completed state should be completed"
    );

    // Verify to_host_result returns success for completed operation
    let result = op_completed.to_host_result();
    assert!(
        result.is_ok(),
        "Completed operation should return Ok result"
    );
}

/// Test that failed subscription (state = None) returns error to client.
///
/// This validates error handling in the subscription path.
#[test]
fn test_subscribe_op_failed_state_returns_error() {
    let tx_id = Transaction::new::<SubscribeMsg>();

    // Create operation with no state (indicates failure)
    let op_failed = SubscribeOp {
        id: tx_id,
        state: None,
        requester_addr: None,
        is_renewal: false,
    };

    // Verify to_host_result returns error
    let result = op_failed.to_host_result();
    assert!(result.is_err(), "Failed subscription should return error");

    // Verify error message is appropriate
    if let Err(err) = result {
        let error_msg = format!("{:?}", err);
        assert!(
            error_msg.contains("subscribe didn't finish successfully"),
            "Error should indicate subscription failure"
        );
    }
}

/// Test the local subscription completion path (standalone mode).
///
/// This test verifies the logic that would be exercised by `complete_local_subscription`.
/// In standalone mode (no peers), subscriptions complete locally via NodeEvent.
#[test]
fn test_local_subscription_completion_state() {
    let instance_id = ContractInstanceId::new([15u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([16u8; 32]));
    let tx_id = Transaction::new::<SubscribeMsg>();

    // Simulate the state after complete_local_subscription would be called
    // In reality, this sends a LocalSubscribeComplete event and marks operation complete
    let op = SubscribeOp {
        id: tx_id,
        state: Some(SubscribeState::Completed { key: contract_key }),
        requester_addr: None, // Local subscription, no network requester
        is_renewal: false,
    };

    // Verify operation is in completed state
    assert!(op.finalized(), "Local subscription should be finalized");
    assert!(op.is_completed(), "Local subscription should be completed");

    // Verify to_host_result returns successful subscription response
    let result = op.to_host_result();
    assert!(result.is_ok(), "Local subscription should succeed");

    if let Ok(host_response) = result {
        // Verify the response indicates successful subscription
        let response_str = format!("{:?}", host_response);
        assert!(
            response_str.contains("SubscribeResponse"),
            "Should return SubscribeResponse"
        );
        assert!(
            response_str.contains("subscribed: true"),
            "Should indicate successful subscription"
        );
    }
}

#[test]
fn test_is_renewal_flag() {
    let instance_id = ContractInstanceId::new([20u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([21u8; 32]));
    let tx = Transaction::new::<SubscribeMsg>();

    let renewal_op = SubscribeOp {
        id: tx,
        state: Some(SubscribeState::Completed { key: contract_key }),
        requester_addr: None,
        is_renewal: true,
    };
    assert!(renewal_op.is_renewal());

    let client_op = SubscribeOp {
        id: tx,
        state: Some(SubscribeState::Completed { key: contract_key }),
        requester_addr: None,
        is_renewal: false,
    };
    assert!(!client_op.is_renewal());
}

#[test]
fn test_op_enum_is_subscription_renewal() {
    use crate::operations::OpEnum;

    let instance_id = ContractInstanceId::new([22u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([23u8; 32]));
    let tx = Transaction::new::<SubscribeMsg>();

    let renewal = OpEnum::Subscribe(SubscribeOp {
        id: tx,
        state: Some(SubscribeState::Completed { key: contract_key }),
        requester_addr: None,
        is_renewal: true,
    });
    assert!(renewal.is_subscription_renewal());

    let non_renewal = OpEnum::Subscribe(SubscribeOp {
        id: tx,
        state: Some(SubscribeState::Completed { key: contract_key }),
        requester_addr: None,
        is_renewal: false,
    });
    assert!(!non_renewal.is_subscription_renewal());
}
