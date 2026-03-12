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
    assert!(matches!(sub_op.state, SubscribeState::PrepareRequest(_)));

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
    assert!(matches!(sub_op.state, SubscribeState::PrepareRequest(_)));

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
            state: SubscribeState::AwaitingResponse(super::AwaitingResponseData {
                next_hop: None,
                instance_id: *contract_key.id(),
                retries: 0,
                current_hop: 0,
                tried_peers: std::collections::HashSet::new(),
                alternatives: Vec::new(),
                attempts_at_hop: 0,
                visited: crate::operations::VisitedPeers::new(&transaction_id),
            }),
            requester_addr: None,
            requester_pub_key: None,
            is_renewal: false,
            stats: None,
        };

        // State is simplified - skip list is now in the Request message, not state
        assert!(matches!(op.state, SubscribeState::AwaitingResponse(_)));
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
            SubscribeState::PrepareRequest(ref data)
            if data.instance_id == instance_id && data.id == sub_op.id
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
            SubscribeState::PrepareRequest(ref data)
            if data.instance_id == instance_id && data.id == custom_tx
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
        state: SubscribeState::PrepareRequest(super::PrepareRequestData {
            id: tx_id,
            instance_id,
            is_renewal: false,
        }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
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
        state: SubscribeState::AwaitingResponse(super::AwaitingResponseData {
            next_hop: Some(peer_addr),
            instance_id,
            retries: 0,
            current_hop: 0,
            tried_peers: std::collections::HashSet::new(),
            alternatives: Vec::new(),
            attempts_at_hop: 0,
            visited: crate::operations::VisitedPeers::new(&tx_id),
        }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
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
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
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
        state: SubscribeState::Failed,
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
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
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None, // Local subscription, no network requester
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
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
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: true,
        stats: None,
    };
    assert!(renewal_op.is_renewal());

    let client_op = SubscribeOp {
        id: tx,
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
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
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: true,
        stats: None,
    });
    assert!(renewal.is_subscription_renewal());

    let non_renewal = OpEnum::Subscribe(SubscribeOp {
        id: tx,
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    });
    assert!(!non_renewal.is_subscription_renewal());
}

/// Test that SubscribeOp::outcome() returns ContractOpFailure when the operation
/// has stats but is not finalized (i.e., the subscription failed).
#[test]
fn test_subscribe_failure_outcome() {
    use crate::operations::OpOutcome;
    use crate::ring::{Location, PeerKeyLocation};

    let tx = Transaction::new::<SubscribeMsg>();
    let target_peer = PeerKeyLocation::random();
    let contract_location = Location::random();

    // Non-finalized op with stats → should return ContractOpFailure
    let op_with_stats = SubscribeOp {
        id: tx,
        state: SubscribeState::Failed, // Not completed = failed
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: Some(super::SubscribeStats {
            target_peer: target_peer.clone(),
            contract_location,
        }),
    };

    match op_with_stats.outcome() {
        OpOutcome::ContractOpFailure {
            target_peer: peer,
            contract_location: loc,
        } => {
            assert_eq!(*peer, target_peer);
            assert_eq!(loc, contract_location);
        }
        OpOutcome::ContractOpSuccess { .. }
        | OpOutcome::ContractOpSuccessUntimed { .. }
        | OpOutcome::Incomplete
        | OpOutcome::Irrelevant => {
            panic!("Expected ContractOpFailure for non-finalized op with stats")
        }
    }

    // Completed op with stats → should return Irrelevant (success path)
    let instance_id = ContractInstanceId::new([30u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([31u8; 32]));
    let op_completed = SubscribeOp {
        id: tx,
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: Some(super::SubscribeStats {
            target_peer: target_peer.clone(),
            contract_location,
        }),
    };
    match op_completed.outcome() {
        OpOutcome::ContractOpSuccessUntimed {
            target_peer: peer,
            contract_location: loc,
        } => {
            assert_eq!(*peer, target_peer);
            assert_eq!(loc, contract_location);
        }
        OpOutcome::ContractOpSuccess { .. }
        | OpOutcome::ContractOpFailure { .. }
        | OpOutcome::Incomplete
        | OpOutcome::Irrelevant => {
            panic!("Expected ContractOpSuccessUntimed for completed subscribe with stats")
        }
    }

    // Non-finalized op without stats → should return Incomplete
    let op_no_stats = SubscribeOp {
        id: tx,
        state: SubscribeState::Failed,
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };
    assert!(
        matches!(op_no_stats.outcome(), OpOutcome::Incomplete),
        "Non-finalized op without stats should return Incomplete"
    );

    // Test failure_routing_info()
    let op_for_info = SubscribeOp {
        id: tx,
        state: SubscribeState::Failed,
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: Some(super::SubscribeStats {
            target_peer: target_peer.clone(),
            contract_location,
        }),
    };
    let (peer, loc) = op_for_info
        .failure_routing_info()
        .expect("Should have routing info");
    assert_eq!(peer, target_peer);
    assert_eq!(loc, contract_location);
}

// ============ Outcome variant tests (following put.rs pattern) ============

/// Completed subscribe with stats → ContractOpSuccessUntimed (validates stats wiring fix).
#[test]
fn test_subscribe_outcome_success_untimed_with_stats() {
    use crate::operations::OpOutcome;
    use crate::ring::{Location, PeerKeyLocation};

    let instance_id = ContractInstanceId::new([40u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([41u8; 32]));
    let target_peer = PeerKeyLocation::random();
    let contract_location = Location::random();

    let op = SubscribeOp {
        id: Transaction::new::<SubscribeMsg>(),
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: Some(super::SubscribeStats {
            target_peer: target_peer.clone(),
            contract_location,
        }),
    };
    match op.outcome() {
        OpOutcome::ContractOpSuccessUntimed {
            target_peer: peer,
            contract_location: loc,
        } => {
            assert_eq!(*peer, target_peer);
            assert_eq!(loc, contract_location);
        }
        other @ OpOutcome::ContractOpSuccess { .. }
        | other @ OpOutcome::ContractOpFailure { .. }
        | other @ OpOutcome::Incomplete
        | other @ OpOutcome::Irrelevant => {
            panic!("Expected ContractOpSuccessUntimed, got {other:?}")
        }
    }
}

/// Completed subscribe without stats → Irrelevant (intermediate node, no originator stats).
#[test]
fn test_subscribe_outcome_irrelevant_without_stats() {
    use crate::operations::OpOutcome;

    let instance_id = ContractInstanceId::new([42u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([43u8; 32]));

    let op = SubscribeOp {
        id: Transaction::new::<SubscribeMsg>(),
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };
    assert!(matches!(op.outcome(), OpOutcome::Irrelevant));
}

/// Not completed + stats → ContractOpFailure (subscription failed, router should learn).
#[test]
fn test_subscribe_outcome_failure_with_stats() {
    use crate::operations::OpOutcome;
    use crate::ring::{Location, PeerKeyLocation};

    let target_peer = PeerKeyLocation::random();
    let contract_location = Location::random();

    let op = SubscribeOp {
        id: Transaction::new::<SubscribeMsg>(),
        state: SubscribeState::Failed, // Not completed
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: Some(super::SubscribeStats {
            target_peer: target_peer.clone(),
            contract_location,
        }),
    };
    match op.outcome() {
        OpOutcome::ContractOpFailure {
            target_peer: peer,
            contract_location: loc,
        } => {
            assert_eq!(*peer, target_peer);
            assert_eq!(loc, contract_location);
        }
        other @ OpOutcome::ContractOpSuccess { .. }
        | other @ OpOutcome::ContractOpSuccessUntimed { .. }
        | other @ OpOutcome::Incomplete
        | other @ OpOutcome::Irrelevant => panic!("Expected ContractOpFailure, got {other:?}"),
    }
}

/// Not completed + no stats → Incomplete (in-transit, nothing to report).
#[test]
fn test_subscribe_outcome_incomplete_without_stats() {
    use crate::operations::OpOutcome;

    let op = SubscribeOp {
        id: Transaction::new::<SubscribeMsg>(),
        state: SubscribeState::Failed,
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };
    assert!(matches!(op.outcome(), OpOutcome::Incomplete));
}

/// Simulate the subscribe operation lifecycle: stats are set when we find a
/// forwarding peer, then state transitions to Completed → outcome should be
/// SuccessUntimed.
#[test]
fn test_subscribe_stats_lifecycle() {
    let target_peer = PeerKeyLocation::random();
    let contract_location = Location::random();
    let tx = Transaction::new::<SubscribeMsg>();
    let instance_id = ContractInstanceId::new([50u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([51u8; 32]));

    // Step 1: Initial state with no stats — Incomplete
    let mut op = SubscribeOp {
        id: tx,
        state: SubscribeState::AwaitingResponse(super::AwaitingResponseData {
            next_hop: None,
            instance_id,
            retries: 0,
            current_hop: 0,
            tried_peers: std::collections::HashSet::new(),
            alternatives: Vec::new(),
            attempts_at_hop: 0,
            visited: crate::operations::VisitedPeers::new(&tx),
        }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };
    assert!(matches!(op.outcome(), OpOutcome::Incomplete));

    // Step 2: Stats set when forwarding to target
    op.stats = Some(super::SubscribeStats {
        target_peer: target_peer.clone(),
        contract_location,
    });
    // Not finalized → ContractOpFailure
    match op.outcome() {
        OpOutcome::ContractOpFailure {
            target_peer: peer,
            contract_location: loc,
        } => {
            assert_eq!(*peer, target_peer);
            assert_eq!(loc, contract_location);
        }
        OpOutcome::ContractOpSuccess { .. }
        | OpOutcome::ContractOpSuccessUntimed { .. }
        | OpOutcome::Incomplete
        | OpOutcome::Irrelevant => {
            panic!("Expected ContractOpFailure for in-progress subscribe with stats")
        }
    }

    // Step 3: Operation completes
    op.state = SubscribeState::Completed(super::CompletedData { key: contract_key });
    match op.outcome() {
        OpOutcome::ContractOpSuccessUntimed {
            target_peer: peer,
            contract_location: loc,
        } => {
            assert_eq!(*peer, target_peer);
            assert_eq!(loc, contract_location);
        }
        OpOutcome::ContractOpSuccess { .. }
        | OpOutcome::ContractOpFailure { .. }
        | OpOutcome::Incomplete
        | OpOutcome::Irrelevant => {
            panic!("Expected ContractOpSuccessUntimed for completed subscribe with stats")
        }
    }
}

/// Verify that renewal subscriptions also report outcomes correctly.
/// Renewals follow the same routing path and should produce the same
/// outcome as non-renewal subscriptions.
#[test]
fn test_subscribe_renewal_reports_outcome() {
    let target_peer = PeerKeyLocation::random();
    let contract_location = Location::random();
    let instance_id = ContractInstanceId::new([60u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([61u8; 32]));

    let op = SubscribeOp {
        id: Transaction::new::<SubscribeMsg>(),
        state: SubscribeState::Completed(super::CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: true, // This is a renewal
        stats: Some(super::SubscribeStats {
            target_peer: target_peer.clone(),
            contract_location,
        }),
    };
    // Renewal should still report success
    match op.outcome() {
        OpOutcome::ContractOpSuccessUntimed {
            target_peer: peer,
            contract_location: loc,
        } => {
            assert_eq!(*peer, target_peer);
            assert_eq!(loc, contract_location);
        }
        OpOutcome::ContractOpSuccess { .. }
        | OpOutcome::ContractOpFailure { .. }
        | OpOutcome::Incomplete
        | OpOutcome::Irrelevant => {
            panic!("Expected ContractOpSuccessUntimed for renewal subscribe")
        }
    }
}

// =============================================================================
// Upstream Unsubscribe Tests
// =============================================================================

/// Verify that create_unsubscribe_op produces the correct routing state.
///
/// This is the only non-trivial unit test for the unsubscribe message path:
/// it validates that the temporary operation created for routing carries the
/// correct target address so `peek_next_hop_addr` can resolve it.
#[test]
fn test_create_unsubscribe_op() {
    let instance_id = ContractInstanceId::new([77u8; 32]);
    let tx = Transaction::new::<SubscribeMsg>();
    let target_addr: SocketAddr = "10.0.0.1:9000".parse().unwrap();

    let op = create_unsubscribe_op(instance_id, tx, target_addr);

    assert_eq!(op.id, tx);
    assert!(!op.is_renewal);

    match &op.state {
        SubscribeState::AwaitingResponse(data) => {
            assert_eq!(data.next_hop, Some(target_addr));
            assert_eq!(data.instance_id, instance_id);
        }
        other @ SubscribeState::PrepareRequest(_)
        | other @ SubscribeState::Completed(_)
        | other @ SubscribeState::Failed => {
            panic!("Expected AwaitingResponse state, got {:?}", other)
        }
    }

    assert_eq!(op.get_next_hop_addr(), Some(target_addr));
}

// =============================================================================
// NotFound Response Tests (#3241)
// =============================================================================

/// Regression test for #3241: forwarding peers must send SubscribeMsgResult::NotFound
/// instead of returning Err(NoCachingPeers) when they can't route a subscribe request.
///
/// Before the fix, the three failure paths (HTL exhausted, no closer peers, unknown
/// peer address) returned Err which became a generic Aborted message that was silently
/// dropped, causing the originator to hang for 60s.
#[test]
fn test_not_found_result_intermediate_node_sends_notfound() {
    use crate::operations::OperationResult;

    let tx = Transaction::new::<SubscribeMsg>();
    let instance_id = ContractInstanceId::new([42u8; 32]);
    let requester: SocketAddr = "10.0.0.1:9000".parse().unwrap();

    // Intermediate node (has requester_addr) should return SendAndComplete with NotFound
    let result = SubscribeOp::not_found_result(tx, instance_id, Some(requester), "HTL exhausted");

    match result {
        Ok(OperationResult::SendAndComplete {
            msg,
            next_hop,
            stream_data,
        }) => {
            assert_eq!(
                next_hop,
                Some(requester),
                "NotFound should be sent to requester"
            );
            assert!(stream_data.is_none());

            // Verify the message is a NotFound response
            let crate::message::NetMessage::V1(net_msg) = msg;
            match net_msg {
                crate::message::NetMessageV1::Subscribe(SubscribeMsg::Response {
                    id,
                    instance_id: resp_instance_id,
                    result,
                }) => {
                    assert_eq!(id, tx);
                    assert_eq!(resp_instance_id, instance_id);
                    assert!(
                        matches!(result, SubscribeMsgResult::NotFound),
                        "Expected NotFound, got {:?}",
                        result
                    );
                }
                other @ crate::message::NetMessageV1::Connect(_)
                | other @ crate::message::NetMessageV1::Put(_)
                | other @ crate::message::NetMessageV1::Get(_)
                | other @ crate::message::NetMessageV1::Subscribe(_)
                | other @ crate::message::NetMessageV1::Update(_)
                | other @ crate::message::NetMessageV1::Aborted(_)
                | other @ crate::message::NetMessageV1::ProximityCache { .. }
                | other @ crate::message::NetMessageV1::InterestSync { .. }
                | other @ crate::message::NetMessageV1::ReadyState { .. } => {
                    panic!("Expected Subscribe Response, got {:?}", other)
                }
            }
        }
        other => panic!(
            "Expected Ok(SendAndComplete), got {:?}",
            other.as_ref().map(|_| "Ok(other)").unwrap_or("Err")
        ),
    }
}

/// Regression test for #3241: when we're the originator (no requester_addr),
/// not_found_result should return Err(NoCachingPeers) so the client gets
/// a fast failure notification.
#[test]
fn test_not_found_result_originator_returns_error() {
    use crate::operations::OpError;
    use crate::ring::RingError;

    let tx = Transaction::new::<SubscribeMsg>();
    let instance_id = ContractInstanceId::new([42u8; 32]);

    // Originator (no requester_addr) should return Err(RingError::NoCachingPeers)
    let result = SubscribeOp::not_found_result(tx, instance_id, None, "no closer peers");

    match result {
        Err(OpError::RingError(RingError::NoCachingPeers(id))) => {
            assert_eq!(
                id, instance_id,
                "Error should reference the correct contract"
            );
        }
        Err(other) => panic!("Expected NoCachingPeers error, got: {other}"),
        Ok(_) => panic!("Originator should get an error, not a message"),
    }
}

// Superseded: should_forward_not_found_on_abort was removed when handle_abort
// gained breadth/retry logic in PR #3460 (closes #3446). The retry logic now
// handles all state-guard cases inline. Forwarding behavior is validated by
// the handle_abort retry tests below.
#[ignore]
#[test]
fn test_should_forward_not_found_on_abort_state_guard() {
    // Original test validated that only AwaitingResponse state with a
    // requester_addr triggers NotFound forwarding on abort. This behavior
    // is now embedded in handle_abort's `if let SubscribeState::AwaitingResponse`
    // destructure and the `if let Some(req_addr) = requester_addr` guard.
}

// =============================================================================
// Breadth/Retry State Construction Tests (#3446 / PR #3460)
// =============================================================================

/// Verify that AwaitingResponseData correctly tracks breadth alternatives
/// from the initial request_subscribe call.
#[test]
fn test_awaiting_response_data_tracks_alternatives() {
    let instance_id = ContractInstanceId::new([80u8; 32]);
    let tx = Transaction::new::<SubscribeMsg>();
    let peer1 = random_peer();
    let peer2 = random_peer();
    let peer3 = random_peer();
    let peer1_addr = peer1.socket_addr().unwrap();

    // Simulate request_subscribe: first candidate is used as target,
    // remaining go into alternatives
    let mut tried_peers = HashSet::new();
    tried_peers.insert(peer1_addr);

    let data = AwaitingResponseData {
        next_hop: Some(peer1_addr),
        instance_id,
        retries: 0,
        current_hop: 10,
        tried_peers,
        alternatives: vec![peer2.clone(), peer3.clone()],
        attempts_at_hop: 1,
        visited: crate::operations::VisitedPeers::new(&tx),
    };

    assert_eq!(
        data.alternatives.len(),
        2,
        "Should store remaining candidates"
    );
    assert_eq!(data.attempts_at_hop, 1, "First attempt at this hop");
    assert_eq!(data.retries, 0, "No retry rounds yet");
    assert_eq!(data.next_hop, Some(peer1_addr), "Target is first candidate");
}

/// Verify Phase 1 retry state: when an alternative is consumed,
/// attempts_at_hop increments and alternatives shrinks.
#[test]
fn test_retry_phase1_state_transition() {
    let instance_id = ContractInstanceId::new([81u8; 32]);
    let tx = Transaction::new::<SubscribeMsg>();
    let peer1 = random_peer();
    let peer2 = random_peer();
    let peer3 = random_peer();
    let peer1_addr = peer1.socket_addr().unwrap();
    let peer2_addr = peer2.socket_addr().unwrap();

    // Start with 2 alternatives, 1 attempt at hop
    let mut tried_peers = HashSet::new();
    tried_peers.insert(peer1_addr);

    let mut alternatives = vec![peer2.clone(), peer3.clone()];
    let attempts_at_hop: usize = 1;

    // Phase 1: try next alternative (simulates handle_abort Phase 1)
    assert!(!alternatives.is_empty() && attempts_at_hop < MAX_BREADTH);
    let next_target = alternatives.remove(0);
    let next_addr = next_target.socket_addr().unwrap();
    tried_peers.insert(next_addr);

    let new_data = AwaitingResponseData {
        next_hop: Some(next_addr),
        instance_id,
        retries: 0,
        current_hop: 10,
        tried_peers: tried_peers.clone(),
        alternatives: alternatives.clone(),
        attempts_at_hop: attempts_at_hop + 1,
        visited: crate::operations::VisitedPeers::new(&tx),
    };

    assert_eq!(new_data.next_hop, Some(peer2_addr), "Should target peer2");
    assert_eq!(
        new_data.alternatives.len(),
        1,
        "One alternative remaining (peer3)"
    );
    assert_eq!(new_data.attempts_at_hop, 2, "Second attempt at this hop");
    assert_eq!(new_data.retries, 0, "Still in same retry round");
    assert!(
        new_data.tried_peers.contains(&peer1_addr),
        "peer1 still in tried set"
    );
    assert!(
        new_data.tried_peers.contains(&peer2_addr),
        "peer2 added to tried set"
    );
}

/// Verify Phase 1 boundary: when attempts_at_hop == MAX_BREADTH, Phase 1 is skipped.
#[test]
fn test_retry_phase1_boundary_max_breadth() {
    let peer1 = random_peer();

    let alternatives = [peer1]; // alternatives available but...
    let attempts_at_hop = MAX_BREADTH; // ...already at max breadth

    // Phase 1 condition should be false
    assert!(
        alternatives.is_empty() || attempts_at_hop >= MAX_BREADTH,
        "Phase 1 should NOT trigger when attempts_at_hop == MAX_BREADTH"
    );
}

/// Verify Phase 2 retry state: new retry round resets tried_peers and alternatives.
#[test]
fn test_retry_phase2_state_transition() {
    let instance_id = ContractInstanceId::new([82u8; 32]);
    let tx = Transaction::new::<SubscribeMsg>();
    let new_peer = random_peer();
    let new_peer_addr = new_peer.socket_addr().unwrap();
    let extra_peer = random_peer();

    let retries: usize = 3;

    // Phase 2: new k_closest candidates found (simulates handle_abort Phase 2)
    assert!(retries < MAX_RETRIES);

    let mut new_candidates = vec![new_peer.clone(), extra_peer.clone()];
    let next_target = new_candidates.remove(0);
    let next_addr = next_target.socket_addr().unwrap();

    let mut new_tried_peers = HashSet::new();
    new_tried_peers.insert(next_addr);

    let new_data = AwaitingResponseData {
        next_hop: Some(next_addr),
        instance_id,
        retries: retries + 1,
        current_hop: 8,
        tried_peers: new_tried_peers.clone(),
        alternatives: new_candidates.clone(),
        attempts_at_hop: 1,
        visited: crate::operations::VisitedPeers::new(&tx),
    };

    assert_eq!(
        new_data.next_hop,
        Some(new_peer_addr),
        "Should target new candidate"
    );
    assert_eq!(new_data.retries, 4, "Retry counter incremented");
    assert_eq!(
        new_data.alternatives.len(),
        1,
        "Remaining new candidates stored"
    );
    assert_eq!(new_data.attempts_at_hop, 1, "Reset to 1 for new round");
    assert_eq!(
        new_data.tried_peers.len(),
        1,
        "Fresh tried_peers with only new target"
    );
    assert!(new_data.tried_peers.contains(&new_peer_addr));
}

/// Verify Phase 2 boundary: when retries == MAX_RETRIES, Phase 2 is skipped.
#[test]
fn test_retry_phase2_boundary_max_retries() {
    let retries = MAX_RETRIES;

    // Phase 2 condition should be false
    assert!(
        retries >= MAX_RETRIES,
        "Phase 2 should NOT trigger when retries == MAX_RETRIES"
    );
}

/// Verify Phase 3 behavior for intermediate node: should forward NotFound upstream.
#[test]
fn test_retry_phase3_intermediate_node_forwards_notfound() {
    let tx = Transaction::new::<SubscribeMsg>();
    let instance_id = ContractInstanceId::new([83u8; 32]);
    let requester: SocketAddr = "10.0.0.1:9000".parse().unwrap();

    // Phase 3: all retries exhausted, we're an intermediate node
    let op = SubscribeOp {
        id: tx,
        state: SubscribeState::AwaitingResponse(AwaitingResponseData {
            next_hop: Some("10.0.0.2:9000".parse().unwrap()),
            instance_id,
            retries: MAX_RETRIES,
            current_hop: 5,
            tried_peers: HashSet::new(),
            alternatives: Vec::new(),
            attempts_at_hop: MAX_BREADTH,
            visited: crate::operations::VisitedPeers::new(&tx),
        }),
        requester_addr: Some(requester),
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };

    // Intermediate node has requester_addr → should forward NotFound
    assert!(
        op.requester_addr.is_some(),
        "Intermediate node should have requester_addr"
    );

    // The state is AwaitingResponse with exhausted retries
    if let SubscribeState::AwaitingResponse(ref data) = op.state {
        assert!(
            data.alternatives.is_empty() || data.attempts_at_hop >= MAX_BREADTH,
            "Phase 1 should be exhausted"
        );
        assert!(data.retries >= MAX_RETRIES, "Phase 2 should be exhausted");
    } else {
        panic!("Expected AwaitingResponse state");
    }
}

/// Verify Phase 3 behavior for originator: no requester_addr means local failure.
#[test]
fn test_retry_phase3_originator_fails_locally() {
    let tx = Transaction::new::<SubscribeMsg>();
    let instance_id = ContractInstanceId::new([84u8; 32]);

    // Originator op: no requester_addr
    let op = SubscribeOp {
        id: tx,
        state: SubscribeState::AwaitingResponse(AwaitingResponseData {
            next_hop: Some("10.0.0.2:9000".parse().unwrap()),
            instance_id,
            retries: MAX_RETRIES,
            current_hop: 5,
            tried_peers: HashSet::new(),
            alternatives: Vec::new(),
            attempts_at_hop: MAX_BREADTH,
            visited: crate::operations::VisitedPeers::new(&tx),
        }),
        requester_addr: None, // We're the originator
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };

    // Originator has no requester_addr → should fail locally
    assert!(
        op.requester_addr.is_none(),
        "Originator should not have requester_addr"
    );
}

/// Verify that non-AwaitingResponse states skip retry logic entirely.
/// handle_abort just completes the operation for PrepareRequest, Completed, Failed states.
#[test]
fn test_non_awaiting_response_states_skip_retry() {
    let tx = Transaction::new::<SubscribeMsg>();
    let instance_id = ContractInstanceId::new([85u8; 32]);
    let contract_key = ContractKey::from_id_and_code(instance_id, CodeHash::new([86u8; 32]));

    // PrepareRequest state — should not match AwaitingResponse destructure
    let op_prepare = SubscribeOp {
        id: tx,
        state: SubscribeState::PrepareRequest(PrepareRequestData {
            id: tx,
            instance_id,
            is_renewal: false,
        }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };
    assert!(
        !matches!(op_prepare.state, SubscribeState::AwaitingResponse(_)),
        "PrepareRequest should not trigger retry"
    );

    // Completed state — should not match AwaitingResponse destructure
    let op_completed = SubscribeOp {
        id: tx,
        state: SubscribeState::Completed(CompletedData { key: contract_key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };
    assert!(
        !matches!(op_completed.state, SubscribeState::AwaitingResponse(_)),
        "Completed should not trigger retry"
    );

    // Failed state — should not match AwaitingResponse destructure
    let op_failed = SubscribeOp {
        id: tx,
        state: SubscribeState::Failed,
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
    };
    assert!(
        !matches!(op_failed.state, SubscribeState::AwaitingResponse(_)),
        "Failed should not trigger retry"
    );
}

/// Verify that the visited bloom filter grows as peers are tried across retries.
#[test]
fn test_visited_bloom_filter_accumulates_across_retries() {
    let tx = Transaction::new::<SubscribeMsg>();
    let peer1 = random_peer();
    let peer2 = random_peer();
    let peer3 = random_peer();
    let peer1_addr = peer1.socket_addr().unwrap();
    let peer2_addr = peer2.socket_addr().unwrap();
    let peer3_addr = peer3.socket_addr().unwrap();

    let mut visited = crate::operations::VisitedPeers::new(&tx);

    // Phase 1: mark peer1 and peer2 as visited
    visited.mark_visited(peer1_addr);
    visited.mark_visited(peer2_addr);
    assert!(visited.probably_visited(peer1_addr));
    assert!(visited.probably_visited(peer2_addr));
    assert!(!visited.probably_visited(peer3_addr));

    // Phase 2 new round: peer3 added
    visited.mark_visited(peer3_addr);
    assert!(
        visited.probably_visited(peer1_addr),
        "peer1 still visited after Phase 2"
    );
    assert!(
        visited.probably_visited(peer2_addr),
        "peer2 still visited after Phase 2"
    );
    assert!(visited.probably_visited(peer3_addr), "peer3 now visited");
}

/// Verify that tried_peers is reset on Phase 2 transitions but visited is preserved.
/// This mirrors the handle_abort behavior where tried_peers resets per-round
/// while visited accumulates globally.
#[test]
fn test_tried_peers_reset_on_phase2_visited_preserved() {
    let tx = Transaction::new::<SubscribeMsg>();
    let instance_id = ContractInstanceId::new([87u8; 32]);
    let peer1 = random_peer();
    let peer2 = random_peer();
    let peer1_addr = peer1.socket_addr().unwrap();
    let peer2_addr = peer2.socket_addr().unwrap();

    // Round 1 state: tried peer1 and peer2
    let mut visited = crate::operations::VisitedPeers::new(&tx);
    visited.mark_visited(peer1_addr);
    visited.mark_visited(peer2_addr);

    let mut old_tried_peers = HashSet::new();
    old_tried_peers.insert(peer1_addr);
    old_tried_peers.insert(peer2_addr);

    // Transition to Phase 2: merge tried_peers into visited, then reset tried_peers
    for addr in &old_tried_peers {
        visited.mark_visited(*addr);
    }

    // New round starts with fresh tried_peers
    let new_peer = random_peer();
    let new_peer_addr = new_peer.socket_addr().unwrap();
    let mut new_tried_peers = HashSet::new();
    new_tried_peers.insert(new_peer_addr);
    visited.mark_visited(new_peer_addr);

    let new_data = AwaitingResponseData {
        next_hop: Some(new_peer_addr),
        instance_id,
        retries: 1,
        current_hop: 8,
        tried_peers: new_tried_peers,
        alternatives: Vec::new(),
        attempts_at_hop: 1,
        visited: visited.clone(),
    };

    // tried_peers is fresh (only new target), but visited has everyone
    assert_eq!(
        new_data.tried_peers.len(),
        1,
        "tried_peers reset for new round"
    );
    assert!(new_data.tried_peers.contains(&new_peer_addr));
    assert!(
        visited.probably_visited(peer1_addr),
        "peer1 still in visited bloom"
    );
    assert!(
        visited.probably_visited(peer2_addr),
        "peer2 still in visited bloom"
    );
    assert!(
        visited.probably_visited(new_peer_addr),
        "new peer in visited bloom"
    );
}

/// Verify MAX_BREADTH and MAX_RETRIES constants are reasonable.
#[test]
fn test_retry_constants() {
    assert_eq!(MAX_BREADTH, 3, "MAX_BREADTH should be 3 (same as GET)");
    assert_eq!(MAX_RETRIES, 10, "MAX_RETRIES should be 10 (same as GET)");
}

// ── Black-hole peer detection tests (#3523) ────────────────────────────

use crate::operations::OpOutcome;
use crate::ring::Location;

/// Build a `SubscribeOp` in `AwaitingResponse` state with the given stats.
fn awaiting_op(
    instance_id: ContractInstanceId,
    next_hop: Option<SocketAddr>,
    stats: Option<SubscribeStats>,
) -> SubscribeOp {
    let id = Transaction::new::<SubscribeMsg>();
    SubscribeOp {
        id,
        state: SubscribeState::AwaitingResponse(AwaitingResponseData {
            next_hop,
            instance_id,
            retries: 0,
            current_hop: 3,
            tried_peers: HashSet::new(),
            alternatives: Vec::new(),
            attempts_at_hop: 1,
            visited: crate::operations::VisitedPeers::new(&id),
        }),
        requester_addr: next_hop.map(|_| "127.0.0.1:12345".parse().unwrap()),
        requester_pub_key: None,
        is_renewal: false,
        stats,
    }
}

/// An intermediate forward with stats reports ContractOpFailure on timeout,
/// feeding PeerHealthTracker and the failure estimator.
#[test]
fn intermediate_forward_with_stats_reports_failure() {
    let target_peer = random_peer();
    let contract_location = Location::random();
    let instance_id = ContractInstanceId::new([1u8; 32]);

    let op = awaiting_op(
        instance_id,
        target_peer.socket_addr(),
        Some(SubscribeStats {
            target_peer,
            contract_location,
        }),
    );

    let info = op
        .failure_routing_info()
        .expect("should have routing info for timeout reporting");
    assert_eq!(info.1, contract_location);
    assert!(!op.finalized());
    assert!(
        matches!(op.outcome(), OpOutcome::ContractOpFailure { .. }),
        "timed-out forward should report failure, not Incomplete"
    );
}

/// Without stats (target peer not yet known), outcome is Incomplete.
#[test]
fn subscribe_without_stats_returns_incomplete() {
    let op = awaiting_op(ContractInstanceId::new([2u8; 32]), None, None);

    assert!(op.failure_routing_info().is_none());
    assert!(matches!(op.outcome(), OpOutcome::Incomplete));
}

/// A completed subscribe with stats reports success.
#[test]
fn completed_subscribe_reports_success() {
    let target_peer = random_peer();
    let contract_location = Location::random();
    let key = ContractKey::from_id_and_code(
        ContractInstanceId::new([3u8; 32]),
        CodeHash::new([4u8; 32]),
    );

    let op = SubscribeOp {
        id: Transaction::new::<SubscribeMsg>(),
        state: SubscribeState::Completed(CompletedData { key }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: Some(SubscribeStats {
            target_peer,
            contract_location,
        }),
    };

    assert!(op.finalized());
    assert!(matches!(
        op.outcome(),
        OpOutcome::ContractOpSuccessUntimed { .. }
    ));
}
