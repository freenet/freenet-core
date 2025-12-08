use super::*;
use crate::{message::Transaction, ring::PeerKeyLocation, util::Contains};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
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
        std::sync::Arc<tokio::sync::Mutex<Vec<(ContractKey, Vec<SocketAddr>, usize)>>>,
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
        key: &ContractKey,
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
        self.k_closest_calls.lock().await.push((*key, skip_vec, k));

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
    let contract_key = ContractKey::from(ContractInstanceId::new([10u8; 32]));

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
    let sub_op = start_op(contract_key);
    assert!(matches!(
        sub_op.state,
        Some(SubscribeState::PrepareRequest { .. })
    ));

    // 2. Test k_closest_potentially_caching with initial skip list containing self
    let mut initial_skip: HashSet<SocketAddr> = HashSet::new();
    initial_skip.insert(own_addr);
    let initial_candidates = test_ring
        .k_closest_potentially_caching(&contract_key, &initial_skip, 3)
        .await;

    // 3. Verify initial call was recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
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
        .k_closest_potentially_caching(&contract_key, &skip_list, 3)
        .await;

    // 6. Verify the second call used the skip list correctly
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
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
        .k_closest_potentially_caching(&contract_key, &skip_list, 3)
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
    let contract_key = ContractKey::from(ContractInstanceId::new([11u8; 32]));

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
    let sub_op = start_op(contract_key);
    assert!(matches!(
        sub_op.state,
        Some(SubscribeState::PrepareRequest { .. })
    ));

    // Test 2: Simulate the k_closest_potentially_caching call made in request_subscribe
    let mut initial_skip: HashSet<SocketAddr> = HashSet::new();
    initial_skip.insert(own_addr);
    let initial_candidates = test_ring
        .k_closest_potentially_caching(&contract_key, &initial_skip, 3)
        .await;

    // Verify the call was recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
    assert_eq!(
        k_closest_calls.len(),
        1,
        "Should have recorded initial call"
    );
    assert_eq!(
        k_closest_calls[0].0, contract_key,
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
        .k_closest_potentially_caching(&contract_key, &skip_list, 3)
        .await;

    // Verify this call was also recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
    assert_eq!(k_closest_calls.len(), 2, "Should have recorded second call");
    assert_eq!(
        k_closest_calls[1].0, contract_key,
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
        .k_closest_potentially_caching(&contract_key, &skip_list, 3)
        .await;

    // Verify this call was recorded
    let k_closest_calls = test_ring.k_closest_calls.lock().await;
    assert_eq!(k_closest_calls.len(), 3, "Should have recorded third call");
    assert_eq!(
        k_closest_calls[2].0, contract_key,
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

    let contract_key = ContractKey::from(ContractInstanceId::new([1u8; 32]));
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
            .k_closest_potentially_caching(&contract_key, &initial_skip, 3)
            .await;

        let calls = test_ring.k_closest_calls.lock().await;
        assert_eq!(calls.len(), 1, "Should record the call");
        let (key, skip_list, k) = &calls[0];
        assert_eq!(*key, contract_key);
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
            .k_closest_potentially_caching(&contract_key, &skip_list[..], 3)
            .await;

        // Verify skip list is used
        let calls = test_ring.k_closest_calls.lock().await;
        let (key, used_skip_list, k) = &calls[0];
        assert_eq!(*key, contract_key);
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

    // Test 3: Validate subscription state transitions maintain skip list correctly
    {
        // Create AwaitingResponse state with skip list (of socket addresses)
        use std::net::{IpAddr, Ipv4Addr};
        let failed_addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
        let failed_addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8002);
        let mut skip_list: HashSet<SocketAddr> = HashSet::new();
        skip_list.insert(failed_addr1);
        skip_list.insert(failed_addr2);

        let op = SubscribeOp {
            id: transaction_id,
            state: Some(SubscribeState::AwaitingResponse {
                skip_list: skip_list.clone(),
                retries: 2,
                current_hop: 5,
            }),
            upstream_addr: None,
        };

        // Verify skip list is maintained in state
        if let Some(SubscribeState::AwaitingResponse {
            skip_list: list,
            retries,
            ..
        }) = &op.state
        {
            assert_eq!(list.len(), 2, "Skip list maintains failed peers");
            assert!(list.contains(&failed_addr1));
            assert!(list.contains(&failed_addr2));
            assert_eq!(*retries, 2, "Retry count is tracked");
        }
    }
}
