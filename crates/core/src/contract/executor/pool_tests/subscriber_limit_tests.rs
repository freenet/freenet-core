//! Tests for subscriber limit enforcement.
//!
//! Covers:
//! - Per-contract subscriber cap (MAX_SUBSCRIBERS_PER_CONTRACT)
//! - Per-client subscription limit (MAX_SUBSCRIPTIONS_PER_CLIENT)
//! - Sorted-insert correctness
//! - Reconnection does not inflate counts

use freenet_stdlib::prelude::*;

use crate::client_events::ClientId;
use crate::contract::executor::mock_wasm_runtime::MockWasmRuntime;
use crate::contract::executor::{
    ContractExecutor, Executor, MAX_SUBSCRIBERS_PER_CONTRACT, MAX_SUBSCRIPTIONS_PER_CLIENT,
    SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE,
};
use crate::wasm_runtime::MockStateStorage;

/// Helper to create a MockWasmRuntime executor (uses production bridged_* paths).
async fn create_executor() -> Executor<MockWasmRuntime, MockStateStorage> {
    let storage = MockStateStorage::new();
    Executor::new_mock_wasm("subscriber_limit_test", storage, None, None, None)
        .await
        .expect("create executor")
}

/// Helper to create a test contract with a unique seed.
fn test_contract(seed: &[u8]) -> ContractContainer {
    crate::contract::executor::mock_runtime::test::create_test_contract(seed)
}

/// Helper to store a contract so register_contract_notifier can find it.
async fn store_contract(
    executor: &mut Executor<MockWasmRuntime, MockStateStorage>,
    seed: &[u8],
) -> ContractKey {
    let contract = test_contract(seed);
    let key = contract.key();
    let state = WrappedState::new(vec![1]);
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");
    key
}

// =========================================================================
// Per-contract subscriber cap
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_per_contract_subscriber_limit_enforced() {
    let mut executor = create_executor().await;
    let key = store_contract(&mut executor, b"sub_limit_test").await;
    let instance_id = *key.id();

    // Register MAX_SUBSCRIBERS_PER_CONTRACT subscribers — all should succeed
    let mut receivers = Vec::new();
    for _ in 0..MAX_SUBSCRIBERS_PER_CONTRACT {
        let client_id = ClientId::next();
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(instance_id, client_id, tx, None)
            .expect("registration should succeed within limit");
        receivers.push(rx);
    }

    // The next registration should fail
    let extra_client = ClientId::next();
    let (tx, _rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    let result = executor.register_contract_notifier(instance_id, extra_client, tx, None);
    assert!(
        result.is_err(),
        "Registration beyond MAX_SUBSCRIBERS_PER_CONTRACT must fail"
    );

    // Verify the error message mentions subscriber limit
    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("subscriber limit"),
        "Error should mention subscriber limit, got: {err_msg}"
    );
}

// =========================================================================
// Per-client subscription limit
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_per_client_subscription_limit_enforced() {
    let mut executor = create_executor().await;
    let client_id = ClientId::next();

    // Register MAX_SUBSCRIPTIONS_PER_CLIENT subscriptions for the same client
    let mut receivers = Vec::new();
    for i in 0..MAX_SUBSCRIPTIONS_PER_CLIENT {
        let seed = format!("client_limit_test_{i}");
        let key = store_contract(&mut executor, seed.as_bytes()).await;
        let instance_id = *key.id();
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(instance_id, client_id, tx, None)
            .expect("registration should succeed within per-client limit");
        receivers.push(rx);
    }

    // The next registration for the same client should fail
    let extra_key = store_contract(&mut executor, b"client_limit_extra").await;
    let (tx, _rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    let result = executor.register_contract_notifier(*extra_key.id(), client_id, tx, None);
    assert!(
        result.is_err(),
        "Registration beyond MAX_SUBSCRIPTIONS_PER_CLIENT must fail"
    );

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("per-client subscription limit"),
        "Error should mention per-client limit, got: {err_msg}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_per_client_limit_does_not_affect_other_clients() {
    let mut executor = create_executor().await;
    let saturated_client = ClientId::next();
    let other_client = ClientId::next();

    // Saturate one client's subscription limit
    let mut receivers = Vec::new();
    for i in 0..MAX_SUBSCRIPTIONS_PER_CLIENT {
        let seed = format!("other_client_test_{i}");
        let key = store_contract(&mut executor, seed.as_bytes()).await;
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(*key.id(), saturated_client, tx, None)
            .expect("registration should succeed");
        receivers.push(rx);
    }

    // A different client should still be able to subscribe
    let key = store_contract(&mut executor, b"other_client_contract").await;
    let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    executor
        .register_contract_notifier(*key.id(), other_client, tx, None)
        .expect("other client should not be affected by first client's limit");
    receivers.push(rx);
}

// =========================================================================
// Sorted-insert correctness
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_sorted_insert_maintains_order() {
    let mut executor = create_executor().await;
    let key = store_contract(&mut executor, b"sorted_insert_test").await;
    let instance_id = *key.id();

    // Register clients in reverse order to test sorted insertion
    let clients: Vec<ClientId> = (0..10).map(|_| ClientId::next()).collect();
    let mut receivers = Vec::new();

    for &client_id in clients.iter().rev() {
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(instance_id, client_id, tx, None)
            .expect("registration should succeed");
        receivers.push(rx);
    }

    // Verify subscription info returns the correct count
    let subs = executor.get_subscription_info();
    let contract_client_ids: Vec<ClientId> = subs
        .iter()
        .filter(|info| info.instance_id == instance_id)
        .map(|info| info.client_id)
        .collect();

    assert_eq!(contract_client_ids.len(), 10, "Should have 10 subscribers");

    // get_subscription_info iterates the internal Vec, which should be sorted
    // Verify the returned client IDs are in sorted order
    let mut sorted = contract_client_ids.clone();
    sorted.sort();
    assert_eq!(
        contract_client_ids, sorted,
        "Client IDs should be in sorted order from the internal storage"
    );
}

// =========================================================================
// Reconnection (same client, same contract, new channel)
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_reconnection_updates_channel_not_count() {
    let mut executor = create_executor().await;
    let key = store_contract(&mut executor, b"reconnect_test").await;
    let instance_id = *key.id();
    let client_id = ClientId::next();

    // First registration
    let (tx1, _rx1) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    executor
        .register_contract_notifier(instance_id, client_id, tx1, None)
        .expect("first registration should succeed");

    // Re-registration with a new channel (simulates reconnection)
    let (tx2, _rx2) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    executor
        .register_contract_notifier(instance_id, client_id, tx2, None)
        .expect("reconnection should succeed");

    // Should still only have 1 subscriber, not 2
    let subs = executor.get_subscription_info();
    let contract_sub_count = subs
        .iter()
        .filter(|info| info.instance_id == instance_id)
        .count();
    assert_eq!(
        contract_sub_count, 1,
        "Reconnection should update channel, not add duplicate"
    );
}

// =========================================================================
// Reconnection should not inflate per-client count
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_reconnection_does_not_inflate_client_count() {
    let mut executor = create_executor().await;
    let client_id = ClientId::next();

    // Register the client for one contract
    let key = store_contract(&mut executor, b"reconnect_count_test").await;
    let (tx1, _rx1) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    executor
        .register_contract_notifier(*key.id(), client_id, tx1, None)
        .expect("registration should succeed");

    // Reconnect multiple times with new channels — per-client count should stay 1
    for _ in 0..5 {
        let (tx, _rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(*key.id(), client_id, tx, None)
            .expect("reconnection should succeed");
    }

    // Now try to subscribe to more contracts (up to the limit)
    // This should succeed because reconnections didn't inflate the count
    let mut receivers = Vec::new();
    for i in 1..MAX_SUBSCRIPTIONS_PER_CLIENT {
        let seed = format!("reconnect_count_extra_{i}");
        let extra_key = store_contract(&mut executor, seed.as_bytes()).await;
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(*extra_key.id(), client_id, tx, None)
            .unwrap_or_else(|e| {
                panic!("Registration {i} should succeed (reconnections didn't inflate count): {e}")
            });
        receivers.push(rx);
    }
}
