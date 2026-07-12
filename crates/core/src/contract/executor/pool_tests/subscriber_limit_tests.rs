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
    Executor::new_mock_wasm("subscriber_limit_test", storage, None, None)
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

// =========================================================================
// Observability: dropped notification to a registered subscriber (#4681)
// =========================================================================

/// Install a thread-local `tracing` subscriber that records every event as a
/// `"<LEVEL> field=value ..."` string. Returns the captured-message buffer and
/// the default-subscriber guard (drop it to detach the capture).
#[cfg(feature = "trace")]
fn install_log_capture() -> (
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    tracing::subscriber::DefaultGuard,
) {
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Default, Clone)]
    struct Capture(Arc<Mutex<Vec<String>>>);
    impl<S: tracing::Subscriber> Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct V(String);
            impl tracing::field::Visit for V {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write;
                    // Writing into a String is infallible; ignore the Result.
                    write!(self.0, " {}={value:?}", field.name()).ok();
                }
            }
            let mut v = V(String::new());
            event.record(&mut v);
            self.0
                .lock()
                .unwrap()
                .push(format!("{}{}", event.metadata().level(), v.0));
        }
    }

    let capture = Capture::default();
    let messages = capture.0.clone();
    let subscriber = tracing_subscriber::registry().with(capture);
    let guard = tracing::subscriber::set_default(subscriber);
    (messages, guard)
}

/// Regression for #4681 (partial hardening): a committed state install for a
/// contract that has NO subscriber snapshot must emit an observable WARN
/// (carrying the contract `instance_id`) instead of returning silently. The
/// silent early-return was the observability gap that let a cross-node
/// subscriber miss a committed `UpdateNotification` with nothing logged.
///
/// The mock executor has no shared storage, so this exercises the LOCAL-storage
/// (`else`) branch — see the `_shared_*` tests below for the shared branch.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn send_update_notification_without_subscribers_emits_warn() {
    let mut executor = create_executor().await;
    let contract = test_contract(b"warn_no_subscribers_4681");
    let key = contract.key();
    let instance_id = *key.id();

    let (messages, guard) = install_log_capture();

    // Fresh install of a contract with NO registered subscribers hits the
    // missing-snapshot path in `send_update_notification`.
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![1, 2, 3])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");

    drop(guard);

    let logs = messages.lock().unwrap();
    let id_str = instance_id.to_string();
    assert!(
        logs.iter().any(|l| l.starts_with("WARN")
            && l.contains("no subscriber snapshot")
            && l.contains("local storage")
            && l.contains(&id_str)),
        "expected a local-storage WARN naming instance_id {id_str}; captured: {logs:?}"
    );
}

/// Regression for #4681 (main revise item): the empty-but-present case. The
/// channel-closed cleanup inside `send_update_notification` empties a
/// subscriber vec in place (leaving `Some([])` in the map), so the NEXT
/// committed update must still emit a WARN — an empty snapshot is as silent a
/// drop as a missing one, and is the MORE diagnostic case (a subscriber was
/// registered and lost). LOCAL-storage branch.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn send_update_notification_empty_local_snapshot_emits_warn() {
    let mut executor = create_executor().await;
    let contract = test_contract(b"warn_empty_local_4681");
    let key = contract.key();
    let instance_id = *key.id();

    // Simulate the post-cleanup state: an entry that exists but whose
    // subscriber vec has been emptied by the channel-closed `retain`. The
    // matching (empty) `subscriber_summaries` entry mirrors the real cleanup,
    // which retains the summaries map. (`pool_tests` is a descendant module of
    // `executor`, so it may touch these private fields.)
    executor
        .update_notifications
        .insert(instance_id, Vec::new());
    executor
        .subscriber_summaries
        .insert(instance_id, std::collections::HashMap::new());

    let (messages, guard) = install_log_capture();

    // Fresh install still calls `send_update_notification`; it now finds a
    // present-but-empty local snapshot.
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![9, 9, 9])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");

    drop(guard);

    let logs = messages.lock().unwrap();
    let id_str = instance_id.to_string();
    assert!(
        logs.iter().any(|l| l.starts_with("WARN")
            && l.contains("no subscriber snapshot")
            && l.contains("local storage")
            && l.contains(&id_str)),
        "an empty (present) local snapshot must still WARN naming instance_id \
         {id_str}; captured: {logs:?}"
    );
}

/// Regression for #4681: the SHARED-storage branch — the exact path the issue
/// cites (`shared_notifications.get(&instance_id) == None`). A committed update
/// with no shared subscriber entry must emit a shared-storage WARN.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn send_update_notification_missing_shared_snapshot_emits_warn() {
    let mut executor = create_executor().await;
    // Attach empty shared storage so `send_update_notification` takes the
    // shared branch instead of the local one.
    executor.set_shared_notifications(
        std::sync::Arc::new(dashmap::DashMap::new()),
        std::sync::Arc::new(dashmap::DashMap::new()),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );

    let contract = test_contract(b"warn_missing_shared_4681");
    let key = contract.key();
    let instance_id = *key.id();

    let (messages, guard) = install_log_capture();

    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![1, 2, 3])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");

    drop(guard);

    let logs = messages.lock().unwrap();
    let id_str = instance_id.to_string();
    assert!(
        logs.iter().any(|l| l.starts_with("WARN")
            && l.contains("no subscriber snapshot")
            && l.contains("shared storage")
            && l.contains(&id_str)),
        "expected a shared-storage WARN naming instance_id {id_str}; captured: {logs:?}"
    );
}

/// Regression for #4681 (main revise item), SHARED-storage branch: a
/// present-but-EMPTY shared subscriber vec (the state the in-place channel-
/// closed `retain` leaves behind) must still emit a shared-storage WARN rather
/// than snapshotting `Some([])` and returning silently.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn send_update_notification_empty_shared_snapshot_emits_warn() {
    let mut executor = create_executor().await;
    let shared_notifications = std::sync::Arc::new(dashmap::DashMap::new());
    executor.set_shared_notifications(
        shared_notifications.clone(),
        std::sync::Arc::new(dashmap::DashMap::new()),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );

    let contract = test_contract(b"warn_empty_shared_4681");
    let key = contract.key();
    let instance_id = *key.id();

    // Pre-seed a present-but-empty subscriber vec (post-`retain` state).
    shared_notifications.insert(instance_id, Vec::new());

    let (messages, guard) = install_log_capture();

    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![7, 7, 7])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");

    drop(guard);

    let logs = messages.lock().unwrap();
    let id_str = instance_id.to_string();
    assert!(
        logs.iter().any(|l| l.starts_with("WARN")
            && l.contains("no subscriber snapshot")
            && l.contains("shared storage")
            && l.contains(&id_str)),
        "an empty (present) shared snapshot must still WARN naming instance_id \
         {id_str}; captured: {logs:?}"
    );
}
