//! Tests for subscriber limit enforcement.
//!
//! Covers:
//! - Per-contract subscriber cap (MAX_SUBSCRIBERS_PER_CONTRACT)
//! - Per-client subscription limit (MAX_SUBSCRIPTIONS_PER_CLIENT)
//! - Sorted-insert correctness
//! - Reconnection does not inflate counts

use freenet_stdlib::client_api::ContractResponse;
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

/// Regression test: the subscriber-limit error must carry the REAL
/// `ContractKey` (correct `CodeHash`), not a synthetic one with a zeroed
/// `CodeHash`. Before the fix, `subscriber_limit_error` fabricated the key
/// from the `ContractInstanceId` alone with an all-zero `CodeHash`, so a
/// client matching on the returned key could never match it against the
/// contract it actually subscribed to.
#[tokio::test(flavor = "current_thread")]
async fn test_subscriber_limit_error_carries_real_contract_key() {
    use freenet_stdlib::client_api::{ContractError as StdContractError, RequestError};

    let mut executor = create_executor().await;
    let key = store_contract(&mut executor, b"sub_limit_key_test").await;
    let instance_id = *key.id();

    // Saturate the per-contract subscriber limit.
    let mut receivers = Vec::new();
    for _ in 0..MAX_SUBSCRIBERS_PER_CONTRACT {
        let client_id = ClientId::next();
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(instance_id, client_id, tx, None)
            .expect("registration should succeed within limit");
        receivers.push(rx);
    }

    let extra_client = ClientId::next();
    let (tx, _rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    let err = executor
        .register_contract_notifier(instance_id, extra_client, tx, None)
        .expect_err("registration beyond MAX_SUBSCRIBERS_PER_CONTRACT must fail");

    // The catch-all below exists only to fail loudly on any other
    // `RequestError` variant (including future ones `RequestError` being
    // `#[non_exhaustive]` might add) — not to match them meaningfully.
    #[allow(clippy::wildcard_enum_match_arm)]
    match *err {
        RequestError::ContractError(StdContractError::Subscribe { key: err_key, .. }) => {
            assert_eq!(
                err_key, key,
                "subscriber-limit error must carry the real ContractKey \
                 (matching CodeHash) so the client can identify the refused \
                 contract; got a different key: {err_key}"
            );
            assert_ne!(
                err_key.code_hash(),
                &CodeHash::new([0u8; 32]),
                "subscriber-limit error regressed to a synthetic zeroed \
                 CodeHash"
            );
        }
        ref other => {
            panic!("expected RequestError::ContractError(Subscribe {{ .. }}), got: {other:?}")
        }
    }
}

/// Same regression as above, for the per-client subscription limit path
/// (the second `subscriber_limit_error` call site in
/// `bridged_register_contract_notifier`).
#[tokio::test(flavor = "current_thread")]
async fn test_per_client_limit_error_carries_real_contract_key() {
    use freenet_stdlib::client_api::{ContractError as StdContractError, RequestError};

    let mut executor = create_executor().await;
    let client_id = ClientId::next();

    let mut receivers = Vec::new();
    for i in 0..MAX_SUBSCRIPTIONS_PER_CLIENT {
        let seed = format!("client_limit_key_test_{i}");
        let key = store_contract(&mut executor, seed.as_bytes()).await;
        let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(*key.id(), client_id, tx, None)
            .expect("registration should succeed within per-client limit");
        receivers.push(rx);
    }

    let extra_key = store_contract(&mut executor, b"client_limit_key_extra").await;
    let (tx, _rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    let err = executor
        .register_contract_notifier(*extra_key.id(), client_id, tx, None)
        .expect_err("registration beyond MAX_SUBSCRIPTIONS_PER_CLIENT must fail");

    // The catch-all below exists only to fail loudly on any other
    // `RequestError` variant (including future ones `RequestError` being
    // `#[non_exhaustive]` might add) — not to match them meaningfully.
    #[allow(clippy::wildcard_enum_match_arm)]
    match *err {
        RequestError::ContractError(StdContractError::Subscribe { key: err_key, .. }) => {
            // `ContractKey`'s `PartialEq` compares only the instance id, not
            // the code hash (see `freenet_stdlib::prelude::ContractKey`), so
            // `err_key == extra_key` alone would pass even with a fabricated
            // zeroed-CodeHash key — the instance id was never the broken
            // part. Assert the code hash explicitly.
            assert_eq!(
                err_key, extra_key,
                "per-client-limit error must carry the real ContractKey of \
                 the refused contract, got: {err_key}"
            );
            assert_eq!(
                err_key.code_hash(),
                extra_key.code_hash(),
                "per-client-limit error must carry the real CodeHash, not a \
                 synthetic zeroed one — got: {:?}, expected: {:?}",
                err_key.code_hash(),
                extra_key.code_hash()
            );
        }
        ref other => {
            panic!("expected RequestError::ContractError(Subscribe {{ .. }}), got: {other:?}")
        }
    }
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
///
/// The mechanics live in [`crate::util::test_log_capture`] because a
/// thread-local capture alone is order-dependent: a concurrently-running test
/// that is the first to reach one of the callsites asserted on here can pin its
/// process-global `tracing` `Interest` to `never`, and the event then never
/// arrives (#4927). Every test in this file asserting on captured logs depends
/// on that helper's fix — do not inline a bare `set_default` capture back here.
#[cfg(feature = "trace")]
fn install_log_capture() -> (
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    tracing::subscriber::DefaultGuard,
) {
    crate::util::test_log_capture::install()
}

/// Re-scope of #4681 by #5040: an ABSENT subscriber entry must NOT warn.
///
/// #4773 deliberately treated "missing" and "empty" snapshots uniformly and
/// warned on both. In production the missing case is the ordinary steady state
/// for any contract a node hosts for the NETWORK with no local client attached
/// (the network mesh is fed by `broadcast_state_change`, not by this local
/// fan-out), so nothing is undelivered. Warning on it produced 22,413 lines in
/// one day on a single node — 96.6% of them for one zero-subscriber contract —
/// which buried the empty-entry case that #4681 actually exists to surface.
/// PR #4773's own reviewer note predicted this ("a follow-up could rate-limit
/// it if it proves noisy").
///
/// The mock executor has no shared storage, so this exercises the LOCAL-storage
/// (`else`) branch — see the `_shared_*` tests below for the shared branch.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn send_update_notification_absent_local_snapshot_does_not_warn() {
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
        !logs
            .iter()
            .any(|l| l.starts_with("WARN") && l.contains("no subscriber snapshot")),
        "an ABSENT local snapshot must not WARN (nothing is owed locally); \
         captured: {logs:?}"
    );
    // The condition remains observable at DEBUG *in debug builds only*.
    // `release_max_level_info` (crates/core/Cargo.toml) compiles `debug!` out of
    // release, so in a shipped binary the absent case is silent, not merely
    // quiet. That is deliberate — it is the steady state and carries no operator
    // action — but it means this assertion pins a line production does not
    // emit, and a log grep can no longer tell "no subscriber registered" from
    // "never reached". See #5040.
    assert!(
        logs.iter().any(|l| l.starts_with("DEBUG")
            && l.contains("no local subscriber")
            && l.contains("local storage")
            && l.contains(&id_str)),
        "expected a local-storage DEBUG naming instance_id {id_str}; captured: {logs:?}"
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
    drop(logs);
    // Deterministic, non-log discriminator. This test and its shared twin were
    // the only two carried entirely by a positive log assertion, which is what
    // #4927 flakes on (a dropped capture reads as a missing WARN). The entry
    // removal is observable in the map, so a #4927-class flake can no longer
    // turn either of them into a silent false result.
    assert!(
        !executor.update_notifications.contains_key(&instance_id),
        "the emptied local entry must also be dropped"
    );
}

/// Re-scope of #4681 by #5040, SHARED-storage branch: an ABSENT shared
/// subscriber entry must NOT warn. This is the exact production path that
/// produced the 22k/day storm (`shared_notifications.get(&instance_id) ==
/// None` for a network-hosted contract with no local client). See the
/// local-storage sibling above for the full rationale.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn send_update_notification_absent_shared_snapshot_does_not_warn() {
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
        !logs
            .iter()
            .any(|l| l.starts_with("WARN") && l.contains("no subscriber snapshot")),
        "an ABSENT shared snapshot must not WARN (nothing is owed locally); \
         captured: {logs:?}"
    );
    assert!(
        logs.iter().any(|l| l.starts_with("DEBUG")
            && l.contains("no local subscriber")
            && l.contains("shared storage")
            && l.contains(&id_str)),
        "expected a shared-storage DEBUG naming instance_id {id_str}; captured: {logs:?}"
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
    // The corrected wording is part of the fix: the old text claimed a delivery
    // was being dropped at this instant, when the subscriber was actually lost
    // on an earlier update and already reported by the per-client ERROR. Pinned
    // so a revert to the misleading phrasing fails.
    assert!(
        logs.iter()
            .any(|l| l.starts_with("WARN") && l.contains("stale entry dropped")),
        "the WARN must say the entry was stale, not that a delivery was just \
         dropped; captured: {logs:?}"
    );
    drop(logs);
    // Deterministic, non-log discriminator — see the local twin above.
    assert!(
        !shared_notifications.contains_key(&instance_id),
        "the emptied shared entry must also be dropped"
    );
}

/// Regression for #5040 — the storm itself, and the assertion that actually
/// fails without the fix.
///
/// The empty-but-present entry was pruned only opportunistically (by the next
/// `RuntimePool::remove_client` from ANY client), so until that happened EVERY
/// committed update on the contract re-emitted the WARN. On a contract taking
/// sustained update traffic with no local subscriber that is unbounded: the
/// reporting node logged 22,413 lines in a day, 96.6% for one contract.
///
/// The loss is worth exactly one line — it is already reported per-client as an
/// ERROR at the point of loss — so the entry is dropped when it goes empty and
/// every subsequent update reads as ABSENT (DEBUG). Two committed updates must
/// therefore produce ONE WARN, not two.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn empty_shared_snapshot_warns_once_not_on_every_update() {
    let mut executor = create_executor().await;
    let shared_notifications = std::sync::Arc::new(dashmap::DashMap::new());
    let shared_summaries = std::sync::Arc::new(dashmap::DashMap::new());
    executor.set_shared_notifications(
        shared_notifications.clone(),
        shared_summaries.clone(),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );

    let contract = test_contract(b"warn_once_5040");
    let key = contract.key();
    let instance_id = *key.id();

    // Post-`retain` state: entry present, subscriber vec emptied in place.
    //
    // The summaries sibling is seeded NON-EMPTY on purpose. That is the shape
    // the real cleanup leaves behind (the local failure path historically never
    // pruned per-client summaries), and it is the shape that catches an
    // orphaned summaries entry. Seeding an empty map instead makes the
    // `!contains_key` assertion below pass no matter what the production code
    // does, because a conditional `remove_if(.., is_empty)` would clear it
    // anyway — a vacuous test that reports success without exercising the fix.
    let dead_client = ClientId::next();
    shared_notifications.insert(instance_id, Vec::new());
    shared_summaries.insert(
        instance_id,
        std::collections::HashMap::from([(dead_client, None)]),
    );

    let (messages, guard) = install_log_capture();

    // Two committed updates over the same empty entry.
    for state in [vec![1u8, 1, 1], vec![2u8, 2, 2]] {
        executor
            .upsert_contract_state(
                key,
                either::Either::Left(WrappedState::new(state)),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .expect("store contract");
    }

    drop(guard);

    let logs = messages.lock().unwrap();
    let warns = logs
        .iter()
        .filter(|l| l.starts_with("WARN") && l.contains("no subscriber snapshot"))
        .count();
    assert_eq!(
        warns, 1,
        "an emptied subscriber entry must WARN exactly once, not once per \
         committed update (#5040); captured: {logs:?}"
    );

    // The entry (and its summaries sibling) is gone, so the next update reads
    // as ABSENT rather than re-warning — this is what bounds the storm.
    assert!(
        !shared_notifications.contains_key(&instance_id),
        "the emptied subscriber entry must be removed once observed"
    );
    assert!(
        !shared_summaries.contains_key(&instance_id),
        "the emptied summaries sibling must be removed alongside it"
    );
}

/// Regression for #5040, source half: the entry is dropped AS it empties, in
/// the channel-closed `retain` itself, not merely when a later update happens
/// to observe it.
///
/// This is the path that actually creates the empty entry in production (a
/// subscriber's receiver goes away without a Disconnect reaching the handler).
/// The loss is worth exactly one line and already has one: the per-client
/// ERROR at the point of loss. Dropping the entry there means no subsequent
/// update warns about it at all.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn closed_subscriber_channel_drops_entry_at_point_of_loss() {
    let mut executor = create_executor().await;
    let shared_notifications = std::sync::Arc::new(dashmap::DashMap::new());
    let shared_summaries = std::sync::Arc::new(dashmap::DashMap::new());
    executor.set_shared_notifications(
        shared_notifications.clone(),
        shared_summaries.clone(),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );

    let contract = test_contract(b"closed_channel_5040");
    let key = contract.key();
    let instance_id = *key.id();

    // A registered subscriber whose receiver has been dropped: the notification
    // send fails `Closed`, which is what empties the vec in place.
    let client_id = ClientId::next();
    let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    drop(rx);
    shared_notifications.insert(instance_id, vec![(client_id, tx)]);
    shared_summaries.insert(
        instance_id,
        std::collections::HashMap::from([(client_id, None)]),
    );

    let (messages, guard) = install_log_capture();

    for state in [vec![1u8, 1, 1], vec![2u8, 2, 2]] {
        executor
            .upsert_contract_state(
                key,
                either::Either::Left(WrappedState::new(state)),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .expect("store contract");
    }

    drop(guard);

    let logs = messages.lock().unwrap();
    // The loss is reported once, at the point of loss, naming the client.
    assert_eq!(
        logs.iter()
            .filter(|l| l.starts_with("ERROR") && l.contains("channel closed"))
            .count(),
        1,
        "a closed subscriber channel must be reported once as an ERROR; \
         captured: {logs:?}"
    );
    // The second update must not warn: the entry was already dropped.
    assert!(
        !logs
            .iter()
            .any(|l| l.starts_with("WARN") && l.contains("no subscriber snapshot")),
        "dropping the entry at the point of loss must leave nothing for a later \
         update to warn about; captured: {logs:?}"
    );
    assert!(
        !shared_notifications.contains_key(&instance_id),
        "the subscriber entry must be removed as it empties"
    );
    assert!(
        !shared_summaries.contains_key(&instance_id),
        "the summaries sibling must be removed alongside it"
    );
}

/// Regression for #5040 — the LOCAL-storage twin of the test above.
///
/// Review found the source fix had landed only on the shared branch while the
/// comments and PR body claimed both. For the identical sequence the local
/// branch emitted one extra WARN (the entry survived to the next update) and
/// leaked the dead client's summary, because its channel-closed cleanup only
/// ran `retain` — it never pruned `subscriber_summaries` and never dropped the
/// emptied entry. The pooled executor is what production uses, but this branch
/// is what the mock/simulation harness runs, so it was untested in both senses.
#[cfg(feature = "trace")]
#[tokio::test(flavor = "current_thread")]
async fn closed_subscriber_channel_drops_entry_at_point_of_loss_local() {
    let mut executor = create_executor().await;
    let contract = test_contract(b"closed_channel_local_5040");
    let key = contract.key();
    let instance_id = *key.id();

    // No shared storage installed, so this takes the local branch.
    let client_id = ClientId::next();
    let (tx, rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    drop(rx);
    executor
        .update_notifications
        .insert(instance_id, vec![(client_id, tx)]);
    executor.subscriber_summaries.insert(
        instance_id,
        std::collections::HashMap::from([(client_id, None)]),
    );

    let (messages, guard) = install_log_capture();

    for state in [vec![1u8, 1, 1], vec![2u8, 2, 2]] {
        executor
            .upsert_contract_state(
                key,
                either::Either::Left(WrappedState::new(state)),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .expect("store contract");
    }

    drop(guard);

    let logs = messages.lock().unwrap();
    assert_eq!(
        logs.iter()
            .filter(|l| l.starts_with("ERROR") && l.contains("channel closed"))
            .count(),
        1,
        "a closed subscriber channel must be reported once as an ERROR on the \
         local branch too; captured: {logs:?}"
    );
    assert!(
        !logs
            .iter()
            .any(|l| l.starts_with("WARN") && l.contains("no subscriber snapshot")),
        "the local branch must also drop the entry at the point of loss, leaving \
         nothing for the next update to warn about; captured: {logs:?}"
    );
    assert!(
        !executor.update_notifications.contains_key(&instance_id),
        "the local subscriber entry must be removed as it empties"
    );
    assert!(
        !executor.subscriber_summaries.contains_key(&instance_id),
        "the local summaries sibling must be removed alongside it"
    );
}

/// Regression for #5040: PARTIAL failure — one subscriber dies, one survives.
///
/// This is the only shape in which the per-client summaries prune matters, and
/// therefore the only shape that can catch its absence. Every other test here
/// uses all-or-nothing failure, where the whole-entry cleanup removes the
/// summaries map anyway and so masks a missing per-client prune entirely (the
/// prune was a surviving mutant until this test existed).
///
/// With a survivor present the vec stays non-empty, the entry is never dropped,
/// and a dead client's summary would persist for the lifetime of the contract.
/// Also the only test asserting a live subscriber actually RECEIVES its
/// notification, which pins that the fan-out path still works at all.
///
/// No `trace` gate: this asserts on map state and a delivered notification,
/// not on captured logs.
#[tokio::test(flavor = "current_thread")]
async fn partial_failure_prunes_only_the_dead_subscriber_local() {
    let mut executor = create_executor().await;
    let contract = test_contract(b"partial_failure_5040");
    let key = contract.key();
    let instance_id = *key.id();

    let dead_client = ClientId::next();
    let live_client = ClientId::next();
    let (dead_tx, dead_rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    let (live_tx, mut live_rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    drop(dead_rx);

    // Sorted by ClientId, matching the production insert order.
    let mut subs = vec![(dead_client, dead_tx), (live_client, live_tx)];
    subs.sort_by_key(|(c, _)| *c);
    executor.update_notifications.insert(instance_id, subs);
    executor.subscriber_summaries.insert(
        instance_id,
        std::collections::HashMap::from([(dead_client, None), (live_client, None)]),
    );

    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![5, 5, 5])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");

    // The dead subscriber is pruned from BOTH maps; the survivor is untouched.
    let notifiers = executor
        .update_notifications
        .get(&instance_id)
        .expect("entry must survive — a live subscriber remains");
    assert_eq!(
        notifiers.len(),
        1,
        "only the dead subscriber may be removed from the notifier list"
    );
    assert_eq!(
        notifiers[0].0, live_client,
        "the surviving subscriber must be the live one"
    );

    let summaries = executor
        .subscriber_summaries
        .get(&instance_id)
        .expect("summaries entry must survive alongside the notifier entry");
    assert!(
        !summaries.contains_key(&dead_client),
        "the dead subscriber's summary must be pruned — otherwise it leaks for \
         the lifetime of the contract, which is exactly what the per-client \
         prune exists to prevent"
    );
    assert!(
        summaries.contains_key(&live_client),
        "the live subscriber's summary must NOT be collaterally removed"
    );

    // And the survivor actually got its notification.
    match live_rx.try_recv() {
        Ok(Ok(resp)) => {
            let as_str = format!("{resp:?}");
            assert!(
                as_str.contains("UpdateNotification"),
                "the live subscriber must receive an UpdateNotification; got: {as_str}"
            );
        }
        other => panic!("the live subscriber must receive its notification; got: {other:?}"),
    }
}

/// The SHARED-storage twin of the partial-failure test above — the branch
/// production actually runs (`handler.rs` wires `ContractExecutor =
/// RuntimePool`, and every pooled executor gets shared storage).
///
/// Same reasoning: the shared per-client summaries prune is masked by
/// all-or-nothing failure, because the whole-entry cleanup removes the
/// summaries map anyway. Only a surviving subscriber exposes it.
#[tokio::test(flavor = "current_thread")]
async fn partial_failure_prunes_only_the_dead_subscriber_shared() {
    let mut executor = create_executor().await;
    let shared_notifications = std::sync::Arc::new(dashmap::DashMap::new());
    let shared_summaries = std::sync::Arc::new(dashmap::DashMap::new());
    executor.set_shared_notifications(
        shared_notifications.clone(),
        shared_summaries.clone(),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );

    let contract = test_contract(b"partial_failure_shared_5040");
    let key = contract.key();
    let instance_id = *key.id();

    let dead_client = ClientId::next();
    let live_client = ClientId::next();
    let (dead_tx, dead_rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    let (live_tx, mut live_rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    drop(dead_rx);

    let mut subs = vec![(dead_client, dead_tx), (live_client, live_tx)];
    subs.sort_by_key(|(c, _)| *c);
    shared_notifications.insert(instance_id, subs);
    shared_summaries.insert(
        instance_id,
        std::collections::HashMap::from([(dead_client, None), (live_client, None)]),
    );

    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![6, 6, 6])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");

    let notifiers = shared_notifications
        .get(&instance_id)
        .expect("entry must survive — a live subscriber remains");
    assert_eq!(
        notifiers.len(),
        1,
        "only the dead subscriber may be removed from the notifier list"
    );
    assert_eq!(
        notifiers[0].0, live_client,
        "the surviving subscriber must be the live one"
    );
    drop(notifiers);

    let summaries = shared_summaries
        .get(&instance_id)
        .expect("summaries entry must survive alongside the notifier entry");
    assert!(
        !summaries.contains_key(&dead_client),
        "the dead subscriber's summary must be pruned on the shared branch too"
    );
    assert!(
        summaries.contains_key(&live_client),
        "the live subscriber's summary must NOT be collaterally removed"
    );
    drop(summaries);

    match live_rx.try_recv() {
        Ok(Ok(resp)) => {
            let as_str = format!("{resp:?}");
            assert!(
                as_str.contains("UpdateNotification"),
                "the live subscriber must receive an UpdateNotification; got: {as_str}"
            );
        }
        other => panic!("the live subscriber must receive its notification; got: {other:?}"),
    }
}

/// Regression for #4681 mechanism 2: a notification dropped because the
/// subscriber's channel was FULL must invalidate that client's cached summary,
/// so the NEXT update resyncs it with full state instead of another delta.
///
/// Why this is a correctness bug and not just staleness: deltas are
/// incremental. A missed delta is never made up by subsequent deltas, so
/// without the invalidation the subscriber is permanently DIVERGED — silently
/// wrong, not merely behind. The old code dropped the notification with a WARN
/// and no recovery of any kind.
///
/// The mock runtime returns the full state as its "delta", so the payload bytes
/// are identical either way; the discriminator is the `UpdateData` VARIANT.
#[tokio::test(flavor = "current_thread")]
async fn full_channel_drop_forces_full_state_resync_local() {
    let mut executor = create_executor().await;
    let contract = test_contract(b"full_channel_resync_4681");
    let key = contract.key();
    let instance_id = *key.id();

    let client_id = ClientId::next();
    let (tx, mut rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    executor
        .update_notifications
        .insert(instance_id, vec![(client_id, tx.clone())]);
    // A cached summary is what makes this client eligible for DELTAS.
    executor.subscriber_summaries.insert(
        instance_id,
        std::collections::HashMap::from([(
            client_id,
            Some(StateSummary::from(vec![1u8, 2, 3]).into_owned()),
        )]),
    );

    // Saturate the channel so the next try_send returns Full.
    for _ in 0..SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE {
        tx.try_send(Ok(freenet_stdlib::client_api::HostResponse::Ok))
            .expect("prefill the subscriber channel to capacity");
    }

    // Update 1: dropped, because the channel is full.
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![7, 7, 7])),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("store contract");

    assert!(
        executor
            .subscriber_summaries
            .get(&instance_id)
            .and_then(|s| s.get(&client_id))
            .expect("the client must stay registered")
            .is_none(),
        "a full-channel drop must invalidate the cached summary, otherwise the \
         next update is another delta and the client stays diverged forever"
    );

    // Drain, so the next notification can actually be delivered.
    while rx.try_recv().is_ok() {}

    // Update 2: must be FULL STATE, not a delta.
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![8, 8, 8])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");

    let received = rx.try_recv().expect("the resync notification must arrive");
    match received {
        Ok(freenet_stdlib::client_api::HostResponse::ContractResponse(
            ContractResponse::UpdateNotification { update, .. },
        )) => {
            assert!(
                matches!(update, UpdateData::State(_)),
                "after a full-channel drop the client must be resynced with FULL \
                 STATE; got {update:?}, which leaves it diverged"
            );
        }
        other => panic!("expected an UpdateNotification; got {other:?}"),
    }
}

/// The SHARED-storage twin of the full-channel resync test — the arm
/// production actually runs (`handler.rs` wires `ContractExecutor =
/// RuntimePool`, and every pooled executor gets shared storage).
///
/// The fix was applied identically to two independently-buggable arms, so a
/// regression in the shared one would otherwise reach production uncaught.
#[tokio::test(flavor = "current_thread")]
async fn full_channel_drop_forces_full_state_resync_shared() {
    let mut executor = create_executor().await;
    let shared_notifications = std::sync::Arc::new(dashmap::DashMap::new());
    let shared_summaries = std::sync::Arc::new(dashmap::DashMap::new());
    executor.set_shared_notifications(
        shared_notifications.clone(),
        shared_summaries.clone(),
        std::sync::Arc::new(dashmap::DashMap::new()),
    );

    let contract = test_contract(b"full_channel_resync_shared_4681");
    let key = contract.key();
    let instance_id = *key.id();

    let client_id = ClientId::next();
    let (tx, mut rx) = tokio::sync::mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    shared_notifications.insert(instance_id, vec![(client_id, tx.clone())]);
    // A cached summary is what makes this client eligible for DELTAS.
    shared_summaries.insert(
        instance_id,
        std::collections::HashMap::from([(
            client_id,
            Some(StateSummary::from(vec![1u8, 2, 3]).into_owned()),
        )]),
    );

    for _ in 0..SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE {
        tx.try_send(Ok(freenet_stdlib::client_api::HostResponse::Ok))
            .expect("prefill the subscriber channel to capacity");
    }

    // Update 1: dropped, channel full.
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![7, 7, 7])),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("store contract");

    assert!(
        shared_summaries
            .get(&instance_id)
            .and_then(|s| s.get(&client_id).cloned())
            .expect("the client must stay registered")
            .is_none(),
        "a full-channel drop must invalidate the cached summary on the SHARED \
         arm too, otherwise the next update is another delta and the client \
         stays diverged forever"
    );

    while rx.try_recv().is_ok() {}

    // Update 2: must be FULL STATE, not a delta.
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(WrappedState::new(vec![8, 8, 8])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("store contract");

    let received = rx.try_recv().expect("the resync notification must arrive");
    match received {
        Ok(freenet_stdlib::client_api::HostResponse::ContractResponse(
            ContractResponse::UpdateNotification { update, .. },
        )) => {
            assert!(
                matches!(update, UpdateData::State(_)),
                "after a full-channel drop the client must be resynced with FULL \
                 STATE; got {update:?}, which leaves it diverged"
            );
        }
        other => panic!("expected an UpdateNotification; got {other:?}"),
    }
}

/// Source-scrape pin for the #4681 delivery counters.
///
/// Why a source pin: every recorder call is gated behind
/// `if let Some(op_manager) = self.op_manager.as_ref()`, and every executor in
/// this suite is built by `Executor::new_mock_wasm(.., None, None)` — i.e.
/// `op_manager = None`. So no behavioral test here can enter the counting
/// branch, and removing all three calls would leave the suite green. The
/// gauge test pins only JSON serialization of hand-stamped fields; it never
/// runs the counting logic.
///
/// The PR's own claim is that these counters are the only production visibility
/// into local notification delivery, so an untested recording path is exactly
/// the gap worth pinning. Asserts each recorder is called from the arm that
/// owns it, in BOTH the shared and local fan-out branches.
#[test]
fn notification_delivery_counters_are_recorded_from_their_arms() {
    let whole = include_str!("../runtime/executor_impl.rs");
    let squash = |s: &str| -> String { s.chars().filter(|c| !c.is_whitespace()).collect() };

    // Scope to `send_update_notification`. The file has a THIRD
    // `TrySendError::Full` arm, on the delegate-notification path, which is a
    // different concern with its own counter — an unscoped scrape picks it up
    // and the pin fails on correct code.
    let fn_start = whole
        .find("async fn send_update_notification(")
        .expect("send_update_notification not found");
    let fn_end = whole[fn_start..]
        .find("mod full_state_version_gate_pins")
        .expect("end-of-function anchor not found")
        + fn_start;
    let src = &whole[fn_start..fn_end];

    // Each `Full` arm runs from its match pattern to the `Closed` pattern that
    // follows it. Two fan-out branches => two of each.
    let full_arms: Vec<&str> = src
        .match_indices("Err(mpsc::error::TrySendError::Full(_)) => {")
        .map(|(i, _)| {
            let rest = &src[i..];
            let end = rest
                .find("Err(mpsc::error::TrySendError::Closed(_))")
                .expect("a Full arm must be followed by the Closed arm");
            &rest[..end]
        })
        .collect();
    assert_eq!(
        full_arms.len(),
        2,
        "expected the shared and local fan-out branches to each have a Full arm"
    );
    for arm in &full_arms {
        assert!(
            squash(arm).contains("record_notification_dropped_channel_full()"),
            "each full-channel drop must be counted; without it the only \
             production visibility into this drop disappears silently"
        );
    }

    assert_eq!(
        squash(src)
            .matches("record_notification_dropped_channel_closed()")
            .count(),
        2,
        "both fan-out branches must count closed-channel drops"
    );
    assert_eq!(
        squash(src)
            .matches("record_notification_no_local_subscriber()")
            .count(),
        2,
        "both fan-out branches must count the no-local-subscriber case — this is \
         the counter that replaced the diagnostic #5040 had to remove"
    );
}

/// Source-scrape pin for the #5040 TOCTOU fix in
/// `RuntimePool::register_contract_notifier`.
///
/// Why a source pin and not a behavioral test: nothing in the suite constructs
/// a real `RuntimePool` (its only `new` call site is production wiring in
/// `handler.rs`, needing a Config + OpManager). Every `register_contract_notifier`
/// test in this file builds an `Executor`, which resolves to the *other* impl
/// — `bridged_register_contract_notifier` — so the pool path is unreachable
/// from here and reverting the fix would leave the whole suite green.
///
/// The invariant: the already-registered path acquires the
/// `shared_notifications` guard EXACTLY ONCE, doing the binary search and the
/// channel write under that same guard. The split version computed the index
/// under a read guard, released it, then re-acquired `get_mut` — a window in
/// which the entry can shrink (out-of-bounds panic) or vanish entirely
/// (silently skipped write, so the client gets a successful subscribe that
/// never delivers — the failure mode #5040's entry-dropping introduced).
///
/// Whitespace-insensitive so rustfmt cannot break it.
#[test]
fn register_contract_notifier_takes_one_guard_for_search_and_write() {
    let src = include_str!("../runtime/pool.rs");
    // Slice the `already_registered` STATEMENT itself, bounded by two code
    // anchors (both unique in pool.rs), not by prose and not by a wider region.
    //
    // An earlier revision of this pin ended at `MAX_SUBSCRIBERS_PER_CONTRACT`,
    // reasoning that API surface beats a comment as an anchor. It does, but
    // that anchor sits PAST the `else` branch's own `shared_notifications`
    // read, so the region swallowed a second, legitimate acquisition and the
    // count assertion failed on correct code. Bounding the statement exactly
    // is both stabler and narrower than either.
    let start = src
        .find("let already_registered =")
        .expect("`already_registered` binding not found in pool.rs");
    let after = &src[start..];
    let end = after
        .find("if let Some(same_channel)")
        .expect("`if let Some(same_channel)` not found after the binding");
    let already_registered: String = after[..end]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();

    assert_eq!(
        already_registered
            .matches("self.shared_notifications")
            .count(),
        1,
        "the already-registered path must touch `shared_notifications` exactly \
         once; a second acquisition reopens the TOCTOU #5040 closed"
    );
    assert!(
        already_registered.contains("self.shared_notifications.get_mut("),
        "the single acquisition must be a WRITE guard (`get_mut`), so the \
         binary search and the channel write happen under it"
    );
    // The guard alone is not the invariant — the WRITE has to still be there.
    // Without this, deleting the refresh entirely satisfies both assertions
    // above while leaving a reconnected client wired to its dead sender
    // forever, receiving nothing. Unreachable behaviorally: nothing in the
    // tree constructs a `RuntimePool`, so no test can observe it.
    assert!(
        already_registered.contains("channels[idx]=(cli_id,notification_ch.clone())"),
        "the reconnect path must actually REFRESH the stored channel; without \
         the write a reconnected client keeps its dead sender and silently \
         receives nothing"
    );
}

/// Source-scrape pin for the subscriber-limit-error real-`ContractKey` fix in
/// `RuntimePool::register_contract_notifier`.
///
/// Why a source pin and not a behavioral test: as documented on
/// `register_contract_notifier_takes_one_guard_for_search_and_write` above,
/// nothing in this suite constructs a real `RuntimePool`, so both
/// subscriber-limit rejection call sites here are unreachable behaviorally
/// from this file. The BEHAVIORAL coverage for the same fix lives in
/// `test_subscriber_limit_error_carries_real_contract_key` and
/// `test_per_client_limit_error_carries_real_contract_key` above, which
/// exercise the twin `bridged_register_contract_notifier` path in
/// `executor_impl.rs` (reached via a plain `Executor`, not `RuntimePool`).
///
/// The invariant: BOTH `subscriber_limit_error(...)` call sites in this file
/// resolve the real key via `self.lookup_key(&instance_id)` first, rather
/// than passing the bare `instance_id` straight through — which is exactly
/// what the pre-fix code did, fabricating a zeroed-`CodeHash` key the client
/// could never match against the contract it subscribed to.
///
/// Bounded to the two enforcement blocks by anchors unique in pool.rs (the
/// "New subscriber" comment above the per-contract check, through the
/// "Insert in sorted order" comment that starts the success path), so this
/// cannot vacuously match content elsewhere in the file.
///
/// Whitespace-insensitive so rustfmt cannot break it.
#[test]
fn pool_subscriber_limit_error_resolves_real_key() {
    let src = include_str!("../runtime/pool.rs");
    let start = src
        .find("// New subscriber: enforce per-contract limit")
        .expect("`New subscriber` comment not found in pool.rs");
    let after = &src[start..];
    let end = after
        .find("// Insert in sorted order for efficient lookup")
        .expect("`Insert in sorted order` comment not found after the limit checks");
    let region: String = after[..end]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();

    assert_eq!(
        region.matches("self.lookup_key(&instance_id)").count(),
        2,
        "both subscriber-limit-error call sites must resolve the real key via \
         `self.lookup_key(&instance_id)` before calling `subscriber_limit_error` \
         — one for the per-contract limit, one for the per-client limit"
    );
    assert_eq!(
        region
            .matches("subscriber_limit_error(instance_id,")
            .count(),
        0,
        "subscriber_limit_error must never be called with the bare \
         `instance_id` directly — that fabricates a zeroed-CodeHash key the \
         client can never match against the refused contract (regression)"
    );
}
