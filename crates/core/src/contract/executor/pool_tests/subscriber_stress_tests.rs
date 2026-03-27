//! Stress tests for subscriber storage under concurrent access.
//!
//! Covers:
//! - Concurrent DashMap registration and removal (validates RwLock→DashMap migration)
//! - Fan-out throughput with many subscribers
//! - Rapid client churn (register/remove cycles)

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use freenet_stdlib::prelude::*;
use tokio::sync::mpsc;

use crate::client_events::{ClientId, HostResult};
use crate::contract::executor::mock_wasm_runtime::MockWasmRuntime;
use crate::contract::executor::{
    ContractExecutor, Executor, MAX_SUBSCRIBERS_PER_CONTRACT, MAX_SUBSCRIPTIONS_PER_CLIENT,
    SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE,
};
use crate::wasm_runtime::MockStateStorage;

/// Helper to create a MockWasmRuntime executor.
async fn create_executor() -> Executor<MockWasmRuntime, MockStateStorage> {
    let storage = MockStateStorage::new();
    Executor::new_mock_wasm("subscriber_stress_test", storage, None, None)
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

// ==========================================================================
// Concurrent DashMap stress (directly tests the shared pool-level types)
// ==========================================================================

type SharedNotifications =
    Arc<DashMap<ContractInstanceId, Vec<(ClientId, mpsc::Sender<HostResult>)>>>;
type SharedClientCounts = Arc<DashMap<ClientId, usize>>;
type SharedSummaries =
    Arc<DashMap<ContractInstanceId, HashMap<ClientId, Option<StateSummary<'static>>>>>;

/// Stress test: concurrent registrations to different contracts from multiple threads.
///
/// Spawns N threads, each registering a subscriber for a unique contract.
/// Validates no data loss or deadlock under concurrent DashMap writes to different shards.
#[test]
fn test_concurrent_registration_different_contracts() {
    let notifications: SharedNotifications = Arc::new(DashMap::new());
    let client_counts: SharedClientCounts = Arc::new(DashMap::new());
    let summaries: SharedSummaries = Arc::new(DashMap::new());

    let num_threads = 16;
    let registrations_per_thread = 50;
    let total_expected = num_threads * registrations_per_thread;
    let registered = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for thread_id in 0..num_threads {
            let notifications = notifications.clone();
            let client_counts = client_counts.clone();
            let summaries = summaries.clone();
            let registered = registered.clone();

            s.spawn(move || {
                for i in 0..registrations_per_thread {
                    let client_id = ClientId::next();
                    // Each registration targets a unique contract
                    let seed = format!("stress_contract_{thread_id}_{i}");
                    let contract = test_contract(seed.as_bytes());
                    let instance_id = *contract.key().id();

                    let (tx, _rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);

                    // Insert into notifications (sorted insert)
                    let mut channels = notifications.entry(instance_id).or_default();
                    let insert_pos = channels.partition_point(|(id, _)| id < &client_id);
                    channels.insert(insert_pos, (client_id, tx));
                    drop(channels);

                    // Increment client count
                    *client_counts.entry(client_id).or_insert(0) += 1;

                    // Insert summary
                    summaries
                        .entry(instance_id)
                        .or_default()
                        .insert(client_id, None);

                    registered.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });

    assert_eq!(registered.load(Ordering::SeqCst), total_expected);
    assert_eq!(notifications.len(), total_expected);

    // Every contract should have exactly 1 subscriber
    for entry in notifications.iter() {
        assert_eq!(
            entry.value().len(),
            1,
            "Each contract should have 1 subscriber"
        );
    }

    // Client counts should sum to total_expected
    let total_client_subs: usize = client_counts.iter().map(|e| *e.value()).sum();
    assert_eq!(total_client_subs, total_expected);
}

/// Stress test: concurrent registrations to the SAME contract from multiple threads.
///
/// Spawns N threads, each registering a unique client for the same contract.
/// Validates sorted-insert consistency and no subscriber loss under shard contention.
#[test]
fn test_concurrent_registration_same_contract() {
    let notifications: SharedNotifications = Arc::new(DashMap::new());
    let client_counts: SharedClientCounts = Arc::new(DashMap::new());

    let num_threads = 8;
    let registrations_per_thread = 30;
    let total_expected = num_threads * registrations_per_thread;

    let contract = test_contract(b"shared_stress_contract");
    let instance_id = *contract.key().id();

    std::thread::scope(|s| {
        for _thread_id in 0..num_threads {
            let notifications = notifications.clone();
            let client_counts = client_counts.clone();

            s.spawn(move || {
                for _ in 0..registrations_per_thread {
                    let client_id = ClientId::next();
                    let (tx, _rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);

                    // Sorted insert under contention
                    let mut channels = notifications.entry(instance_id).or_default();
                    let insert_pos = channels.partition_point(|(id, _)| id < &client_id);
                    channels.insert(insert_pos, (client_id, tx));
                    drop(channels);

                    *client_counts.entry(client_id).or_insert(0) += 1;
                }
            });
        }
    });

    // All subscribers should be present for the single contract
    let channels = notifications.get(&instance_id).unwrap();
    assert_eq!(
        channels.len(),
        total_expected,
        "All {total_expected} subscribers should be registered"
    );

    // Verify sorted order is maintained despite concurrent inserts
    let ids: Vec<ClientId> = channels.iter().map(|(id, _)| *id).collect();
    let mut sorted_ids = ids.clone();
    sorted_ids.sort();
    assert_eq!(ids, sorted_ids, "Subscriber IDs should be in sorted order");
}

/// Stress test: concurrent registration and removal (simulates client churn).
///
/// Half the threads register new clients, the other half remove existing clients.
/// Validates that DashMap retain + entry don't deadlock under cross-operation contention.
#[test]
fn test_concurrent_register_and_remove() {
    let notifications: SharedNotifications = Arc::new(DashMap::new());
    let client_counts: SharedClientCounts = Arc::new(DashMap::new());

    let num_contracts = 20;
    let clients_per_contract = 10;

    // Pre-populate: register clients for each contract
    let mut all_clients: Vec<(ContractInstanceId, ClientId)> = Vec::new();
    for c in 0..num_contracts {
        let contract = test_contract(format!("churn_contract_{c}").as_bytes());
        let instance_id = *contract.key().id();

        for _ in 0..clients_per_contract {
            let client_id = ClientId::next();
            let (tx, _rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);

            let mut channels = notifications.entry(instance_id).or_default();
            let insert_pos = channels.partition_point(|(id, _)| id < &client_id);
            channels.insert(insert_pos, (client_id, tx));
            drop(channels);

            *client_counts.entry(client_id).or_insert(0) += 1;
            all_clients.push((instance_id, client_id));
        }
    }

    let initial_total: usize = notifications.iter().map(|e| e.value().len()).sum::<usize>();
    assert_eq!(initial_total, num_contracts * clients_per_contract);

    // Concurrently: some threads add new clients, some threads remove existing ones
    let clients_to_remove: Vec<_> = all_clients.iter().step_by(3).cloned().collect();
    let remove_count = clients_to_remove.len();

    let add_count = Arc::new(AtomicUsize::new(0));
    let removed_count = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        // Adder threads: add new clients to existing contracts
        for t in 0..4 {
            let notifications = notifications.clone();
            let client_counts = client_counts.clone();
            let add_count = add_count.clone();

            s.spawn(move || {
                for i in 0..25 {
                    let contract =
                        test_contract(format!("churn_contract_{}", i % num_contracts).as_bytes());
                    let instance_id = *contract.key().id();
                    let client_id = ClientId::next();
                    let (tx, _rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);

                    let mut channels = notifications.entry(instance_id).or_default();
                    let insert_pos = channels.partition_point(|(id, _)| id < &client_id);
                    channels.insert(insert_pos, (client_id, tx));
                    drop(channels);

                    *client_counts.entry(client_id).or_insert(0) += 1;
                    add_count.fetch_add(1, Ordering::SeqCst);

                    // Small yield to increase interleaving
                    if (t + i) % 5 == 0 {
                        std::thread::yield_now();
                    }
                }
            });
        }

        // Remover threads: remove pre-selected clients using DashMap::retain
        for chunk in clients_to_remove.chunks(remove_count.max(1) / 2 + 1) {
            let notifications = notifications.clone();
            let client_counts = client_counts.clone();
            let removed_count = removed_count.clone();

            let chunk = chunk.to_vec();
            s.spawn(move || {
                for (instance_id, client_id) in &chunk {
                    if let Some(mut channels) = notifications.get_mut(instance_id) {
                        if let Ok(i) = channels.binary_search_by_key(&client_id, |(id, _)| id) {
                            channels.remove(i);
                            removed_count.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    client_counts.remove(client_id);
                }
            });
        }
    });

    let added = add_count.load(Ordering::SeqCst);
    let removed = removed_count.load(Ordering::SeqCst);

    let final_total: usize = notifications.iter().map(|e| e.value().len()).sum::<usize>();
    assert_eq!(
        final_total,
        initial_total + added - removed,
        "Final count should be initial ({initial_total}) + added ({added}) - removed ({removed})"
    );
}

/// Stress test: concurrent readers and writers (simulates fan-out reads during registration).
///
/// Some threads iterate over all entries (like get_subscription_info / snapshot for fan-out),
/// while other threads insert new entries. Validates no deadlock between DashMap::iter() and
/// DashMap::entry().
#[test]
fn test_concurrent_read_during_write() {
    let notifications: SharedNotifications = Arc::new(DashMap::new());
    let total_reads = Arc::new(AtomicUsize::new(0));

    // Pre-populate some contracts
    for c in 0..10 {
        let contract = test_contract(format!("read_write_contract_{c}").as_bytes());
        let instance_id = *contract.key().id();
        let client_id = ClientId::next();
        let (tx, _rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        notifications
            .entry(instance_id)
            .or_default()
            .push((client_id, tx));
    }

    std::thread::scope(|s| {
        // Reader threads: iterate all entries (simulates get_subscription_info)
        for _ in 0..4 {
            let notifications = notifications.clone();
            let total_reads = total_reads.clone();

            s.spawn(move || {
                for _ in 0..100 {
                    let count: usize = notifications.iter().map(|e| e.value().len()).sum::<usize>();
                    total_reads.fetch_add(count, Ordering::Relaxed);
                    std::thread::yield_now();
                }
            });
        }

        // Writer threads: add new entries
        for t in 0..4 {
            let notifications = notifications.clone();

            s.spawn(move || {
                for i in 0..50 {
                    let contract = test_contract(format!("read_write_new_{t}_{i}").as_bytes());
                    let instance_id = *contract.key().id();
                    let client_id = ClientId::next();
                    let (tx, _rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);

                    notifications
                        .entry(instance_id)
                        .or_default()
                        .push((client_id, tx));
                    std::thread::yield_now();
                }
            });
        }
    });

    // After all threads complete, verify data integrity
    let final_count = notifications.len();
    // 10 initial + 4 threads × 50 = 210
    assert_eq!(final_count, 210, "Should have 10 initial + 200 new entries");
    assert!(
        total_reads.load(Ordering::Relaxed) > 0,
        "Reader threads should have read some entries"
    );
}

// ==========================================================================
// Fan-out throughput (standalone executor path)
// ==========================================================================

/// Stress test: fan-out notification to many subscribers.
///
/// Registers 200 subscribers for a single contract, then triggers a state update.
/// Verifies all subscribers receive the notification (no lost messages).
#[tokio::test(flavor = "current_thread")]
async fn test_fanout_throughput_200_subscribers() {
    let mut executor = create_executor().await;
    let key = store_contract(&mut executor, b"fanout_stress_contract").await;
    let instance_id = *key.id();

    let subscriber_count = 200;
    let mut receivers = Vec::with_capacity(subscriber_count);

    // Register 200 subscribers
    for _ in 0..subscriber_count {
        let client_id = ClientId::next();
        let (tx, rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(instance_id, client_id, tx, None)
            .expect("registration should succeed");
        receivers.push(rx);
    }

    // Trigger a state update which will fan out to all subscribers
    let updated_state = WrappedState::new(vec![42; 100]);
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(updated_state),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("upsert should succeed");

    // Verify all receivers got a notification
    let mut received = 0;
    for rx in &mut receivers {
        match rx.try_recv() {
            Ok(Ok(_)) => received += 1,
            Ok(Err(e)) => panic!("Received error notification: {e}"),
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => {
                panic!("Channel disconnected unexpectedly")
            }
        }
    }

    assert_eq!(
        received, subscriber_count,
        "All {subscriber_count} subscribers should receive the notification"
    );
}

/// Stress test: rapid register-update cycles.
///
/// Alternates between registering new subscribers and triggering updates.
/// Validates that subscribers registered between updates correctly receive
/// notifications, and no state corruption occurs from interleaved operations.
#[tokio::test(flavor = "current_thread")]
async fn test_rapid_register_update_interleave() {
    let mut executor = create_executor().await;
    let key = store_contract(&mut executor, b"interleave_stress").await;
    let instance_id = *key.id();

    let rounds = 20;
    let clients_per_round = 5;
    let mut all_receivers: Vec<mpsc::Receiver<HostResult>> = Vec::new();

    for round in 0..rounds {
        // Register new subscribers
        for _ in 0..clients_per_round {
            let client_id = ClientId::next();
            let (tx, rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
            executor
                .register_contract_notifier(instance_id, client_id, tx, None)
                .expect("registration should succeed");
            all_receivers.push(rx);
        }

        // Trigger an update — all registered subscribers so far should be notified
        let updated_state = WrappedState::new(vec![round as u8; 10]);
        executor
            .upsert_contract_state(
                key,
                either::Either::Left(updated_state),
                RelatedContracts::default(),
                None,
            )
            .await
            .expect("upsert should succeed");

        // Drain all pending notifications for existing receivers
        let expected_count = (round + 1) * clients_per_round;
        let mut received_this_round = 0;
        for rx in all_receivers.iter_mut() {
            while rx.try_recv().is_ok() {
                received_this_round += 1;
            }
        }

        assert_eq!(
            received_this_round, expected_count,
            "Round {round}: expected {expected_count} notifications, got {received_this_round}"
        );
    }
}

// ==========================================================================
// Client churn (standalone executor path)
// ==========================================================================

/// Stress test: rapid client churn with limit enforcement.
///
/// Registers clients up to the per-contract limit, removes some via channel close,
/// then registers new ones. Validates the limit is consistently enforced and
/// the subscriber count stays accurate through churn cycles.
#[tokio::test(flavor = "current_thread")]
async fn test_client_churn_with_limit_enforcement() {
    let mut executor = create_executor().await;
    let key = store_contract(&mut executor, b"churn_limit_stress").await;
    let instance_id = *key.id();

    // Fill to half capacity
    let half = MAX_SUBSCRIBERS_PER_CONTRACT / 2;
    let mut active_receivers = Vec::with_capacity(MAX_SUBSCRIBERS_PER_CONTRACT);

    for _ in 0..half {
        let client_id = ClientId::next();
        let (tx, rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(instance_id, client_id, tx, None)
            .expect("registration should succeed");
        active_receivers.push(rx);
    }

    // Fill to full capacity
    for _ in half..MAX_SUBSCRIBERS_PER_CONTRACT {
        let client_id = ClientId::next();
        let (tx, rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(instance_id, client_id, tx, None)
            .expect("registration should succeed");
        active_receivers.push(rx);
    }

    // Verify we're at the limit
    let extra_client = ClientId::next();
    let (tx, _rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    assert!(
        executor
            .register_contract_notifier(instance_id, extra_client, tx, None)
            .is_err(),
        "Should be at capacity"
    );

    // Drop half the receivers (simulates disconnection)
    // On the standalone executor path, dropping the receiver doesn't auto-cleanup.
    // The cleanup happens when notification fails during fan-out.
    let kept = active_receivers.split_off(half);
    drop(active_receivers); // Drop the first half (channels close)

    // Trigger an update — this will detect closed channels and clean them up
    let updated_state = WrappedState::new(vec![99]);
    executor
        .upsert_contract_state(
            key,
            either::Either::Left(updated_state),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("upsert should succeed");

    // Now we should be able to register new subscribers (since half were cleaned up)
    let mut new_receivers = Vec::new();
    for _ in 0..half {
        let client_id = ClientId::next();
        let (tx, rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(instance_id, client_id, tx, None)
            .expect("registration should succeed after cleanup");
        new_receivers.push(rx);
    }

    // Verify total is correct
    let subs = executor.get_subscription_info();
    let contract_sub_count = subs
        .iter()
        .filter(|info| info.instance_id == instance_id)
        .count();

    // Should have: half (kept) + half (new) = MAX_SUBSCRIBERS_PER_CONTRACT
    assert_eq!(
        contract_sub_count, MAX_SUBSCRIBERS_PER_CONTRACT,
        "After churn: should be back at full capacity"
    );

    // Keep references alive to prevent premature drop
    drop(kept);
    drop(new_receivers);
}

/// Stress test: per-client limit enforcement through churn.
///
/// Registers a client to MAX_SUBSCRIPTIONS_PER_CLIENT contracts, drops some
/// receiver channels, triggers updates to clean up, then verifies the client
/// can register for new contracts again.
#[tokio::test(flavor = "current_thread")]
async fn test_per_client_churn_with_limit() {
    let mut executor = create_executor().await;
    let client_id = ClientId::next();

    // Register the client up to the per-client limit
    let mut receivers = Vec::with_capacity(MAX_SUBSCRIPTIONS_PER_CLIENT);
    let mut keys = Vec::with_capacity(MAX_SUBSCRIPTIONS_PER_CLIENT);

    for i in 0..MAX_SUBSCRIPTIONS_PER_CLIENT {
        let seed = format!("per_client_churn_{i}");
        let key = store_contract(&mut executor, seed.as_bytes()).await;
        let (tx, rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(*key.id(), client_id, tx, None)
            .expect("registration should succeed");
        receivers.push(rx);
        keys.push(key);
    }

    // Verify we're at the per-client limit
    let extra_key = store_contract(&mut executor, b"per_client_churn_extra").await;
    let (tx, _rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
    assert!(
        executor
            .register_contract_notifier(*extra_key.id(), client_id, tx, None)
            .is_err(),
        "Should be at per-client limit"
    );

    // Drop half the receivers to simulate disconnection
    let drop_count = MAX_SUBSCRIPTIONS_PER_CLIENT / 2;
    for _ in 0..drop_count {
        receivers.pop(); // Drop the receiver (closes channel)
    }

    // Trigger updates on the contracts whose receivers were dropped
    // This cleans up the closed channels
    for key in keys.iter().skip(MAX_SUBSCRIPTIONS_PER_CLIENT - drop_count) {
        let updated_state = WrappedState::new(vec![77]);
        executor
            .upsert_contract_state(
                *key,
                either::Either::Left(updated_state),
                RelatedContracts::default(),
                None,
            )
            .await
            .expect("upsert should succeed");
    }

    // Now the client should be able to register for new contracts
    for i in 0..drop_count {
        let seed = format!("per_client_churn_new_{i}");
        let new_key = store_contract(&mut executor, seed.as_bytes()).await;
        let (tx, rx) = mpsc::channel(SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE);
        executor
            .register_contract_notifier(*new_key.id(), client_id, tx, None)
            .unwrap_or_else(|e| panic!("Re-registration {i} should succeed after cleanup: {e}"));
        receivers.push(rx);
    }
}
