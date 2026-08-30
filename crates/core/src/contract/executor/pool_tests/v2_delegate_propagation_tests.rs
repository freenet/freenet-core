//! Deterministic guards for V2 delegate write propagation (#5479).
//!
//! A V2 delegate's `put_contract_state` / `update_contract_state` writes
//! straight through the raw `Storage`, bypassing the executor's
//! `state_store.{store,update}` chokepoints. Everything those chokepoints do
//! has to be re-applied by the callback installed on `Runtime` — and until
//! #5479 network propagation was the leg nobody had re-applied, so a V2 write
//! returned success, read back correctly on the writing node, and was never
//! seen anywhere else.
//!
//! The end-to-end proof is
//! `crates/core/tests/operations.rs::test_v2_delegate_update_propagates_to_second_peer`,
//! which is the only place the bug is observable by construction (a second
//! peer). These tests are the cheap, deterministic complement: they drive the
//! PRODUCTION closure — `v2_delegate_state_write_callback`, the exact value
//! `install_v2_delegate_state_write_hooks` installs — against a real
//! `OpManager`/`Ring` and assert what it emits. That keeps the regression
//! covered in a fast unit run even when the 3-node E2E is not exercised.

use std::sync::Arc;

use either::Either;
use freenet_stdlib::prelude::*;

use crate::config::ConfigArgs;
use crate::contract::executor::OperationMode;
use crate::contract::executor::runtime::v2_delegate_state_write_callback;
use crate::message::NodeEvent;
use crate::node::{EventLoopNotificationsReceiver, OpManager};
use crate::wasm_runtime::{MockStateStorage, StateStore};

/// Build a real `OpManager` and hand back the notification RECEIVER typed, so a
/// test can read the events the callback emits. (The sibling harness in
/// `identical_input_probe_tests.rs` buries the receiver in an opaque guard box
/// because it only needs it kept alive.)
async fn build_op_manager(
    id: &str,
) -> (
    Arc<OpManager>,
    EventLoopNotificationsReceiver,
    Box<dyn std::any::Any>,
) {
    let config_args = ConfigArgs {
        id: Some(id.to_string()),
        mode: Some(OperationMode::Local),
        ..Default::default()
    };
    let node_config =
        crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
            .await
            .expect("build NodeConfig");

    let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
    let (ops_ch_channel, ch_channel, wait_for_event) = crate::contract::contract_handler_channel();
    let connection_manager = crate::ring::ConnectionManager::new(&node_config);
    let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
    let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();

    let op_manager = Arc::new(
        OpManager::new(
            notification_tx,
            ops_ch_channel,
            &node_config,
            crate::tracing::DynamicRegister::new(vec![]),
            connection_manager,
            result_router_tx,
            &task_monitor,
        )
        .expect("build OpManager"),
    );
    op_manager.ring.attach_op_manager(&op_manager);

    let guards: Box<dyn std::any::Any> =
        Box::new((ch_channel, wait_for_event, result_router_rx, task_monitor));
    (op_manager, notification_rx, guards)
}

fn test_key(seed: u8) -> ContractKey {
    let code = ContractCode::from(vec![seed, seed, seed]);
    let params = Parameters::from(vec![seed]);
    ContractKey::from_params_and_code(&params, &code)
}

/// Drain the notification channel and return the first `BroadcastStateChange`,
/// or `None` if the channel holds no such event.
fn take_broadcast(rx: &mut EventLoopNotificationsReceiver) -> Option<(ContractKey, WrappedState)> {
    while let Ok(event) = rx.notifications_receiver.try_recv() {
        if let Either::Right(NodeEvent::BroadcastStateChange { key, new_state, .. }) = event {
            return Some((key, new_state));
        }
    }
    None
}

/// #5479: the installed V2 write callback must emit `BroadcastStateChange`
/// carrying the state that was written.
///
/// Before the fix the callback could not have done this even in principle —
/// it received only `(key, state_size)`, so the state to broadcast was not
/// available to it. The signature change is the fix; this asserts the emission.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_write_callback_emits_broadcast_state_change() {
    let (op_manager, mut notifications, _guards) = build_op_manager("v2-prop-emit").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    let key = test_key(11);
    let written = WrappedState::new(vec![9, 8, 7, 6]);

    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));
    callback(&key, &written);

    let (broadcast_key, broadcast_state) = take_broadcast(&mut notifications).expect(
        "a V2 delegate write must emit NodeEvent::BroadcastStateChange — without it the \
         write is invisible to the network even though the host function returned success \
         (this is #5479)",
    );
    assert_eq!(
        broadcast_key, key,
        "broadcast must name the written contract"
    );
    assert_eq!(
        broadcast_state.as_ref(),
        written.as_ref(),
        "broadcast must carry the bytes that were actually written, not a re-read or a \
         reconstruction"
    );
}

/// The write must still be metered and the caches still invalidated — the legs
/// that already existed before #5479 must not have been traded for the new one.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_write_callback_still_meters_and_invalidates() {
    let (op_manager, _notifications, _guards) = build_op_manager("v2-prop-meter").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    let key = test_key(12);
    let written = WrappedState::new(vec![1, 2, 3, 4, 5]);

    // Warm the change-detector, as a prior summarize slow path would.
    state_store.cache_state_hash(key, crate::wasm_runtime::state_hash(&written));
    assert!(state_store.cached_state_hash(&key).is_some());

    let generation_before = op_manager.ring.state_generation(&key);

    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));
    callback(&key, &written);

    assert_eq!(
        state_store.cached_state_hash(&key),
        None,
        "the callback must drop StateStore's change-detector entry, or the summarize/delta \
         fast path serves a stale summary for a state it never saw written"
    );
    assert_ne!(
        op_manager.ring.state_generation(&key),
        generation_before,
        "the callback must bump the per-contract state-write generation (the EvictContract \
         re-host race)"
    );
}

/// A contract flagged as violating a CRDT invariant must NOT be broadcast, the
/// same suppression `Executor::broadcast_state_change` applies. Otherwise the
/// V2 path becomes a hole in the storm suppression that #4251/#4279 installed.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_write_callback_suppresses_broken_contract_broadcast() {
    let (op_manager, mut notifications, _guards) = build_op_manager("v2-prop-broken").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    let key = test_key(13);
    let written = WrappedState::new(vec![4, 4, 4]);

    op_manager
        .ring
        .record_broken_invariant(key, crate::ring::BrokenInvariant::NonIdempotent);
    assert!(op_manager.ring.is_contract_broken(&key));

    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));
    callback(&key, &written);

    assert!(
        take_broadcast(&mut notifications).is_none(),
        "a contract flagged as non-idempotent must not be broadcast from the V2 delegate \
         write path either — otherwise V2 is a hole in the suppression that stops the \
         #4279 storm"
    );
}

/// An executor with no `OpManager` (unit-test and local-only harnesses) has no
/// ring to meter or broadcast against. The callback must still invalidate the
/// caches and must not panic.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_write_callback_without_op_manager_only_invalidates() {
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();
    let key = test_key(14);
    let written = WrappedState::new(vec![7, 7]);

    state_store.cache_state_hash(key, crate::wasm_runtime::state_hash(&written));

    let callback = v2_delegate_state_write_callback(state_store.cache_invalidator(), None);
    callback(&key, &written);

    assert_eq!(
        state_store.cached_state_hash(&key),
        None,
        "cache invalidation is unconditional — it is a correctness leg, not a telemetry leg, \
         and a local-only executor still serves reads from these caches"
    );
}
