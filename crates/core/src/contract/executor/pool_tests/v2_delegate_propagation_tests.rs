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

/// Drain the notification channel and return every queued V2 broadcast, in
/// order. The event names the contract and carries NO state — the handler
/// re-reads what is stored when it drains — so there are no bytes to return.
fn take_broadcasts(rx: &mut EventLoopNotificationsReceiver) -> Vec<ContractKey> {
    let mut out = Vec::new();
    while let Ok(event) = rx.notifications_receiver.try_recv() {
        if let Either::Right(NodeEvent::V2DelegateStateChanged { key }) = event {
            out.push(key);
        }
    }
    out
}

/// #5479: the installed V2 write callback must emit `V2DelegateStateChanged`
/// carrying the state that was written.
///
/// Before the fix the callback could not have done this even in principle —
/// it received only `(key, state_size)`, so the state to broadcast was not
/// available to it. The signature change is the fix; this asserts the emission.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_write_callback_queues_a_broadcast() {
    let (op_manager, mut notifications, _guards) = build_op_manager("v2-prop-emit").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    let key = test_key(11);
    let written = WrappedState::new(vec![9, 8, 7, 6]);

    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));
    callback(&key, &written, true);

    let queued = take_broadcasts(&mut notifications);
    assert_eq!(
        queued,
        vec![key],
        "a V2 delegate write must queue exactly one NodeEvent::V2DelegateStateChanged \
         naming the written contract — without it the write is invisible to the network \
         even though the host function returned success (this is #5479)"
    );
}

/// Successive writes to ONE contract must coalesce into a single queued
/// broadcast while that broadcast is undrained.
///
/// This is the property that makes the queue's message-count bound also a
/// bound on what it retains: the event carries a contract id rather than a
/// `WrappedState`, and repeats for the same contract add nothing at all. The
/// single drain re-reads stored state, so it fans out the NEWEST value — the
/// coalescing loses no information, it only avoids re-announcing the same
/// contract N times.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_writes_to_one_contract_coalesce_while_undrained() {
    let (op_manager, mut notifications, _guards) = build_op_manager("v2-prop-coalesce").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    let key = test_key(15);
    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));

    // Ten content-changing writes, nothing draining the channel in between.
    for i in 0..10u8 {
        callback(&key, &WrappedState::new(vec![i, i, i, i]), true);
    }

    assert_eq!(
        take_broadcasts(&mut notifications),
        vec![key],
        "ten writes to one contract with nothing draining must queue ONE broadcast, not \
         ten: the marker set by the first write suppresses the rest until a drain clears \
         it. If this reads ten, the coalescing is gone and the queue again grows one \
         entry per write"
    );
}

/// Coalescing must be PER CONTRACT — a queued broadcast for one contract must
/// not suppress another contract's.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_broadcast_coalescing_is_per_contract() {
    let (op_manager, mut notifications, _guards) = build_op_manager("v2-prop-per-key").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));
    let (a, b) = (test_key(16), test_key(17));
    callback(&a, &WrappedState::new(vec![1]), true);
    callback(&b, &WrappedState::new(vec![2]), true);
    callback(&a, &WrappedState::new(vec![3]), true);

    let mut queued = take_broadcasts(&mut notifications);
    queued.sort_by_key(|k| k.id().as_bytes().to_vec());
    let mut expected = vec![a, b];
    expected.sort_by_key(|k| k.id().as_bytes().to_vec());
    assert_eq!(
        queued, expected,
        "each contract must get its own queued broadcast; the repeat write to `a` \
         coalesces into a's pending entry and must not be suppressed by, or suppress, b's"
    );
}

/// Once a queued broadcast is DRAINED, the next write must queue a fresh one.
///
/// The marker is cleared by the handler when it begins draining, so this pins
/// that the suppression is a coalescing window and not a latch — a latch would
/// silently stop a contract propagating for the rest of the process lifetime.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_broadcast_requeues_after_drain() {
    let (op_manager, mut notifications, _guards) = build_op_manager("v2-prop-requeue").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    let key = test_key(18);
    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));

    callback(&key, &WrappedState::new(vec![1]), true);
    assert_eq!(take_broadcasts(&mut notifications), vec![key]);

    // Second write while the marker is still set: coalesced away.
    callback(&key, &WrappedState::new(vec![2]), true);
    assert!(
        take_broadcasts(&mut notifications).is_empty(),
        "still-pending contract must coalesce"
    );

    // The handler clears the marker as it starts draining.
    op_manager.clear_v2_delegate_broadcast_pending(&key);

    callback(&key, &WrappedState::new(vec![3]), true);
    assert_eq!(
        take_broadcasts(&mut notifications),
        vec![key],
        "after the pending broadcast is drained, the next write must queue a new one. If \
         this is empty the marker has become a latch and the contract has stopped \
         propagating permanently"
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
    callback(&key, &written, true);

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
    callback(&key, &written, true);

    assert!(
        take_broadcasts(&mut notifications).is_empty(),
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
    callback(&key, &written, true);

    assert_eq!(
        state_store.cached_state_hash(&key),
        None,
        "cache invalidation is unconditional — it is a correctness leg, not a telemetry leg, \
         and a local-only executor still serves reads from these caches"
    );
}

/// A FAILED enqueue must release the coalescing marker.
///
/// This is the error path `queue_v2_delegate_broadcast` calls out as critical,
/// and it fails in the worst direction available: leaving the marker set turns
/// a one-off dropped broadcast into a permanent wedge. Every later write to
/// that contract would coalesce into a queued broadcast that no handler will
/// ever drain — because none was ever queued — so the contract silently stops
/// propagating for the lifetime of the process. That is the same
/// write-returns-success-but-the-network-never-learns shape as #5479, one
/// layer up, and it would be very hard to diagnose from outside the node.
///
/// Dropping the receiver closes the notification channel, so `try_send`
/// returns `Closed` and the enqueue fails deterministically — no need to push
/// 2048 messages to fill it.
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_failed_enqueue_releases_the_coalescing_marker() {
    let (op_manager, notifications, _guards) = build_op_manager("v2-prop-enqueue-fail").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    // Close the channel: every subsequent enqueue attempt now fails.
    drop(notifications);

    let key = test_key(19);
    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));
    callback(&key, &WrappedState::new(vec![1, 2, 3]), true);

    assert!(
        !op_manager.v2_delegate_broadcast_pending.contains(key.id()),
        "a failed enqueue must release the coalescing marker. Left set, it latches the \
         contract: every later write coalesces into a broadcast that was never queued and \
         will never drain, so the contract stops propagating permanently"
    );

    // And the latch must genuinely be absent, not merely unobserved: a second
    // write has to be free to try again rather than be swallowed as a repeat.
    callback(&key, &WrappedState::new(vec![4, 5, 6]), true);
    assert!(
        !op_manager.v2_delegate_broadcast_pending.contains(key.id()),
        "a later write must still attempt its own enqueue after an earlier failure"
    );
}

/// The `content_changed = false` half of the callback, which nothing else covers.
///
/// Every other test in this file passes `true`, so the early return that gates
/// ONLY the fan-out is invisible to all of them: moving it above
/// `commit_state_write` — one line — leaves the entire suite green while
/// re-opening the eviction race that split was made to close. This is the same
/// function that carried a stranded mutation earlier in this workstream, so it
/// gets a behavioural assertion rather than trust.
///
/// Asserts both halves of the split at once: the generation still advances (the
/// bookkeeping is owed for a write that committed), and no broadcast is queued
/// (the fan-out is the one leg an identical rewrite does not need).
#[tokio::test(flavor = "current_thread")]
async fn v2_delegate_unchanged_write_meters_but_does_not_queue() {
    let (op_manager, mut notifications, _guards) = build_op_manager("v2-prop-unchanged").await;
    let state_store = StateStore::new(MockStateStorage::new(), 10_000_000).unwrap();

    let key = test_key(21);
    let written = WrappedState::new(vec![5, 5, 5]);
    let callback =
        v2_delegate_state_write_callback(state_store.cache_invalidator(), Some(op_manager.clone()));

    let generation_before = op_manager.ring.state_generation(&key);
    callback(&key, &written, false);

    assert_ne!(
        op_manager.ring.state_generation(&key),
        generation_before,
        "an unchanged write still COMMITTED to storage, so `commit_state_write` is owed: its \
         generation bump is what tells a scheduled EvictContract the contract was written \
         after the eviction was queued. If this is unchanged, the early return has moved \
         above the bookkeeping and an in-flight eviction can reclaim a just-written contract"
    );
    assert!(
        take_broadcasts(&mut notifications).is_empty(),
        "an unchanged write must queue NO fan-out — that is the one leg a byte-identical \
         rewrite does not need, and the whole reason the flag is threaded through"
    );
}

/// #4549: the drain read must be BOUNDED, and the bound must be its own, not
/// the contract handler's `CH_EV_RESPONSE_TIME_OUT` (300 s).
///
/// This is the test whose absence let a 5x widening of that bound ship. The
/// mutation lens proved the point rather than argued it: replacing the passed
/// `timeout` with `Duration::from_secs(300)` inside
/// `read_state_for_broadcast_drain` SURVIVED the entire 5,440-test suite, as
/// did replacing the constant with 1 ms. The suite was green for 1 ms, 2 s,
/// 10 s and 300 s alike, so the rustdoc's central claim — "it is bounded" —
/// was prose that nothing executed.
///
/// Why it matters more than a slow test: this read is awaited INLINE on the
/// network event loop, so overrunning it stops the node processing UDP and
/// connection events entirely. And no slow-iteration warning covers it —
/// `SLOW_EVENT_THRESHOLD` is measured around `process_select_result`, while
/// this arm runs after that elapsed time is taken. An unbounded await here is
/// the #4549 wedge that took a gateway network-dead.
///
/// The harness never services the contract-handler channel, which is exactly
/// the condition the bound exists for: a handler that cannot answer.
///
/// The ceiling asserted here is ABSOLUTE, not derived from the constant. A
/// test that scaled with the value it guards would pass for any value — the
/// self-referential shape that let `MAX_V2_DRAIN_RETRIES` 3 -> 100 survive
/// elsewhere in this diff.
#[tokio::test(flavor = "multi_thread")]
async fn the_drain_read_is_bounded_and_does_not_inherit_the_handler_timeout() {
    use crate::node::DrainStateRead;

    let (op_manager, _rx, _guards) = build_op_manager("v2-drain-bound").await;
    let key = test_key(41);

    let started = std::time::Instant::now();
    let outcome = op_manager
        .read_state_for_broadcast_drain(&key, std::time::Duration::from_secs(2))
        .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(outcome, DrainStateRead::Unavailable),
        "a handler that never answers must classify as Unavailable — the arm \
         that retries — and NOT as NotHeld, which is a correct and final no-op. \
         Conflating them silently discards a committed write. Got {outcome:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(30),
        "the drain read must honour its own bound rather than inheriting the \
         handler's 300 s CH_EV_RESPONSE_TIME_OUT. It ran for {elapsed:?}, which \
         is the #4549 wedge: that time is spent inline on the network event \
         loop, processing no packets and emitting no slow-iteration warning."
    );
}

/// The drain bound must leave room for a handler that is merely BUSY.
///
/// The paired lower bound to the test above, and the two are only meaningful
/// together. An upper-bound assertion alone is satisfied by a 1 ms timeout —
/// which the mutation lens confirmed also survives the suite — and a 1 ms bound
/// would classify every real read as `Unavailable`, so every V2 write would
/// take the retry path and then be reported dropped. That is the opposite
/// failure and just as silent.
///
/// Asserted against the CONSTANT the production arm actually passes, because
/// the property under test is a property of that value.
#[test]
fn the_drain_bound_leaves_room_for_a_busy_handler() {
    use crate::node::V2_BROADCAST_DRAIN_READ_TIMEOUT as BOUND;

    assert!(
        BOUND >= std::time::Duration::from_millis(500),
        "a bound this tight classifies a merely-busy handler as Unavailable, so \
         every V2 write takes the retry path and is then reported as a dropped \
         broadcast. Got {BOUND:?}"
    );
    assert!(
        BOUND <= std::time::Duration::from_secs(5),
        "this read is awaited INLINE on the network event loop and no \
         slow-iteration warning covers it, so the bound is a cap on how long \
         the node stops processing packets. The marker coalesces PER CONTRACT, \
         so a delegate writing N contracts costs N times this. Got {BOUND:?}"
    );
}
