//! Tests for the cheap state-change-detector that backs the read-only
//! `summarize_contract_state` / `get_contract_state_delta` caches.
//!
//! The optimization replaces the per-call full-state load + `DefaultHasher`
//! pass (used only to build the summary/delta cache key) with a cheap
//! per-contract change-detector kept in `StateStore`. The detector is
//! invalidated by every state write and repopulated from a freshly-loaded
//! state, so an unchanged contract summarizes/deltas without reloading or
//! rehashing the state, or re-running WASM — while a changed contract is always
//! recomputed.
//!
//! The stake is divergence: a stale summary OR delta served against changed
//! state would hand a peer the wrong bytes. The correctness tests below assert
//! freshness after an update and would FAIL if the detector missed a write.

use std::sync::Arc;

use either::Either;
use freenet_stdlib::prelude::*;

use crate::config::ConfigArgs;
use crate::contract::executor::mock_wasm_runtime::MockWasmRuntime;
use crate::contract::executor::{ContractExecutor, Executor, OperationMode};
use crate::node::OpManager;
use crate::ring::AccessType;
use crate::wasm_runtime::MockStateStorage;

use super::super::mock_runtime::test::create_test_contract as test_contract;

async fn cached_executor(storage: MockStateStorage) -> Executor<MockWasmRuntime, MockStateStorage> {
    Executor::new_mock_wasm("summarize_delta_cache", storage, None, None)
        .await
        .expect("create cached MockWasmRuntime executor")
}

// =========================================================================
// CORRECTNESS (the guard against divergence)
// =========================================================================

/// PUT a contract, summarize, UPDATE it, summarize again → the second summarize
/// MUST reflect the updated state, never the stale cached summary. This fails if
/// the detector misses the write (e.g. if a write path stopped invalidating it).
#[tokio::test(flavor = "current_thread")]
async fn summarize_is_fresh_after_update_not_stale_cache() {
    let storage = MockStateStorage::new();
    let mut exec = cached_executor(storage).await;

    let contract = test_contract(b"summarize_fresh_after_update");
    let key = contract.key();
    let state_a = WrappedState::new(vec![1, 2, 3]);
    let state_b = WrappedState::new(vec![9, 8, 7, 6]);

    // Initial PUT of state A.
    exec.upsert_contract_state(
        key,
        Either::Left(state_a.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT A");

    let summary_a = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize A");
    assert_eq!(
        summary_a.as_ref(),
        blake3::hash(state_a.as_ref()).as_bytes(),
        "summary of A must be blake3(A)"
    );

    // Repeat on unchanged state: still A (exercises the fast path).
    let summary_a2 = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize A again");
    assert_eq!(summary_a2.as_ref(), summary_a.as_ref());

    // UPDATE to state B.
    exec.upsert_contract_state(
        key,
        Either::Left(state_b.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE B");

    // Summarize again: MUST be fresh (B), not the stale cached A.
    let summary_b = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize B");
    assert_eq!(
        summary_b.as_ref(),
        blake3::hash(state_b.as_ref()).as_bytes(),
        "summary must reflect updated state B, not stale cached A"
    );
    assert_ne!(
        summary_b.as_ref(),
        summary_a.as_ref(),
        "summary must change after the state changed"
    );
}

/// The delta sibling of the test above: a delta computed against a fixed peer
/// summary MUST be recomputed after the state changes, never served stale.
/// `MockWasmRuntime::get_state_delta` returns the full state as the delta, so a
/// fresh delta equals the new state bytes and a stale one would equal the old.
#[tokio::test(flavor = "current_thread")]
async fn delta_is_fresh_after_update_not_stale_cache() {
    let storage = MockStateStorage::new();
    let mut exec = cached_executor(storage).await;

    let contract = test_contract(b"delta_fresh_after_update");
    let key = contract.key();
    let state_a = WrappedState::new(vec![1, 2, 3]);
    let state_b = WrappedState::new(vec![9, 8, 7, 6]);
    // A fixed peer summary: the same value is used before and after the update so
    // ONLY the state component of the delta cache key changes.
    let peer_summary = StateSummary::from(vec![42u8; 4]);

    exec.upsert_contract_state(
        key,
        Either::Left(state_a.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT A");

    let delta_a = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta A");
    assert_eq!(
        delta_a.as_ref(),
        state_a.as_ref(),
        "mock delta is the full state A"
    );

    // Repeat on unchanged state + same peer summary: cached, identical delta.
    let delta_a2 = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta A again");
    assert_eq!(delta_a2.as_ref(), delta_a.as_ref());

    // UPDATE to state B.
    exec.upsert_contract_state(
        key,
        Either::Left(state_b.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE B");

    // Delta against the SAME peer summary: MUST be fresh (B), not stale (A).
    let delta_b = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta B");
    assert_eq!(
        delta_b.as_ref(),
        state_b.as_ref(),
        "delta must reflect updated state B, not stale cached A"
    );
    assert_ne!(
        delta_b.as_ref(),
        delta_a.as_ref(),
        "delta must change after the state changed"
    );
}

// =========================================================================
// OPTIMIZATION (the fast path skips the state load + rehash + WASM)
// =========================================================================

/// On an UNCHANGED contract, repeated summarize must NOT reload the state from
/// storage (and therefore must not re-hash it or re-run WASM). Uses an uncached
/// `StateStore` so every load is observable via `MockStateStorage::get_count`.
#[tokio::test(flavor = "current_thread")]
async fn summarize_unchanged_skips_state_load() {
    let storage = MockStateStorage::new();
    let mut exec = Executor::new_mock_wasm_uncached("summarize_opt", storage.clone())
        .await
        .expect("create uncached executor");

    let contract = test_contract(b"summarize_opt");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3, 4, 5]);

    exec.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT");

    // First summarize: cold detector → slow path, loads the state.
    let _ = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize 1");
    let gets_after_first = storage.get_count();
    assert!(
        gets_after_first >= 1,
        "the cold first summarize must load the state at least once"
    );

    // Second + third summarize on unchanged state: fast path, NO extra loads.
    let _ = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize 2");
    let _ = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize 3");
    assert_eq!(
        storage.get_count(),
        gets_after_first,
        "repeated unchanged summarize must not reload the state (fast path)"
    );

    // After an UPDATE the detector is invalidated → the next summarize reloads.
    let new_state = WrappedState::new(vec![6, 7, 8, 9, 10, 11]);
    exec.upsert_contract_state(
        key,
        Either::Left(new_state.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE");
    let gets_before_post_update = storage.get_count();
    let _ = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize after update");
    assert!(
        storage.get_count() > gets_before_post_update,
        "summarize after an update must reload the changed state"
    );
}

/// Delta sibling: repeated delta on an UNCHANGED contract against the same peer
/// summary must NOT reload the state.
#[tokio::test(flavor = "current_thread")]
async fn delta_unchanged_skips_state_load() {
    let storage = MockStateStorage::new();
    let mut exec = Executor::new_mock_wasm_uncached("delta_opt", storage.clone())
        .await
        .expect("create uncached executor");

    let contract = test_contract(b"delta_opt");
    let key = contract.key();
    let state = WrappedState::new(vec![5, 4, 3, 2, 1]);
    let peer_summary = StateSummary::from(vec![7u8; 3]);

    exec.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT");

    let _ = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta 1");
    let gets_after_first = storage.get_count();
    assert!(
        gets_after_first >= 1,
        "cold first delta must load the state"
    );

    let _ = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta 2");
    let _ = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta 3");
    assert_eq!(
        storage.get_count(),
        gets_after_first,
        "repeated unchanged delta must not reload the state (fast path)"
    );

    // After an UPDATE the detector is invalidated → the next delta against the
    // SAME peer summary must reload the changed state (symmetry with
    // `summarize_unchanged_skips_state_load`).
    let new_state = WrappedState::new(vec![9, 8, 7, 6, 5, 4]);
    exec.upsert_contract_state(
        key,
        Either::Left(new_state.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE");
    let gets_before_post_update = storage.get_count();
    let _ = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta after update");
    assert!(
        storage.get_count() > gets_before_post_update,
        "delta after an update must reload the changed state"
    );
}

// =========================================================================
// EDGE CASES
// =========================================================================

/// State-absent: summarizing a contract with no stored state must error (the
/// #4610 state-presence behavior), never fast-path a phantom summary.
#[tokio::test(flavor = "current_thread")]
async fn summarize_absent_contract_errors() {
    let storage = MockStateStorage::new();
    let mut exec = cached_executor(storage).await;
    let contract = test_contract(b"summarize_absent");
    let key = contract.key();

    let result = exec.summarize_contract_state(key).await;
    assert!(
        result.is_err(),
        "summarize of an absent contract must error, not return a phantom summary"
    );
}

/// Cold caches (simulating a restart or a detector/LRU eviction): a second
/// executor that shares the backing storage but has cold detector + summary
/// caches must recompute the correct summary from the on-disk state, not error
/// or return a phantom.
#[tokio::test(flavor = "current_thread")]
async fn summarize_with_cold_caches_recomputes_correctly() {
    let storage = MockStateStorage::new();
    let contract = test_contract(b"summarize_restart");
    let key = contract.key();
    let state = WrappedState::new(vec![3, 1, 4, 1, 5]);

    // Executor 1 PUTs and summarizes (warming its caches and the backing store).
    let mut e1 = cached_executor(storage.clone()).await;
    e1.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT");
    let s1 = e1
        .summarize_contract_state(key)
        .await
        .expect("summarize e1");
    assert_eq!(s1.as_ref(), blake3::hash(state.as_ref()).as_bytes());

    // Executor 2 shares the backing storage but its detector + summary caches are
    // cold (the StateStore detector is per-StateStore-instance). It must still
    // recompute the correct summary from disk.
    let mut e2 = cached_executor(storage.clone()).await;
    let s2 = e2
        .summarize_contract_state(key)
        .await
        .expect("summarize e2");
    assert_eq!(
        s2.as_ref(),
        blake3::hash(state.as_ref()).as_bytes(),
        "cold-cache executor must recompute the correct summary from disk"
    );
}

// =========================================================================
// CACHE SIZING (coverage is driven by the live hosted COUNT, not a fixed cap)
// =========================================================================

/// Build a real `OpManager` backed by a temp-dir `Config` so
/// `Ring::hosting_contracts_count()` reflects contracts hosted via
/// `op_manager.ring.host_contract(..)`. Mirrors
/// `disk_budget_gate_tests::build_op_manager`; the returned guard bundle keeps the
/// channel receivers + task monitor alive for the whole test (dropping them
/// mid-run would tear down the OpManager's channels).
async fn build_op_manager(id: &str) -> (Arc<OpManager>, Box<dyn std::any::Any>) {
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

    let guards: Box<dyn std::any::Any> = Box::new((
        notification_rx,
        ch_channel,
        wait_for_event,
        result_router_rx,
        task_monitor,
    ));
    (op_manager, guards)
}

/// The summary cache's COUNT target must cover the node's live hosted-contract
/// count, not sit at a fixed capacity. This proves it by count, not by a magic
/// threshold: for several hosted-set sizes (including well past the historical
/// 1024 cap), summarizing the whole hosted set twice must reload state ZERO times
/// on the second pass — which only holds if the cache grew to cover every hosted
/// contract. Under the old fixed 1024-entry cap the 3000/5000 cases would evict
/// the earliest keys and reload (and thus recompile a cold module) on pass 2:
/// exactly the interest-heartbeat thrash this fix removes. The small (5-byte)
/// states here keep total bytes far below the summary cache's byte budget, so the
/// COUNT target binds (coverage), not the byte backstop. Uses an uncached
/// `StateStore` so every reload is observable via `MockStateStorage::get_count`.
#[tokio::test(flavor = "current_thread")]
async fn summary_cache_covers_live_hosted_count_not_fixed_cap() {
    for n in [1024usize, 3000, 5000] {
        let storage = MockStateStorage::new();
        let (op_manager, _guards) = build_op_manager(&format!("cache_cover_{n}")).await;
        let mut exec = Executor::new_mock_wasm_uncached_with_op_manager(
            "cache_cover",
            storage.clone(),
            op_manager.clone(),
        )
        .await
        .expect("create uncached executor with op_manager");

        // Store N distinct contracts+states and host each one, driving
        // `hosting_contracts_count()` to exactly N.
        let mut keys = Vec::with_capacity(n);
        for i in 0..n {
            let contract = test_contract(format!("cache_cover_{n}_{i}").as_bytes());
            let key = contract.key();
            let state = WrappedState::new(vec![(i & 0xff) as u8, (i >> 8) as u8, 1, 2, 3]);
            exec.upsert_contract_state(
                key,
                Either::Left(state.clone()),
                RelatedContracts::default(),
                Some(contract.clone()),
            )
            .await
            .expect("PUT");
            op_manager
                .ring
                .host_contract(key, state.as_ref().len() as u64, AccessType::Get);
            keys.push(key);
        }
        assert_eq!(
            op_manager.ring.hosting_contracts_count(),
            n,
            "all {n} contracts must be hosted (this count is what drives cache sizing)"
        );

        // Pass 1: cold summarize of every hosted contract. The first call resizes
        // the cache to cover the live hosted count; all N summaries then fill it.
        for key in &keys {
            let _ = exec
                .summarize_contract_state(*key)
                .await
                .expect("summarize pass 1");
        }
        let gets_after_pass1 = storage.get_count();

        // Pass 2: summarize every hosted contract again, all unchanged. If the
        // cache covers the whole hosted set, every summary hits the fast path and
        // reloads NOTHING. A fixed cap < N would have evicted the earliest keys,
        // forcing reloads here (get_count would climb).
        for key in &keys {
            let _ = exec
                .summarize_contract_state(*key)
                .await
                .expect("summarize pass 2");
        }
        assert_eq!(
            storage.get_count(),
            gets_after_pass1,
            "pass 2 reloaded state for hosted_count={n}: the summary cache must be sized \
             to the live hosted count so no cold-module recompute happens on the interest \
             heartbeat (a fixed cap < N evicts and reloads)"
        );
    }
}
