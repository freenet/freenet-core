//! Executor-side rules of the applied-payload record
//! (`crate::ring::applied_payloads`): a payload is recorded only after its
//! merge commits (changed or unchanged), never after a refused or suppressed
//! merge, and an initial install or corrupted-state recovery resets the record.
//!
//! These run the real `bridged_upsert_contract_state` through
//! `Executor<MockWasmRuntime, MockStateStorage>` with a real `OpManager`, so the
//! record lives on the same `Ring` the network receive paths read.

use either::Either;
use freenet_stdlib::prelude::*;
use std::sync::Arc;

use crate::config::ConfigArgs;
use crate::contract::executor::mock_wasm_runtime::UpdateOverride;
use crate::contract::executor::{ContractExecutor, Executor, OperationMode};
use crate::node::OpManager;
use crate::wasm_runtime::MockStateStorage;

use super::super::mock_runtime::test::create_test_contract as test_contract;

/// Same wiring as `disk_budget_gate_tests::build_op_manager`.
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

fn state_hash(op_manager: &OpManager, bytes: &[u8]) -> u64 {
    op_manager.ring.applied_payloads.payload_hash(false, bytes)
}

fn delta_hash(op_manager: &OpManager, bytes: &[u8]) -> u64 {
    op_manager.ring.applied_payloads.payload_hash(true, bytes)
}

/// Install `initial` as a fresh contract and return the executor and key.
async fn installed(
    op_manager: &Arc<OpManager>,
    seed: &[u8],
    initial: &[u8],
) -> (
    Executor<crate::contract::executor::mock_wasm_runtime::MockWasmRuntime, MockStateStorage>,
    ContractKey,
) {
    let contract = test_contract(seed);
    let key = contract.key();
    let mut executor =
        Executor::new_mock_wasm("t", MockStateStorage::new(), None, Some(op_manager.clone()))
            .await
            .expect("build mock-wasm executor");
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(initial.to_vec())),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("initial install");
    (executor, key)
}

#[tokio::test(flavor = "current_thread")]
async fn a_committed_changing_merge_is_recorded() {
    let (op_manager, _guards) = build_op_manager("applied-changed").await;
    let (mut executor, key) = installed(&op_manager, b"applied_changed", b"s0").await;
    let set = &op_manager.ring.applied_payloads;

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(b"s1".to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("merge");
    assert!(matches!(result, crate::contract::UpsertResult::Updated(_)));
    assert!(set.contains(&key, state_hash(&op_manager, b"s1")));

    executor
        .upsert_contract_state(
            key,
            Either::Right(StateDelta::from(b"d1".to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("delta merge");
    assert!(
        set.contains(&key, delta_hash(&op_manager, b"d1")),
        "a committed delta must be recorded under the delta discriminant"
    );
    assert!(
        !set.contains(&key, state_hash(&op_manager, b"d1")),
        "the same bytes as a full state were never merged"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_merge_that_leaves_the_state_unchanged_is_recorded() {
    let (op_manager, _guards) = build_op_manager("applied-unchanged").await;
    let (mut executor, key) = installed(&op_manager, b"applied_unchanged", b"keep").await;
    // A merge that always keeps the current state: the incoming payload is
    // absorbed without changing anything.
    executor.runtime.update_overrides.insert(
        *key.id(),
        UpdateOverride::MergeWith(|current: &[u8], _incoming: &[u8]| current.to_vec()),
    );
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(b"other".to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("merge");
    assert!(matches!(result, crate::contract::UpsertResult::NoChange));
    assert!(
        op_manager
            .ring
            .applied_payloads
            .contains(&key, state_hash(&op_manager, b"other"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_byte_identical_state_is_recorded() {
    let (op_manager, _guards) = build_op_manager("applied-identical").await;
    let (mut executor, key) = installed(&op_manager, b"applied_identical", b"same").await;
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(b"same".to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("identical re-push");
    assert!(
        op_manager
            .ring
            .applied_payloads
            .contains(&key, state_hash(&op_manager, b"same"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_refused_merge_is_not_recorded() {
    let (op_manager, _guards) = build_op_manager("applied-refused").await;
    let (mut executor, key) = installed(&op_manager, b"applied_refused", b"s0").await;
    executor.runtime.update_overrides.insert(
        *key.id(),
        UpdateOverride::RejectInvalidUpdate {
            reason: "refused".into(),
        },
    );
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(b"bad".to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("merge must be refused");
    assert!(
        !op_manager
            .ring
            .applied_payloads
            .contains(&key, state_hash(&op_manager, b"bad")),
        "a refused merge must stay retryable"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_merge_suppressed_for_a_broken_contract_is_not_recorded() {
    let (op_manager, _guards) = build_op_manager("applied-broken").await;
    let (mut executor, key) = installed(&op_manager, b"applied_broken", b"s0").await;
    op_manager.ring.record_broken_invariant(
        key,
        crate::ring::BrokenInvariant::NonIdempotent,
        crate::ring::BrokenInvariantDetector::IdenticalInput,
        "test",
    );
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(b"s1".to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("suppressed merge returns Ok");
    assert!(matches!(result, crate::contract::UpsertResult::NoChange));
    assert!(
        !op_manager
            .ring
            .applied_payloads
            .contains(&key, state_hash(&op_manager, b"s1")),
        "nothing was stored, so the payload was not absorbed"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_initial_install_resets_the_record() {
    let (op_manager, _guards) = build_op_manager("applied-install").await;
    let contract = test_contract(b"applied_install");
    let key = contract.key();
    let stale = state_hash(&op_manager, b"seen-before");
    // A record left over from an earlier copy of this contract.
    op_manager
        .ring
        .applied_payloads
        .record_absorbed(&key, stale);

    let mut executor =
        Executor::new_mock_wasm("t", MockStateStorage::new(), None, Some(op_manager.clone()))
            .await
            .expect("build mock-wasm executor");
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(b"fresh".to_vec())),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("install");
    assert!(
        !op_manager.ring.applied_payloads.contains(&key, stale),
        "an install replaces the state without a merge, so the old record \
         must not survive it"
    );
}

/// Source pins for the reset hooks that the tests above cannot reach without
/// a full `Executor<Runtime>` and a `RuntimePool`.
#[test]
fn eviction_and_reclamation_reset_the_record() {
    let ops = include_str!("../../../operations.rs");
    let start = ops
        .find("pub(crate) fn reclaim_evicted_contract(")
        .expect("reclaim_evicted_contract exists");
    let body = &ops[start..];
    let body = &body[..body.find("\n}\n").expect("function end")];
    let in_use_return = body
        .find("pending_reclamation_add")
        .expect("in-use early return");
    let reset = body
        .lines()
        .position(|l| l.trim() == "op_manager.ring.applied_payloads.reset(&key);")
        .expect(
            "reclaim_evicted_contract must reset the applied-payload record at \
             the eviction decision",
        );
    let reset_offset: usize = body.lines().take(reset).map(|l| l.len() + 1).sum();
    assert!(
        reset_offset > in_use_return,
        "the reset belongs after the in-use early return (the state stays \
         hosted there)"
    );

    let ops2 = include_str!("../runtime/contract_ops.rs");
    let start = ops2
        .find("pub(super) async fn reclaim_contract_storage(")
        .expect("reclaim_contract_storage exists");
    let body = &ops2[start..];
    let body = &body[..body.find("\n    }\n").expect("method end")];
    assert!(
        body.lines()
            .any(|l| l.trim() == "op_manager.ring.applied_payloads.reset(key);"),
        "deleting a contract's state must reset its applied-payload record"
    );
}
