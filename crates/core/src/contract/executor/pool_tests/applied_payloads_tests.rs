//! Executor-side rules of the applied-payload record
//! (`crate::ring::applied_payloads`): a payload is recorded only after a merge
//! committed a changed state, or when it equals the stored state byte for
//! byte. It is never recorded after a refused, broken-suppressed or unchanged
//! merge. An initial install, a corrupted-state recovery and a store through
//! `verify_and_store_contract` reset the record, before their write.
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

thread_local! {
    /// Calls of [`keeps_current`].
    static KEEP_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// A merge that always returns the current state, counting its calls. Stands
/// in for a contract that silently drops a payload it cannot use yet.
fn keeps_current(current: &[u8], _incoming: &[u8]) -> Vec<u8> {
    KEEP_CALLS.with(|calls| calls.set(calls.get() + 1));
    current.to_vec()
}

/// A merge that ran and left the state unchanged is NOT recorded, and the same
/// payload offered again reaches the merge again.
///
/// Why: an unchanged result may be a silent refusal rather than an
/// absorption. A contract can drop part of a payload under a condition that
/// later clears (an entry whose prerequisite has not arrived yet) and return
/// its old state instead of an error. If that were recorded, the receive path
/// would skip the same bytes when they are offered again after the condition
/// cleared, and the divergence could stay for good. The cost is that a
/// payload which genuinely changes nothing is merged again when re-offered.
#[tokio::test(flavor = "current_thread")]
async fn a_merge_that_leaves_the_state_unchanged_is_not_recorded() {
    let (op_manager, _guards) = build_op_manager("applied-unchanged").await;
    let (mut executor, key) = installed(&op_manager, b"applied_unchanged", b"keep").await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(keeps_current));
    KEEP_CALLS.with(|calls| calls.set(0));
    for round in 1..=2 {
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
            !op_manager
                .ring
                .applied_payloads
                .contains(&key, state_hash(&op_manager, b"other")),
            "round {round}: an unchanged merge must not be recorded"
        );
        assert_eq!(
            KEEP_CALLS.with(std::cell::Cell::get),
            round,
            "round {round}: the re-offered payload must reach the merge"
        );
    }
}

/// Refuses to merge into the stored state `[9]`, which the contract also
/// considers invalid; otherwise the incoming state wins.
fn refuses_corrupt_base(current: &[u8], incoming: &[u8]) -> Option<Vec<u8>> {
    if current == [9] {
        return None;
    }
    Some(incoming.to_vec())
}

/// A corrupted-state recovery replaces the stored state with the incoming one
/// instead of merging, so it must reset the record rather than add to it.
#[tokio::test(flavor = "current_thread")]
async fn a_corrupted_state_recovery_resets_the_record() {
    use crate::contract::executor::mock_wasm_runtime::ValidateOverride;

    let (op_manager, _guards) = build_op_manager("applied-recovery").await;
    let (mut executor, key) = installed(&op_manager, b"applied_recovery", &[9]).await;
    let set = &op_manager.ring.applied_payloads;
    let stale = state_hash(&op_manager, b"recorded-before-recovery");
    set.record_absorbed(&key, stale);

    let runtime = executor.mock_runtime_mut();
    runtime.update_overrides.insert(
        *key.id(),
        UpdateOverride::TryMergeWith(refuses_corrupt_base),
    );
    runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::ValidWhen(|s| s != [9]));

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![3])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("recovery replaces the corrupted state");
    assert!(matches!(result, crate::contract::UpsertResult::Updated(_)));
    assert!(
        !set.contains(&key, stale),
        "the record from before the recovery must be gone"
    );
    assert_eq!(
        set.len_for(&key),
        0,
        "a recovery is not a merge, so it records nothing"
    );
}

/// The ring reaper keeps records only for contracts this node hosts or
/// serves, and drops the rest.
#[tokio::test(flavor = "current_thread")]
async fn the_reaper_drops_records_for_contracts_no_longer_hosted() {
    let (op_manager, _guards) = build_op_manager("applied-reaper").await;
    let hosted = test_contract(b"applied_reaper_hosted").key();
    let gone = test_contract(b"applied_reaper_gone").key();
    let set = &op_manager.ring.applied_payloads;
    set.record_absorbed(&hosted, 1);
    set.record_absorbed(&gone, 2);
    op_manager.ring.host_contract(
        hosted,
        16,
        crate::ring::AccessType::Get,
        crate::ring::HostingCause::Other,
    );

    assert_eq!(op_manager.ring.reap_applied_payloads(), 1);
    assert!(
        set.contains(&hosted, 1),
        "a hosted contract keeps its record"
    );
    assert!(
        !set.contains(&gone, 2),
        "an unhosted, unused contract loses it"
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

/// Source pin: the initial-install reset runs BEFORE the state store, so no
/// window exists in which a record from an earlier copy describes the newly
/// installed state.
#[test]
fn the_install_reset_precedes_the_store() {
    let src = include_str!("../runtime/executor_impl.rs");
    let branch = src
        .find("if is_new_contract {\n")
        .expect("initial-install branch");
    let region = &src[branch..];
    let reset = region
        .lines()
        .position(|l| l.trim() == "set.reset(&key);")
        .expect("install branch resets the record");
    let store = region
        .lines()
        .position(|l| l.trim() == ".store(key, state_to_store, params.clone())")
        .expect("install branch stores the state");
    assert!(reset < store, "the reset must precede the store");
}

/// Source pin: the corrupted-state recovery reset runs BEFORE
/// `commit_state_update`, so no window exists in which a stale record
/// describes the replaced state.
#[test]
fn the_recovery_reset_precedes_the_commit() {
    let src = include_str!("../runtime/executor_impl.rs");
    let commit_at = src
        .find("let commit_outcome = self")
        .expect("final commit of the merge path");
    let before = &src[..commit_at];
    let gate = before
        .rfind("if recovery_performed {")
        .expect("a recovery-gated reset precedes the commit");
    let block = &before[gate..];
    assert!(
        block.lines().any(|l| l.trim() == "set.reset(&key);"),
        "the recovery-gated block before the commit must reset the record"
    );
    let after = &src[commit_at..];
    let after = &after[..after.find("\n    }\n").expect("function end")];
    assert!(
        !after.contains("set.reset(&key);"),
        "the recovery reset must not run after the commit"
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
    // The reset must sit in the `Ok` arm of the state delete, before its
    // `Err` arm: a failed delete leaves the state in place.
    let delete = body
        .find("match self.state_store.delete(key).await {")
        .expect("state delete");
    let ok_arm = delete
        + body[delete..]
            .find("Ok(()) => {")
            .expect("Ok arm of the delete");
    let err_arm = ok_arm + body[ok_arm..].find("Err(e) => {").expect("Err arm");
    let reset = body
        .match_indices("op_manager.ring.applied_payloads.reset(key);")
        .map(|(i, _)| i)
        .find(|i| {
            let line_start = body[..*i].rfind('\n').map_or(0, |n| n + 1);
            body[line_start..*i].trim().is_empty()
        })
        .expect("deleting a contract's state must reset its applied-payload record");
    assert!(
        ok_arm < reset && reset < err_arm,
        "the reset must be in the Ok arm after state_store.delete \
         (ok {ok_arm}, reset {reset}, err {err_arm})"
    );

    // Every non-merge store through `verify_and_store_contract` (the fresh
    // install and the re-store from a fetched copy) resets before it writes.
    let start = ops2
        .find("async fn verify_and_store_contract(")
        .expect("verify_and_store_contract exists");
    let body = &ops2[start..];
    let body = &body[..body.find("\n    }\n").expect("method end")];
    let reset = body
        .lines()
        .position(|l| l.trim() == "op_manager.ring.applied_payloads.reset(&key);")
        .expect("verify_and_store_contract must reset the applied-payload record");
    let store = body
        .lines()
        .position(|l| l.trim() == ".store(key, state, params)")
        .expect("state store call");
    assert!(reset < store, "the reset must precede the state store");
}
