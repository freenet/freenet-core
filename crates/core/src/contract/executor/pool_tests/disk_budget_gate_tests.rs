//! Disk-budget admission-gate wiring tests for the V1 `Executor<_>` PUT/UPDATE
//! chokepoints (#4683, PR 3/4).
//!
//! The lower-level `DiskUsageTracker` / `HostingManager` admit+record arithmetic
//! is unit-tested in isolation (boundary, overflow, growth-only, concurrency) in
//! `ring/hosting.rs` and `ring/hosting/disk_usage.rs`. The V2 delegate path is
//! covered by a `StateAdmitCallback` closure in `wasm_runtime/delegate_api.rs`.
//!
//! What was NOT covered before this file: the *executor-side wiring* of those
//! gates — that `bridged_upsert_contract_state` actually calls
//! `admit_wasm_write` → `record_wasm_write` → `admit_state_write` in the right
//! order, and that every rollback branch reverses exactly the right charge. A
//! wiring bug the tracker unit tests could NOT catch — a rollback that forgets
//! `record_wasm_removed`, double-reverses it, or calls `admit_state_write` where
//! `admit_state_update` was intended — is exactly what these tests pin.
//!
//! These exercise the generic bridged path via `Executor<MockWasmRuntime,
//! MockStateStorage>`: `MockWasmRuntime` delegates PUT/UPDATE into the same
//! `bridged_upsert_contract_state` the production `Runtime` uses, so the gate
//! call sites in `executor_impl.rs` (220/246/417/434 …) are the real ones. No
//! wasm compilation needed — the mock stores state without executing code.
//!
//! Scope note: the `contract_ops.rs` gate sites (`perform_contract_put` re-PUT
//! growth-only, `verify_and_store_contract`) live on `Executor<Runtime>` only
//! (real wasmtime, unreachable via any mock runtime) and would require a compiled
//! test-contract fixture to reach past `validate_state`. They call the SAME
//! `Ring::{admit_wasm_write, admit_state_write, admit_state_update,
//! record_wasm_write, record_wasm_removed}` methods proven here; the
//! charge/reverse wiring pattern is identical. The high-value, error-prone
//! rollback branches (wasm-charge reversal on a state-gate rejection) are the
//! bridged ones covered below.

use either::Either;
use freenet_stdlib::prelude::*;
use std::sync::Arc;

use crate::config::ConfigArgs;
use crate::contract::executor::{ContractExecutor, Executor, OperationMode};
use crate::node::OpManager;
use crate::ring::MIN_DEFAULT_HOSTING_BUDGET_BYTES;
use crate::wasm_runtime::MockStateStorage;

use super::super::mock_runtime::test::create_test_contract as test_contract;

/// Build a real `OpManager` backed by a temp-dir `Config`, mirroring the wiring
/// in `runtime/pool.rs::test_eviction_emits_hosting_retraction`.
///
/// Returns the `OpManager` plus a boxed guard bundle whose `Drop` end-of-scope
/// keeps the channel receivers + task monitor alive for the whole test (dropping
/// them mid-run would tear down the OpManager's channels). The guard's concrete
/// types are erased behind `Box<dyn Any>` so this helper needs no fragile type
/// annotations for the channel halves.
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

    // Keep the receivers + task monitor alive for the test scope.
    let guards: Box<dyn std::any::Any> = Box::new((
        notification_rx,
        ch_channel,
        wait_for_event,
        result_router_rx,
        task_monitor,
    ));
    (op_manager, guards)
}

/// Seed the disk tracker with `state_seed` state bytes for a throwaway key, then
/// install a real `disk_budget_bytes` = `budget`. After this the gates
/// (`admit_wasm_write` / `admit_state_write`) reject any charge that would push
/// the aggregate past `budget`.
///
/// `budget` MUST be `>= MIN_DEFAULT_HOSTING_BUDGET_BYTES` (128 MiB): the recompute
/// clamps the computed disk budget to `[MIN, cap]`, so a sub-MIN target would be
/// silently raised to MIN and the gate would never fire. We therefore run at
/// realistic scale — `state_seed` is a large integer, but it is only a counter
/// (no real disk is written).
fn install_tight_budget(
    op_manager: &OpManager,
    seed_key: ContractKey,
    state_seed: u64,
    budget: u64,
) {
    assert!(
        budget >= MIN_DEFAULT_HOSTING_BUDGET_BYTES,
        "budget must be >= MIN or the recompute clamps it up"
    );
    op_manager
        .ring
        .seed_disk_tracker_for_test([(seed_key, state_seed)]);
    // pct=1.0 over an injected `available` of `budget - state_seed` yields
    // `disk_budget = clamp(1.0 * (used + available), MIN, cap)` = `budget`
    // (used == state_seed, cap == budget so the upper clamp is a no-op).
    // `disk_budget_bytes` (what the aggregate gate reads) is stored as this raw
    // disk budget BEFORE the ram-min that only affects the effective cache
    // budget. See `HostingManager::recompute_effective_budget`.
    op_manager.ring.configure_disk_budget_for_test(1.0, budget);
    let available = budget.saturating_sub(state_seed);
    op_manager
        .ring
        .recompute_effective_budget_for_test(available);
    assert_eq!(
        op_manager.ring.disk_budget_bytes_for_test(),
        budget,
        "test setup: disk_budget_bytes must be exactly the target budget"
    );
}

/// The wasm blob length that `store_contract` charges for a `test_contract`
/// container: the code bytes wrapped inside it.
fn blob_len_of(contract: &ContractContainer) -> u64 {
    contract.data().len() as u64
}

/// PR-3 wiring: a fresh PUT whose new state pushes aggregate disk past the budget
/// must be REJECTED at the `admit_state_write` chokepoint, AFTER the wasm blob was
/// charged — and the rejection's rollback must reverse that wasm charge exactly
/// once. Pins `executor_impl.rs:220/246/417/434`.
#[tokio::test(flavor = "current_thread")]
async fn v1_put_over_budget_rejects_and_reverses_wasm_charge() {
    let (op_manager, _guards) = build_op_manager("disk-budget-put-reject").await;

    let contract = test_contract(b"disk_budget_put_reject_contract");
    let key = contract.key();
    let blob_len = blob_len_of(&contract);
    let state = WrappedState::new(vec![7u8; 32]);
    let state_len = state.as_ref().len() as u64;

    // A throwaway seed key distinct from the contract under test — it only gives
    // the tracker a nonzero seeded state total.
    let seed_key = test_contract(b"disk_budget_seed_key").key();

    // Budget at the MIN floor (128 MiB — the recompute cannot install less).
    // Sized so the wasm charge fits but wasm + state does not:
    //   seed = B - blob_len - 1  → after wasm charge aggregate = B - 1 (admits)
    //   then + state_len  → over B (state gate rejects).
    let budget = MIN_DEFAULT_HOSTING_BUDGET_BYTES;
    let state_seed = budget - blob_len - 1;
    assert!(
        state_seed + blob_len + state_len > budget,
        "test math: wasm+state must exceed budget"
    );
    install_tight_budget(&op_manager, seed_key, state_seed, budget);

    let wasm_before = op_manager
        .ring
        .disk_usage_stats_for_test()
        .expect("tracker seeded")
        .wasm_bytes;

    let mut executor =
        Executor::new_mock_wasm("t", MockStateStorage::new(), None, Some(op_manager.clone()))
            .await
            .expect("build mock-wasm executor");

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await;

    // Rejected as a non-fatal PUT error carrying the disk-budget cause.
    match result {
        Err(err) => {
            let msg = err.to_string().to_lowercase();
            assert!(
                msg.contains("disk budget"),
                "PUT rejection must cite the disk budget, got: {msg}"
            );
        }
        Ok(other) => panic!("over-budget PUT must be rejected, got Ok({other:?})"),
    }

    // The wasm charge must have been reversed EXACTLY once — not left leaked
    // (missing `record_wasm_removed`) and not double-reversed below the seed.
    let wasm_after = op_manager
        .ring
        .disk_usage_stats_for_test()
        .expect("tracker still seeded")
        .wasm_bytes;
    assert_eq!(
        wasm_after, wasm_before,
        "rejected PUT must reverse the wasm charge exactly once (leak or \
         double-reverse would move wasm_bytes off its pre-PUT value)"
    );

    // The rejected state must NOT have landed.
    let stored = executor.fetch_contract(key, false).await;
    assert!(
        matches!(stored, Ok((None, _)) | Err(_)),
        "rejected PUT must leave no state on disk, got {stored:?}"
    );
}

/// PR-3 wiring: with ample budget the SAME fresh PUT is admitted, the wasm blob
/// is charged and stays charged, and the state lands. This is the positive
/// control proving the rejection above is caused by the budget — not by the
/// executor refusing every PUT. Pins `executor_impl.rs:246` (charge that is NOT
/// reversed on the success path) + the state store.
#[tokio::test(flavor = "current_thread")]
async fn v1_put_under_budget_admits_and_charges_wasm() {
    let (op_manager, _guards) = build_op_manager("disk-budget-put-admit").await;

    let contract = test_contract(b"disk_budget_put_admit_contract");
    let key = contract.key();
    let blob_len = blob_len_of(&contract);
    let state = WrappedState::new(vec![9u8; 32]);
    let seed_key = test_contract(b"disk_budget_admit_seed_key").key();

    // Roomy budget at the MIN floor: seed small, budget 128 MiB — wasm + state
    // both fit comfortably.
    let budget = MIN_DEFAULT_HOSTING_BUDGET_BYTES;
    install_tight_budget(&op_manager, seed_key, 1_000, budget);

    let wasm_before = op_manager
        .ring
        .disk_usage_stats_for_test()
        .expect("seeded")
        .wasm_bytes;

    let mut executor =
        Executor::new_mock_wasm("t", MockStateStorage::new(), None, Some(op_manager.clone()))
            .await
            .expect("build mock-wasm executor");

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("under-budget PUT must be admitted");
    assert!(
        matches!(result, crate::contract::UpsertResult::Updated(_)),
        "admitted PUT must return Updated, got {result:?}"
    );

    // The wasm blob was charged and — since the PUT succeeded — NOT reversed.
    let wasm_after = op_manager
        .ring
        .disk_usage_stats_for_test()
        .expect("seeded")
        .wasm_bytes;
    assert_eq!(
        wasm_after,
        wasm_before + blob_len,
        "admitted PUT must charge the wasm blob and keep it charged"
    );

    // State landed.
    let (fetched, _) = executor
        .fetch_contract(key, false)
        .await
        .expect("fetch after admitted PUT");
    assert_eq!(
        fetched.map(|s| s.as_ref().to_vec()),
        Some(state.as_ref().to_vec()),
        "admitted PUT state must be readable"
    );
}

/// Build two contract containers that share ONE code hash (same code bytes) but
/// have DIFFERENT params — hence different `ContractInstanceId`s. This is the
/// River "many rooms, one room-contract WASM" shape (#2380) that #4218's
/// disk-budget dedup must not double-count.
fn same_code_two_instances(code_bytes: &[u8]) -> (ContractContainer, ContractContainer) {
    let code = std::sync::Arc::new(ContractCode::from(code_bytes.to_vec()));
    let mk = |params: Vec<u8>| {
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            code.clone(),
            Parameters::from(params),
        )))
    };
    (mk(vec![1, 1, 1]), mk(vec![2, 2, 2]))
}

/// Regression test for issue #4218 (disk-budget double-count via the executor
/// gate): PUTting a SECOND instance of already-stored code (same code hash,
/// different params) must NOT charge the shared wasm blob against the disk
/// budget a second time. The disk-budget dedup probe is keyed by CODE HASH
/// (`code_blob_stored`), so the second PUT is only indexed — never re-charged.
///
/// The budget is sized so exactly one blob plus both states fit. If the second
/// instance re-charged the shared blob (the pre-#4218 instance-keyed probe
/// behavior), the second PUT would exceed the budget and be REJECTED at the
/// wasm gate; with the code-hash dedup it is admitted and the wasm total stays
/// charged exactly once. Pins the `executor_impl.rs` gate wiring against the
/// dedup path.
#[tokio::test(flavor = "current_thread")]
async fn v1_put_second_instance_same_code_not_double_charged() {
    let (op_manager, _guards) = build_op_manager("disk-budget-dedup-second-instance").await;

    // Distinct code so this test's code hash is independent of other tests.
    let code_bytes = vec![0xABu8; 4096];
    let (contract1, contract2) = same_code_two_instances(&code_bytes);
    let key1 = contract1.key();
    let key2 = contract2.key();
    assert_eq!(
        key1.code_hash(),
        key2.code_hash(),
        "the two instances must share one code hash"
    );
    assert_ne!(key1, key2, "the two instances must have different keys");

    let blob_len = blob_len_of(&contract1);
    let state1 = WrappedState::new(vec![1u8; 32]);
    let state2 = WrappedState::new(vec![2u8; 32]);
    let state1_len = state1.as_ref().len() as u64;
    let state2_len = state2.as_ref().len() as u64;

    let seed_key = test_contract(b"disk_budget_dedup_seed_key").key();

    // Budget sized so ONE blob + both states fit exactly; a SECOND blob charge
    // (the double-count bug) would overshoot and reject the second PUT.
    let budget = MIN_DEFAULT_HOSTING_BUDGET_BYTES;
    let state_seed = budget - blob_len - state1_len - state2_len;
    assert!(
        state_seed + 2 * blob_len + state1_len + state2_len > budget,
        "test math: a second wasm charge must overshoot the budget"
    );
    install_tight_budget(&op_manager, seed_key, state_seed, budget);

    let wasm_before = op_manager
        .ring
        .disk_usage_stats_for_test()
        .expect("tracker seeded")
        .wasm_bytes;

    let mut executor =
        Executor::new_mock_wasm("t", MockStateStorage::new(), None, Some(op_manager.clone()))
            .await
            .expect("build mock-wasm executor");

    // PUT instance 1: admitted, charges the shared blob once.
    executor
        .upsert_contract_state(
            key1,
            Either::Left(state1.clone()),
            RelatedContracts::default(),
            Some(contract1.clone()),
        )
        .await
        .expect("first instance PUT must be admitted");

    // PUT instance 2 (same code, different params): must be admitted WITHOUT a
    // second wasm charge. Pre-fix, the instance-keyed probe re-charged the blob,
    // overshooting the budget and rejecting this PUT.
    let result2 = executor
        .upsert_contract_state(
            key2,
            Either::Left(state2.clone()),
            RelatedContracts::default(),
            Some(contract2.clone()),
        )
        .await
        .expect("second instance of shared code must be admitted (no blob double-count)");
    assert!(
        matches!(result2, crate::contract::UpsertResult::Updated(_)),
        "second instance PUT must return Updated, got {result2:?}"
    );

    // The shared wasm blob was charged EXACTLY ONCE across both PUTs.
    let wasm_after = op_manager
        .ring
        .disk_usage_stats_for_test()
        .expect("tracker seeded")
        .wasm_bytes;
    assert_eq!(
        wasm_after,
        wasm_before + blob_len,
        "the shared code blob must be charged exactly once for two instances \
         sharing it (#4218 disk-budget double-count)"
    );

    // Both instances are stored.
    assert!(
        matches!(executor.fetch_contract(key1, false).await, Ok((Some(_), _))),
        "instance 1 state must be readable"
    );
    assert!(
        matches!(executor.fetch_contract(key2, false).await, Ok((Some(_), _))),
        "instance 2 state must be readable"
    );
}

/// Call-site pin for issue #4218: the disk-budget dedup probe at the executor
/// gate sites MUST be keyed by code hash via `code_blob_stored`, NOT by instance
/// id via `fetch_contract` / `fetch_contract_code`. Reverting either probe back
/// to the instance-keyed form (which double-counts a second instance of shared
/// code across pool executors) must fail a test — the `ContractStore`-level
/// regression tests alone would not catch a call-site revert.
#[test]
fn dedup_probe_call_sites_use_code_blob_stored() {
    let executor_impl_src = include_str!("../runtime/executor_impl.rs");
    let contract_ops_src = include_str!("../runtime/contract_ops.rs");

    // executor_impl.rs bridged PUT gate must probe by code hash.
    assert!(
        executor_impl_src.contains("self.runtime.code_blob_stored(key.code_hash())"),
        "executor_impl.rs disk-budget dedup gate must probe via code_blob_stored (#4218)"
    );
    assert!(
        !executor_impl_src.contains("self.runtime.fetch_contract_code(&key, &params).is_none()"),
        "executor_impl.rs must NOT gate the wasm charge on the instance-keyed \
         fetch_contract_code probe (double-counts shared code across executors, #4218)"
    );

    // contract_ops.rs verify_and_store gate must probe by code hash. (Matched as
    // a substring so rustfmt line-wrapping of the call chain can't break the pin.)
    assert!(
        contract_ops_src.contains(".code_blob_stored(key.code_hash())"),
        "contract_ops.rs disk-budget dedup gate must probe via code_blob_stored (#4218)"
    );
    assert!(
        !contract_ops_src.contains(".fetch_contract(&key, &params)"),
        "contract_ops.rs must NOT gate the wasm charge on the instance-keyed \
         fetch_contract probe (double-counts shared code across executors, #4218)"
    );
}
