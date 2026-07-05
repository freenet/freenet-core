//! Regression tests for issue #4151 — same-version merge-rejected spam.
//!
//! When `update_state` WASM returns `InvalidUpdateWithInfo` for an idempotent
//! re-push (incoming version == current version), the `merge_rejected_valid_local`
//! event MUST:
//!   1. Be classified as `is_invalid_update_rejection()` = true (the predicate
//!      that gates log severity).
//!   2. NOT fire at INFO level (this is tested indirectly via the predicate).
//!
//! Additionally, when the incoming state bytes are byte-identical to the stored
//! state, the short-circuit MUST return `UpsertResult::NoChange` without invoking
//! the WASM `update_state` function at all (verified by a spy-counted override).

use either::Either;
use freenet_stdlib::prelude::*;
use std::sync::Arc;

use crate::contract::executor::mock_wasm_runtime::{MockWasmRuntime, UpdateOverride};
use crate::contract::executor::{ContractExecutor, Executor, UpsertResult};
use crate::wasm_runtime::MockStateStorage;

/// Create a MockWasmRuntime executor.
async fn create_executor() -> Executor<MockWasmRuntime, MockStateStorage> {
    let storage = MockStateStorage::new();
    Executor::new_mock_wasm("merge_rejected_test", storage, None, None)
        .await
        .expect("create MockWasmRuntime executor")
}

/// Create a contract with the given code bytes and empty params.
fn make_contract(code_bytes: &[u8]) -> ContractContainer {
    let code = ContractCode::from(code_bytes.to_vec());
    let params = Parameters::from(vec![]);
    ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(code),
        params,
    )))
}

// =========================================================================
// Test: same-version rejection error is classified as is_invalid_update_rejection
// =========================================================================

/// Regression test for #4151.
///
/// When the contract WASM's `update_state` returns `InvalidUpdateWithInfo`
/// (e.g. "New state version X must be higher than current version X"), the
/// resulting `ExecutorError` MUST satisfy `is_invalid_update_rejection()`.
/// The production code uses this predicate to decide whether to log at DEBUG
/// (benign idempotent push) or INFO (unexpected merge failure).
#[tokio::test(flavor = "current_thread")]
async fn test_same_version_rejection_is_classified_as_invalid_update() {
    let mut executor = create_executor().await;

    // PUT the contract with initial state.
    let contract = make_contract(b"same_version_test");
    let key = contract.key();
    let initial_state = WrappedState::new(vec![1, 2, 3, 4]);

    executor
        .upsert_contract_state(
            key,
            Either::Left(initial_state.clone()),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("initial PUT should succeed");

    // Wire the same-version rejection override so update_state returns
    // InvalidUpdateWithInfo (exactly what website-contract emits when
    // new_version <= current_version).
    let version_rejection_reason =
        "New state version 100 must be higher than current version 100".to_string();
    executor.runtime.update_overrides.insert(
        *key.id(),
        UpdateOverride::RejectInvalidUpdate {
            reason: version_rejection_reason.clone(),
        },
    );

    // Attempt to update with a different state (different bytes so the
    // byte-equality short-circuit does NOT fire — we want the WASM path).
    let different_state = WrappedState::new(vec![5, 6, 7, 8]);
    let err = executor
        .upsert_contract_state(
            key,
            Either::Left(different_state),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("same-version update should fail");

    // The error MUST be recognized as a benign invalid-update rejection.
    // This is the predicate the production code uses to downgrade the log
    // level from INFO to DEBUG (issue #4151).
    assert!(
        err.is_invalid_update_rejection(),
        "same-version WASM rejection must satisfy is_invalid_update_rejection(); \
         got: {err:?}"
    );

    // It must also satisfy the broader is_contract_exec_rejection predicate
    // (auto-fetch gate).
    assert!(
        err.is_contract_exec_rejection(),
        "same-version rejection must also satisfy is_contract_exec_rejection()"
    );
}

// =========================================================================
// Test: byte-identical state update short-circuits without calling WASM
// =========================================================================

/// Regression test for #4151 (short-circuit).
///
/// When the incoming state bytes are identical to the stored state bytes,
/// `upsert_contract_state` MUST return `UpsertResult::NoChange` immediately
/// without invoking `update_state` in the WASM contract.  This avoids a
/// pointless WASM round-trip — and, critically, suppresses the spurious
/// `merge_rejected_valid_local` INFO log that fires when the contract's
/// version-gating logic rejects an idempotent re-push.
///
/// We verify the short-circuit by wiring a `RejectInvalidUpdate` override:
/// if `update_state` were called, the executor would return `Err`; if the
/// short-circuit fires, we get `Ok(UpsertResult::NoChange)`.
#[tokio::test(flavor = "current_thread")]
async fn test_identical_state_short_circuits_before_wasm_update() {
    let mut executor = create_executor().await;

    // PUT the contract with initial state.
    let contract = make_contract(b"short_circuit_test");
    let key = contract.key();
    let initial_state = WrappedState::new(vec![10, 20, 30, 40]);

    executor
        .upsert_contract_state(
            key,
            Either::Left(initial_state.clone()),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("initial PUT should succeed");

    // Wire the rejection override so that if update_state is called it errors.
    // The short-circuit must prevent this from being reached.
    executor.runtime.update_overrides.insert(
        *key.id(),
        UpdateOverride::RejectInvalidUpdate {
            reason: "short-circuit spy: update_state must NOT be called for identical state"
                .to_string(),
        },
    );

    // Attempt to update with the SAME state bytes.
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(initial_state.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("identical-state upsert must succeed (short-circuit, not WASM error)");

    assert!(
        matches!(result, UpsertResult::NoChange),
        "byte-identical state update must return NoChange (short-circuit), got: {result:?}"
    );
}

// =========================================================================
// Test: ExecutorError predicate for the inline error message format
// =========================================================================

/// Unit test for the `is_invalid_update_rejection` predicate matching the
/// exact string that `website-contract` emits for same-version pushes.
///
/// This is a plain unit test (no executor needed) that guards against
/// string-format drift between the predicate and the contract's error message.
#[test]
fn test_is_invalid_update_rejection_matches_website_contract_format() {
    use crate::contract::executor::ExecutorError;
    use freenet_stdlib::client_api::ContractError as StdContractError;

    let key = crate::contract::executor::test_fixtures::make_contract_key();
    // Exact string format from website-contract/src/lib.rs:179
    let cause_string = "invalid contract update, reason: \
                        New state version 100 must be higher than current version 100";
    let err = ExecutorError::request(StdContractError::update_exec_error(key, cause_string));

    assert!(
        err.is_invalid_update_rejection(),
        "predicate must match website-contract's same-version error format"
    );
}

// =========================================================================
// Test: cross-node non-delta update with missing params is classified as
//       is_missing_contract_parameters (issue #3279)
// =========================================================================

/// Regression test for issue #3279 (Part 2).
///
/// Reproduces the "missing contract parameters" failure that a node hits
/// when it applies a cross-node full-state (non-delta) UPDATE for a contract
/// whose parameters are absent from its `state_store`. This is the exact
/// on-disk shape the issue describes: the node has the contract *state* (and,
/// in production, the contract *code*) but never persisted the *params* —
/// e.g. because an earlier code-less GET response could not cache them, or
/// because of the documented `StateStore::store` partial-failure window where
/// the state write succeeds but the params write fails, orphaning state
/// without params.
///
/// The upsert MUST fail with an error that satisfies
/// `is_missing_contract_parameters()`. That predicate is the load-bearing
/// discriminator the whole recovery chain depends on:
///   - the inbound broadcast/relay path
///     (`update/op_ctx_task.rs::drive_relay_broadcast_to`) triggers
///     `try_auto_fetch_contract` only when the full-state merge failed
///     because code/params are missing (NOT on a contract-side WASM
///     rejection);
///   - the originator UPDATE self-heal
///     (`update/op_ctx_task.rs`) auto-fetches on exactly this predicate.
///
/// If this classification silently broke (e.g. the cause string drifted, or
/// the error were mapped to a different `ContractError` variant), every
/// missing-params recovery path would misclassify and issue #3279 would
/// regress into a hard, unrecoverable "missing contract parameters" on every
/// cross-node update — with no auto-fetch to heal it.
#[tokio::test(flavor = "current_thread")]
async fn test_missing_params_non_delta_update_is_classified_as_missing_parameters() {
    let storage = MockStateStorage::new();
    let mut executor = Executor::new_mock_wasm("missing_params_test", storage.clone(), None, None)
        .await
        .expect("create MockWasmRuntime executor");

    let contract = make_contract(b"missing_params_3279");
    let key = contract.key();
    let initial_state = WrappedState::new(vec![1, 2, 3, 4]);

    // A clean initial PUT: contract code lands in the runtime store, state and
    // params land on disk.
    executor
        .upsert_contract_state(
            key,
            Either::Left(initial_state.clone()),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("initial PUT should succeed");

    // Now model the cross-node inconsistency issue #3279 reports: the node
    // still has the contract state (and code) but has LOST its params (an
    // earlier code-less GET could not cache them, or the documented
    // `StateStore::store` partial-failure window orphaned state-without-params).
    storage.remove_params_for_test(&key);
    assert!(
        storage.get_stored_state(&key).is_some(),
        "state must remain on disk after dropping params"
    );
    assert!(
        storage.get_stored_params(&key).is_none(),
        "params must be absent — this is the #3279 orphaned-state shape"
    );

    // Now apply a cross-node full-state (non-delta) UPDATE with code == None
    // (the wire delivery of a broadcast UPDATE carries no contract code). The
    // executor must take the `else` branch that reads params from the
    // state_store, find them missing, and return the typed error.
    let incoming_state = WrappedState::new(vec![5, 6, 7, 8]);
    let err = executor
        .upsert_contract_state(
            key,
            Either::Left(incoming_state),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("non-delta update with missing params must fail");

    assert!(
        err.is_missing_contract_parameters(),
        "missing-params full-state update MUST satisfy \
         is_missing_contract_parameters() — the recovery-chain discriminator \
         for issue #3279; got: {err:?}"
    );

    // It must NOT be misclassified as a contract-side WASM rejection: that
    // would suppress auto-fetch (the merge is assumed to have run against
    // present code/params), permanently wedging the cross-node update.
    assert!(
        !err.is_contract_exec_rejection(),
        "missing-params error must NOT be classified as a contract exec \
         rejection, or auto-fetch recovery would be suppressed"
    );
}
