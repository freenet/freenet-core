//! Mock Fidelity Conformance Tests
//!
//! These tests catch **behavioral drift** between `MockRuntime` and `MockWasmRuntime`
//! (and by extension, the production `Runtime`).
//!
//! ## Background
//!
//! `MockRuntime` has its own `ContractExecutor` implementation (~350 lines) with
//! hash-based merge semantics. `MockWasmRuntime` delegates to the same `bridged_*`
//! methods used by the production `Runtime`, giving it much higher fidelity.
//!
//! The `ContractExecutor` trait enforces interface compatibility at compile time,
//! but it cannot enforce **behavioral** equivalence. These tests fill that gap
//! by exercising both runtimes with identical inputs and asserting key invariants.
//!
//! ## What these tests catch
//!
//! - New production behavior added to `bridged_*` methods that `MockRuntime` doesn't
//!   replicate (e.g., the BSC emission bug from PR #3110)
//! - Regression in `MockWasmRuntime` that breaks parity with production paths
//! - Accidental removal of subscriber notification, state storage, or key indexing
//!
//! ## Known intentional differences (not bugs)
//!
//! | Behavior | MockRuntime | MockWasmRuntime |
//! |----------|-------------|-----------------|
//! | Merge strategy | Hash-based (largest hash wins) | Production validate→update pipeline |
//! | `register_contract_notifier` | No-op | Tracks subscribers (production path) |
//! | `get_subscription_info` | Returns `vec![]` | Returns real subscription data |
//! | `notify_subscription_error` | No-op | Sends errors to subscribers |
//! | `remove_client` | No-op (trait default) | Cleans up channels and summaries |
//! | CRDT emulation | Supported (version-based LWW) | Not supported |
//!
//! See issue #3141 (CI & Testing Redesign) and #3154 (Mock fidelity audits).

use either::Either;
use freenet_stdlib::prelude::*;

use crate::contract::executor::mock_runtime::MockRuntime;
use crate::contract::executor::mock_wasm_runtime::MockWasmRuntime;
use crate::contract::executor::{ContractExecutor, Executor};
use crate::wasm_runtime::MockStateStorage;

/// Helper to create a MockRuntime executor.
async fn create_mock_runtime_executor() -> Executor<MockRuntime, MockStateStorage> {
    let storage = MockStateStorage::new();
    Executor::new_mock_in_memory("conformance_mock", storage, None, None)
        .await
        .expect("create MockRuntime executor")
}

/// Helper to create a MockWasmRuntime executor.
async fn create_mock_wasm_executor() -> Executor<MockWasmRuntime, MockStateStorage> {
    let storage = MockStateStorage::new();
    Executor::new_mock_wasm("conformance_wasm", storage, None, None)
        .await
        .expect("create MockWasmRuntime executor")
}

use super::super::mock_runtime::test::create_test_contract as test_contract;

// =========================================================================
// Invariant: PUT new contract → Updated result with same state
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn conformance_put_new_contract_both_return_updated() {
    let mut mock = create_mock_runtime_executor().await;
    let mut wasm = create_mock_wasm_executor().await;

    // Non-WASM bytes are fine here: neither runtime executes contract code
    // during PUT (upsert_contract_state stores state without validation).
    let contract = test_contract(b"conformance_put_test");
    let key = contract.key();
    let state = WrappedState::new(vec![10, 20, 30]);

    let mock_result = mock
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("MockRuntime PUT should succeed");

    let wasm_result = wasm
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("MockWasmRuntime PUT should succeed");

    assert!(
        matches!(mock_result, crate::contract::UpsertResult::Updated(_)),
        "MockRuntime: new contract PUT must return Updated"
    );
    assert!(
        matches!(wasm_result, crate::contract::UpsertResult::Updated(_)),
        "MockWasmRuntime: new contract PUT must return Updated"
    );
}

// =========================================================================
// Invariant: FETCH after PUT returns same state on both runtimes
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn conformance_fetch_returns_same_state() {
    let mut mock = create_mock_runtime_executor().await;
    let mut wasm = create_mock_wasm_executor().await;

    let contract = test_contract(b"conformance_fetch_test");
    let key = contract.key();
    let state = WrappedState::new(vec![42, 43, 44, 45]);

    // PUT on both
    mock.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    wasm.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    // FETCH on both
    let (mock_state, mock_code) = mock.fetch_contract(key, true).await.unwrap();
    let (wasm_state, wasm_code) = wasm.fetch_contract(key, true).await.unwrap();

    assert_eq!(
        mock_state.as_ref().map(|s| s.as_ref()),
        wasm_state.as_ref().map(|s| s.as_ref()),
        "Both runtimes must return the same state after PUT"
    );

    // Verify the fetched state is exactly what we stored
    assert_eq!(
        mock_state.as_ref().map(|s| s.as_ref()),
        Some(state.as_ref()),
        "Fetched state must equal the originally stored state"
    );

    assert!(
        mock_code.is_some(),
        "MockRuntime must return contract code when requested"
    );
    assert!(
        wasm_code.is_some(),
        "MockWasmRuntime must return contract code when requested"
    );
}

// =========================================================================
// Invariant: lookup_key works identically on both after storing a contract
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn conformance_lookup_key_after_put() {
    let mut mock = create_mock_runtime_executor().await;
    let mut wasm = create_mock_wasm_executor().await;

    let contract = test_contract(b"conformance_lookup_test");
    let key = contract.key();
    let instance_id = *key.id();
    let state = WrappedState::new(vec![1]);

    // PUT on both
    mock.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    wasm.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    let mock_key = mock.lookup_key(&instance_id);
    let wasm_key = wasm.lookup_key(&instance_id);

    assert!(mock_key.is_some(), "MockRuntime must find key after PUT");
    assert!(
        wasm_key.is_some(),
        "MockWasmRuntime must find key after PUT"
    );
    assert_eq!(
        mock_key.unwrap(),
        wasm_key.unwrap(),
        "Both runtimes must return the same ContractKey"
    );
}

// =========================================================================
// Invariant: summarize_contract_state succeeds on both after PUT
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn conformance_summarize_succeeds_on_both() {
    let mut mock = create_mock_runtime_executor().await;
    let mut wasm = create_mock_wasm_executor().await;

    let contract = test_contract(b"conformance_summarize_test");
    let key = contract.key();
    let state = WrappedState::new(vec![100, 200]);

    // PUT on both
    mock.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    wasm.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    let mock_summary = mock
        .summarize_contract_state(key)
        .await
        .expect("MockRuntime summarize must succeed");
    let wasm_summary = wasm
        .summarize_contract_state(key)
        .await
        .expect("MockWasmRuntime summarize must succeed");

    // Both use blake3 hashing for summarization, so summaries must be identical.
    assert_eq!(
        mock_summary.as_ref(),
        wasm_summary.as_ref(),
        "Summaries must match: both runtimes use blake3 hashing"
    );

    // Verify it's a valid blake3 hash (32 bytes)
    assert_eq!(
        mock_summary.as_ref().len(),
        32,
        "Summary must be a 32-byte blake3 hash"
    );

    // Verify it matches the expected blake3 hash of the input state
    let expected = blake3::hash(state.as_ref());
    assert_eq!(mock_summary.as_ref(), expected.as_bytes());
}

// =========================================================================
// Invariant: UPDATE with full state on existing contract succeeds on both
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn conformance_update_full_state_both_succeed() {
    let mut mock = create_mock_runtime_executor().await;
    let mut wasm = create_mock_wasm_executor().await;

    let contract = test_contract(b"conformance_update_test");
    let key = contract.key();
    let initial_state = WrappedState::new(vec![1, 2, 3]);
    let updated_state = WrappedState::new(vec![4, 5, 6]);

    // MockRuntime uses blake3 hash comparison (larger hash wins), so we need
    // the updated state's hash to be larger to guarantee both runtimes return Updated.
    assert!(
        blake3::hash(updated_state.as_ref()).as_bytes()
            > blake3::hash(initial_state.as_ref()).as_bytes(),
        "Test data assumption violated: updated_state must have a larger blake3 hash \
         than initial_state for MockRuntime to return Updated. Change the test values."
    );

    // PUT initial state on both
    mock.upsert_contract_state(
        key,
        Either::Left(initial_state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    wasm.upsert_contract_state(
        key,
        Either::Left(initial_state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    // UPDATE with different state on both
    let mock_result = mock
        .upsert_contract_state(
            key,
            Either::Left(updated_state.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("MockRuntime UPDATE should not error");

    let wasm_result = wasm
        .upsert_contract_state(
            key,
            Either::Left(updated_state.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("MockWasmRuntime UPDATE should not error");

    // Both should return Updated since the state bytes differ.
    // MockRuntime uses hash-based merge (larger blake3 hash wins), and
    // MockWasmRuntime uses the production validate→update pipeline.
    // In both cases, a different state should be accepted.
    let mock_state = match mock_result {
        crate::contract::UpsertResult::Updated(s) => s,
        other => panic!("MockRuntime: expected Updated, got {other:?}"),
    };
    let wasm_state = match wasm_result {
        crate::contract::UpsertResult::Updated(s) => s,
        other => panic!("MockWasmRuntime: expected Updated, got {other:?}"),
    };

    // Both runtimes should have stored the updated state
    assert_eq!(
        mock_state.as_ref(),
        updated_state.as_ref(),
        "MockRuntime must store the updated state"
    );
    assert_eq!(
        wasm_state.as_ref(),
        updated_state.as_ref(),
        "MockWasmRuntime must store the updated state"
    );
}

// =========================================================================
// Drift detector: register_contract_notifier
//
// MockRuntime is a no-op while MockWasmRuntime tracks subscribers.
// This test documents the drift and will fail if MockRuntime is "fixed"
// to track subscribers (which would be the correct fix).
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn drift_detector_register_notifier_divergence() {
    let mut mock = create_mock_runtime_executor().await;
    let mut wasm = create_mock_wasm_executor().await;

    let contract = test_contract(b"conformance_notifier_test");
    let key = contract.key();
    let instance_id = *key.id();
    let state = WrappedState::new(vec![1]);

    // PUT on both
    mock.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();
    wasm.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .unwrap();

    // Register a notifier on both.
    // Note: _rx1/_rx2 are kept alive (not dropped) so the sender channels remain
    // valid. This test verifies registration tracking, not notification delivery.
    // Notification delivery is covered by the wasm_conformance_tests which test
    // the ContractRuntimeInterface directly.
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    let client_id = crate::client_events::ClientId::next();

    mock.register_contract_notifier(instance_id, client_id, tx1, None)
        .expect("MockRuntime register should succeed (no-op)");
    wasm.register_contract_notifier(instance_id, client_id, tx2, None)
        .expect("MockWasmRuntime register should succeed");

    // KNOWN DRIFT: MockRuntime returns empty, MockWasmRuntime returns real data.
    // When this assertion starts failing (both return non-empty), MockRuntime
    // has been fixed and this drift has been resolved. Update this test accordingly.
    let mock_subs = mock.get_subscription_info();
    let wasm_subs = wasm.get_subscription_info();

    assert!(
        mock_subs.is_empty(),
        "DRIFT DETECTOR: MockRuntime.get_subscription_info() is a no-op. \
         If this fails, MockRuntime now tracks subscribers — update this test!"
    );
    assert!(
        !wasm_subs.is_empty(),
        "MockWasmRuntime must track subscribers via bridged path. \
         If this fails, the production subscriber tracking path is broken!"
    );
}

// =========================================================================
// Invariant: Missing contract returns appropriate error on both
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn conformance_missing_contract_errors_on_both() {
    let mut mock = create_mock_runtime_executor().await;
    let mut wasm = create_mock_wasm_executor().await;

    // Create a key for a contract that was never PUT
    let contract = test_contract(b"conformance_missing_test");
    let key = contract.key();

    // UPDATE without prior PUT should fail on both
    let mock_result = mock
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![1])),
            RelatedContracts::default(),
            None,
        )
        .await;

    let wasm_result = wasm
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![1])),
            RelatedContracts::default(),
            None,
        )
        .await;

    assert!(
        mock_result.is_err(),
        "MockRuntime must error on UPDATE to non-existent contract"
    );
    assert!(
        wasm_result.is_err(),
        "MockWasmRuntime must error on UPDATE to non-existent contract"
    );
}

// =========================================================================
// Invariant: lookup_key returns None for unknown contracts on both
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn conformance_lookup_unknown_returns_none() {
    let mock = create_mock_runtime_executor().await;
    let wasm = create_mock_wasm_executor().await;

    let contract = test_contract(b"conformance_unknown_test");
    let instance_id = *contract.key().id();

    assert!(
        mock.lookup_key(&instance_id).is_none(),
        "MockRuntime must return None for unknown contract"
    );
    assert!(
        wasm.lookup_key(&instance_id).is_none(),
        "MockWasmRuntime must return None for unknown contract"
    );
}
