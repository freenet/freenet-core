//! Tests for the related contracts mechanism (depth=1).
//!
//! These tests exercise the `fetch_related_for_validation` helper and the
//! code paths that use it: PUT with new contract, PUT with existing contract
//! (merge), and validation after update.
//!
//! The `ValidateOverride` mechanism in `MockWasmRuntime` allows configuring
//! per-contract validation behavior without WASM.

use either::Either;
use freenet_stdlib::prelude::*;
use std::sync::Arc;

use crate::contract::executor::mock_wasm_runtime::{MockWasmRuntime, ValidateOverride};
use crate::contract::executor::{ContractExecutor, Executor, UpsertResult};
use crate::wasm_runtime::MockStateStorage;

/// Create a MockWasmRuntime executor.
async fn create_executor() -> Executor<MockWasmRuntime, MockStateStorage> {
    let storage = MockStateStorage::new();
    Executor::new_mock_wasm("related_test", storage, None, None)
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
// Test: PUT a "gated" contract succeeds when gate exists locally
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_put_with_related_succeeds() {
    let mut executor = create_executor().await;

    // First, PUT the "gate" contract (no overrides, validates as Valid)
    let gate_contract = make_contract(b"gate_code");
    let gate_key = gate_contract.key();
    let gate_state = WrappedState::new(br#"{"open": true}"#.to_vec());

    let result = executor
        .upsert_contract_state(
            gate_key,
            Either::Left(gate_state.clone()),
            RelatedContracts::default(),
            Some(gate_contract),
        )
        .await;
    assert!(result.is_ok(), "gate contract PUT should succeed");

    // Now PUT the "gated" contract that requests the gate's state
    let gated_contract = make_contract(b"gated_code");
    let gated_key = gated_contract.key();
    let gated_state = WrappedState::new(br#"{"count": 0}"#.to_vec());

    // Configure: gated contract requests gate's state on first validation
    executor.runtime.validate_overrides.insert(
        *gated_key.id(),
        ValidateOverride::RequestRelated(vec![*gate_key.id()]),
    );

    let result = executor
        .upsert_contract_state(
            gated_key,
            Either::Left(gated_state),
            RelatedContracts::default(),
            Some(gated_contract),
        )
        .await;
    assert!(
        result.is_ok(),
        "gated contract PUT should succeed: {result:?}"
    );
    assert!(
        matches!(result.unwrap(), UpsertResult::Updated(_)),
        "expected Updated result"
    );
}

// =========================================================================
// Test: PUT gated when gate state says closed → Invalid
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_put_with_gate_closed() {
    let mut executor = create_executor().await;

    // PUT the gate with "closed" state
    let gate_contract = make_contract(b"gate_code_closed");
    let gate_key = gate_contract.key();
    let gate_state = WrappedState::new(br#"{"open": false}"#.to_vec());

    executor
        .upsert_contract_state(
            gate_key,
            Either::Left(gate_state),
            RelatedContracts::default(),
            Some(gate_contract),
        )
        .await
        .expect("gate PUT");

    // PUT gated contract configured to always return Invalid.
    // The mock doesn't inspect state content, so we use ValidateOverride::Invalid
    // directly to simulate a gate that rejects validation.
    let gated_contract = make_contract(b"gated_code_closed");
    let gated_key = gated_contract.key();
    let gated_state = WrappedState::new(br#"{"count": 0}"#.to_vec());

    executor
        .runtime
        .validate_overrides
        .insert(*gated_key.id(), ValidateOverride::Invalid);

    let result = executor
        .upsert_contract_state(
            gated_key,
            Either::Left(gated_state),
            RelatedContracts::default(),
            Some(gated_contract),
        )
        .await;
    assert!(
        result.is_err(),
        "gated contract PUT should fail when invalid"
    );
}

// =========================================================================
// Test: Self-reference rejected
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_put_self_reference_rejected() {
    let mut executor = create_executor().await;

    let contract = make_contract(b"self_ref_code");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3]);

    // Configure: contract requests its own ID
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::RequestRelated(vec![*key.id()]));

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await;
    assert!(result.is_err(), "self-reference should be rejected");
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("cannot request itself"),
        "error should mention self-reference: {err_msg}"
    );
}

// =========================================================================
// Test: Too many related contracts rejected
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_put_too_many_related_rejected() {
    let mut executor = create_executor().await;

    let contract = make_contract(b"too_many_code");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3]);

    // Create 11 distinct contract IDs by making contracts with different code bytes
    let too_many_ids: Vec<ContractInstanceId> = (0..11u8)
        .map(|i| {
            let contract = make_contract(&[i, 100, 200]);
            *contract.key().id()
        })
        .collect();

    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::RequestRelated(too_many_ids));

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await;
    assert!(result.is_err(), "too many related should be rejected");
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("limit is 10"),
        "error should mention limit: {err_msg}"
    );
}

// =========================================================================
// Test: Missing related contract errors (not hang)
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_put_missing_related_errors() {
    let mut executor = create_executor().await;

    let contract = make_contract(b"missing_related_code");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3]);

    // Request a contract that doesn't exist (and can't be fetched from network in local mode)
    let nonexistent_contract = make_contract(b"nonexistent_contract_code_xyz");
    let nonexistent_id = *nonexistent_contract.key().id();
    executor.runtime.validate_overrides.insert(
        *key.id(),
        ValidateOverride::RequestRelated(vec![nonexistent_id]),
    );

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await;
    // In local mode without network, fetching a missing contract should error
    assert!(
        result.is_err(),
        "missing related contract should error, not hang"
    );
}

// =========================================================================
// Test: Depth exceeded (related contract also returns RequestRelated)
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_depth_exceeded_rejected() {
    let mut executor = create_executor().await;

    // Put the "related" contract first
    let related_contract = make_contract(b"related_depth_code");
    let related_key = related_contract.key();
    let related_state = WrappedState::new(vec![10, 20]);

    executor
        .upsert_contract_state(
            related_key,
            Either::Left(related_state),
            RelatedContracts::default(),
            Some(related_contract),
        )
        .await
        .expect("related contract PUT");

    // Now PUT the main contract that AlwaysRequestRelated (even after getting related)
    let contract = make_contract(b"depth_test_code");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3]);

    executor.runtime.validate_overrides.insert(
        *key.id(),
        ValidateOverride::AlwaysRequestRelated(vec![*related_key.id()]),
    );

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await;
    assert!(result.is_err(), "depth exceeded should be rejected");
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("depth=1 limit exceeded"),
        "error should mention depth limit: {err_msg}"
    );
}

// =========================================================================
// Test: Empty RequestRelated treated as error
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_empty_request_related() {
    let mut executor = create_executor().await;

    let contract = make_contract(b"empty_related_code");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3]);

    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::EmptyRequestRelated);

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await;
    assert!(result.is_err(), "empty RequestRelated should be rejected");
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("empty list"),
        "error should mention empty list: {err_msg}"
    );
}

// =========================================================================
// Test: Duplicate IDs are deduped (only fetch once)
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_duplicate_ids_deduped() {
    let mut executor = create_executor().await;

    // PUT the related contract
    let related_contract = make_contract(b"dedup_related_code");
    let related_key = related_contract.key();
    let related_state = WrappedState::new(vec![10, 20]);

    executor
        .upsert_contract_state(
            related_key,
            Either::Left(related_state),
            RelatedContracts::default(),
            Some(related_contract),
        )
        .await
        .expect("related contract PUT");

    // PUT main contract that requests the same ID twice
    let contract = make_contract(b"dedup_test_code");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3]);

    executor.runtime.validate_overrides.insert(
        *key.id(),
        ValidateOverride::RequestRelated(vec![*related_key.id(), *related_key.id()]),
    );

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await;
    assert!(
        result.is_ok(),
        "duplicate IDs should be deduped and succeed: {result:?}"
    );
}

// =========================================================================
// Test: Existing contract PUT (merge path) with related contracts
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_existing_contract_put_with_related() {
    let mut executor = create_executor().await;

    // PUT the gate contract
    let gate_contract = make_contract(b"gate_merge_code");
    let gate_key = gate_contract.key();
    let gate_state = WrappedState::new(br#"{"open": true}"#.to_vec());

    executor
        .upsert_contract_state(
            gate_key,
            Either::Left(gate_state),
            RelatedContracts::default(),
            Some(gate_contract),
        )
        .await
        .expect("gate PUT");

    // PUT the gated contract initially (no overrides → Valid)
    let gated_contract = make_contract(b"gated_merge_code");
    let gated_key = gated_contract.key();
    let initial_state = WrappedState::new(br#"{"count": 0}"#.to_vec());

    executor
        .upsert_contract_state(
            gated_key,
            Either::Left(initial_state),
            RelatedContracts::default(),
            Some(gated_contract.clone()),
        )
        .await
        .expect("gated initial PUT");

    // Now configure the gated contract to request related on validation
    // This affects the post-merge validate_state call
    executor.runtime.validate_overrides.insert(
        *gated_key.id(),
        ValidateOverride::RequestRelated(vec![*gate_key.id()]),
    );

    // PUT again (merge path) with updated state
    let updated_state = WrappedState::new(br#"{"count": 1}"#.to_vec());
    let result = executor
        .upsert_contract_state(
            gated_key,
            Either::Left(updated_state),
            RelatedContracts::default(),
            Some(gated_contract),
        )
        .await;
    assert!(
        result.is_ok(),
        "existing contract PUT with related should succeed: {result:?}"
    );
}

// =========================================================================
// Test: Update path with related contracts
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_update_with_related_succeeds() {
    let mut executor = create_executor().await;

    // PUT the gate
    let gate_contract = make_contract(b"gate_update_code");
    let gate_key = gate_contract.key();
    let gate_state = WrappedState::new(br#"{"open": true}"#.to_vec());

    executor
        .upsert_contract_state(
            gate_key,
            Either::Left(gate_state),
            RelatedContracts::default(),
            Some(gate_contract),
        )
        .await
        .expect("gate PUT");

    // PUT the gated contract initially
    let gated_contract = make_contract(b"gated_update_code");
    let gated_key = gated_contract.key();
    let initial_state = WrappedState::new(br#"{"count": 0}"#.to_vec());

    executor
        .upsert_contract_state(
            gated_key,
            Either::Left(initial_state),
            RelatedContracts::default(),
            Some(gated_contract),
        )
        .await
        .expect("gated initial PUT");

    // Now configure validation override for post-update validation
    executor.runtime.validate_overrides.insert(
        *gated_key.id(),
        ValidateOverride::RequestRelated(vec![*gate_key.id()]),
    );

    // Send a delta update
    let delta = StateDelta::from(br#"{"count": 1}"#.to_vec());
    let result = executor
        .upsert_contract_state(
            gated_key,
            Either::Right(delta),
            RelatedContracts::default(),
            None,
        )
        .await;
    assert!(
        result.is_ok(),
        "update with related validation should succeed: {result:?}"
    );
}

// =========================================================================
// Test: Exactly 10 related contracts (boundary) succeeds
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_exactly_at_limit_succeeds() {
    let mut executor = create_executor().await;

    // Create and PUT exactly 10 related contracts
    let mut related_ids = Vec::new();
    for i in 0..10u8 {
        let contract = make_contract(&[i, 200, 200]);
        let key = contract.key();
        related_ids.push(*key.id());
        executor
            .upsert_contract_state(
                key,
                Either::Left(WrappedState::new(vec![i])),
                RelatedContracts::default(),
                Some(contract),
            )
            .await
            .expect("related contract PUT");
    }

    // PUT a contract that requests all 10
    let contract = make_contract(b"boundary_code");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3]);

    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::RequestRelated(related_ids));

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await;
    assert!(
        result.is_ok(),
        "exactly 10 related contracts should succeed: {result:?}"
    );
}

// =========================================================================
// Test: Multiple distinct related contracts in single request
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_related_contracts() {
    let mut executor = create_executor().await;

    // PUT 3 distinct related contracts
    let mut related_ids = Vec::new();
    for i in 0..3u8 {
        let contract = make_contract(&[i, 150, 150]);
        let key = contract.key();
        related_ids.push(*key.id());
        executor
            .upsert_contract_state(
                key,
                Either::Left(WrappedState::new(vec![10 + i])),
                RelatedContracts::default(),
                Some(contract),
            )
            .await
            .expect("related contract PUT");
    }

    // PUT a contract that requests all 3
    let contract = make_contract(b"multi_related_code");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3]);

    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::RequestRelated(related_ids));

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await;
    assert!(
        result.is_ok(),
        "multiple related contracts should succeed: {result:?}"
    );
}
