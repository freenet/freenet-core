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
    Executor::new_mock_wasm("related_test", storage, None, None, None)
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

// =========================================================================
// Test: UPDATE that triggers RequestRelated for a missing contract errors
// (no network in mock executor, MissingRelated returned synchronously).
//
// Regression for the bridged-path network-fetch fix: the bridged
// `fetch_related_for_validation` previously returned MissingRelated
// without ever consulting `op_manager`. The fix escalates to a network
// GET when `op_manager` is `Some`. The mock executor has
// `op_manager == None`, so the legacy MissingRelated outcome must be
// preserved here — production-side network behavior is exercised by
// the live-node E2E harness (freenet/mail) rather than this unit test.
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_update_missing_related_local_only_errors() {
    let mut executor = create_executor().await;

    // PUT the contract being updated, with no override at first so the PUT
    // succeeds without fetching related state.
    let target_contract = make_contract(b"update_missing_related_target");
    let target_key = target_contract.key();
    let initial_state = WrappedState::new(br#"{"v":0}"#.to_vec());
    executor
        .upsert_contract_state(
            target_key,
            Either::Left(initial_state),
            RelatedContracts::default(),
            Some(target_contract),
        )
        .await
        .expect("initial PUT");

    // Now configure the validator to demand a related contract that doesn't
    // exist anywhere — local lookup must miss, network path must be skipped
    // because mock executor has no `op_manager`, and `upsert` must surface
    // a MissingRelated error.
    let nonexistent_contract = make_contract(b"update_missing_related_target_missing");
    let nonexistent_id = *nonexistent_contract.key().id();
    executor.runtime.validate_overrides.insert(
        *target_key.id(),
        ValidateOverride::RequestRelated(vec![nonexistent_id]),
    );

    let delta = StateDelta::from(br#"{"v":1}"#.to_vec());
    let result = executor
        .upsert_contract_state(
            target_key,
            Either::Right(delta),
            RelatedContracts::default(),
            None,
        )
        .await;
    assert!(
        result.is_err(),
        "UPDATE that triggers RequestRelated for a missing contract \
         must error in local-only mode (mock executor has no op_manager); \
         got {result:?}"
    );
}

// =========================================================================
// Test: UPDATE that triggers RequestRelated for a contract not in
// state_store DOES escalate to network when the override stub is wired.
//
// This is the regression for the actual fix in PR #4006: previously the
// bridged path returned MissingRelated immediately on local miss with no
// network attempt. The fix calls into `fetch_related_via_network`, which
// in production uses `op_manager` + `start_sub_op_get` and in tests
// honors the `TEST_NETWORK_FETCH_OVERRIDE` thread-local stub. If the
// fix were reverted (back to local-only), this test fails because the
// stub never gets called.
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_update_missing_related_escalates_to_network_when_stubbed() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let mut executor = create_executor().await;

    let target_contract = make_contract(b"update_network_escalate_target");
    let target_key = target_contract.key();
    let initial_state = WrappedState::new(br#"{"v":0}"#.to_vec());
    executor
        .upsert_contract_state(
            target_key,
            Either::Left(initial_state),
            RelatedContracts::default(),
            Some(target_contract),
        )
        .await
        .expect("initial PUT");

    let related_contract = make_contract(b"update_network_escalate_related");
    let related_id = *related_contract.key().id();
    executor.runtime.validate_overrides.insert(
        *target_key.id(),
        ValidateOverride::RequestRelated(vec![related_id]),
    );

    // Pre-fix: bridged_upsert hit local state_store, missed, returned
    // MissingRelated synchronously and never asked the network. Post-fix:
    // local miss falls through to `fetch_related_via_network`, which the
    // stub services with a synthetic "fetched from network" state. The
    // call counter proves the network branch fired at least once for the
    // requested id; if the fix were reverted the counter stays at 0.
    let calls: Rc<RefCell<Vec<ContractInstanceId>>> = Rc::new(RefCell::new(Vec::new()));
    let calls_inner = calls.clone();
    crate::contract::executor::runtime::set_test_network_fetch_override(Some(Rc::new(move |id| {
        calls_inner.borrow_mut().push(id);
        Ok(freenet_stdlib::prelude::WrappedState::new(
            br#"{"network_fetched":true}"#.to_vec(),
        ))
    })));

    let delta = StateDelta::from(br#"{"v":1}"#.to_vec());
    let result = executor
        .upsert_contract_state(
            target_key,
            Either::Right(delta),
            RelatedContracts::default(),
            None,
        )
        .await;

    crate::contract::executor::runtime::set_test_network_fetch_override(None);

    assert!(
        result.is_ok(),
        "UPDATE must succeed once the network stub services the missing related contract: {result:?}"
    );
    let observed_calls = calls.borrow().clone();
    assert!(
        observed_calls.contains(&related_id),
        "fetch_related_via_network must have been invoked for related_id={related_id}; \
         observed calls: {observed_calls:?}"
    );
}

// =========================================================================
// Test: UPDATE that triggers RequestRelated returns MissingRelated when
// the network branch is reached but the fetch itself fails.
//
// Pairs with `test_update_missing_related_escalates_to_network_when_stubbed`
// (happy path) — this test covers the failure path where the stub
// returns `Err(...)`. Without this, `SubOpGetOutcome::NotFound` /
// `SubOpGetOutcome::Infra` mapping in `fetch_related_via_network` would
// have no automated coverage.
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_update_missing_related_network_fetch_fails() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let mut executor = create_executor().await;

    let target_contract = make_contract(b"update_network_fail_target");
    let target_key = target_contract.key();
    let initial_state = WrappedState::new(br#"{"v":0}"#.to_vec());
    executor
        .upsert_contract_state(
            target_key,
            Either::Left(initial_state),
            RelatedContracts::default(),
            Some(target_contract),
        )
        .await
        .expect("initial PUT");

    let related_contract = make_contract(b"update_network_fail_related");
    let related_id = *related_contract.key().id();
    executor.runtime.validate_overrides.insert(
        *target_key.id(),
        ValidateOverride::RequestRelated(vec![related_id]),
    );

    let calls: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
    let calls_inner = calls.clone();
    crate::contract::executor::runtime::set_test_network_fetch_override(Some(Rc::new(move |id| {
        *calls_inner.borrow_mut() += 1;
        Err(crate::contract::ExecutorError::request(
            freenet_stdlib::client_api::ContractError::MissingRelated { key: id },
        ))
    })));

    let delta = StateDelta::from(br#"{"v":1}"#.to_vec());
    let result = executor
        .upsert_contract_state(
            target_key,
            Either::Right(delta),
            RelatedContracts::default(),
            None,
        )
        .await;

    crate::contract::executor::runtime::set_test_network_fetch_override(None);

    assert_eq!(
        *calls.borrow(),
        1,
        "stub must be called exactly once (one related id requested)"
    );
    assert!(
        result.is_err(),
        "UPDATE must surface the network fetch failure as an error: {result:?}"
    );
}

// =========================================================================
// Test: UPDATE where the contract's update_state returns
// `requires(missing)` falls through to the network fetch + retry path
// (the production cross-node UPDATE flow that #80 surfaced — inbox's
// `update_state` returns requires(AFT-record) on the receiver because
// the receiver hasn't seen the sender's AFT yet).
//
// Regression for the bridged_upsert_contract_state branch that used to
// MissingRelated immediately on `Either::Right`. With the fix the
// missing related contracts are fetched via the same local-then-network
// path used by validate-side RequestRelated, then update_state is
// re-invoked with `RelatedState` entries surfaced; the second call
// completes the merge and the upsert succeeds.
// =========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_update_state_requires_related_fetches_and_retries() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let mut executor = create_executor().await;

    let target_contract = make_contract(b"update_requires_target");
    let target_key = target_contract.key();
    let initial_state = WrappedState::new(br#"{"v":0}"#.to_vec());
    executor
        .upsert_contract_state(
            target_key,
            Either::Left(initial_state),
            RelatedContracts::default(),
            Some(target_contract),
        )
        .await
        .expect("initial PUT");

    let related_contract = make_contract(b"update_requires_related");
    let related_id = *related_contract.key().id();
    executor.runtime.update_overrides.insert(
        *target_key.id(),
        crate::contract::executor::mock_wasm_runtime::UpdateOverride::RequiresRelated(vec![
            related_id,
        ]),
    );

    let calls: Rc<RefCell<Vec<ContractInstanceId>>> = Rc::new(RefCell::new(Vec::new()));
    let calls_inner = calls.clone();
    crate::contract::executor::runtime::set_test_network_fetch_override(Some(Rc::new(move |id| {
        calls_inner.borrow_mut().push(id);
        Ok(freenet_stdlib::prelude::WrappedState::new(
            br#"{"network_supplied_related":true}"#.to_vec(),
        ))
    })));

    let delta = StateDelta::from(br#"{"v":1}"#.to_vec());
    let result = executor
        .upsert_contract_state(
            target_key,
            Either::Right(delta),
            RelatedContracts::default(),
            None,
        )
        .await;

    crate::contract::executor::runtime::set_test_network_fetch_override(None);

    assert!(
        result.is_ok(),
        "UPDATE must succeed once network supplies the related state: {result:?}"
    );
    let observed_calls = calls.borrow().clone();
    assert!(
        observed_calls.contains(&related_id),
        "fetch_related_via_network must have been invoked for the requires(missing) path; \
         observed calls: {observed_calls:?}"
    );
}
