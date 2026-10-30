//! Regression tests for #6050: an update whose merge leaves the stored state
//! unchanged must not run `validate_state` on that unchanged state.
//!
//! The stored state was validated when it was stored, so validating identical
//! bytes again adds nothing, and for a contract whose `validate_state` checks
//! signatures over a large state it was most of the cost of every duplicate or
//! replayed update. Every state that DOES change must still be validated, and an
//! invalid one must still be rejected.

use either::Either;
use freenet_stdlib::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::contract::executor::mock_wasm_runtime::{
    MockWasmRuntime, UpdateOverride, ValidateOverride,
};
use crate::contract::executor::{ContractExecutor, Executor, UpsertOutcome, UpsertResult};
use crate::wasm_runtime::MockStateStorage;

const STORED: [u8; 3] = [1, 2, 3];

/// An executor holding a contract whose stored state is [`STORED`].
async fn executor_with_stored_state(
    name: &str,
) -> (Executor<MockWasmRuntime, MockStateStorage>, ContractKey) {
    let mut executor = Executor::new_mock_wasm(name, MockStateStorage::new(), None, None)
        .await
        .expect("create MockWasmRuntime executor");
    let code = ContractCode::from(name.as_bytes().to_vec());
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(code),
        Parameters::from(vec![]),
    )));
    let key = contract.key();
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(STORED.to_vec())),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("initial PUT should succeed");
    (executor, key)
}

/// Record every state `validate_state` is called on from here on.
fn record_validations(
    executor: &mut Executor<MockWasmRuntime, MockStateStorage>,
    key: &ContractKey,
) -> Arc<Mutex<Vec<Vec<u8>>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::Record(seen.clone()));
    seen
}

fn keep_current(current: &[u8], _incoming: &[u8]) -> Vec<u8> {
    current.to_vec()
}

fn take_incoming(_current: &[u8], incoming: &[u8]) -> Vec<u8> {
    incoming.to_vec()
}

fn union(current: &[u8], incoming: &[u8]) -> Vec<u8> {
    let mut out = [current, incoming].concat();
    out.sort_unstable();
    out.dedup();
    out
}

/// A full state the merge drops leaves the stored state unchanged. Only the
/// incoming full state is validated (on entry, as before); the stored state is not
/// validated again.
#[tokio::test(flavor = "current_thread")]
async fn a_dropped_full_state_does_not_validate_the_unchanged_state() {
    let (mut executor, key) = executor_with_stored_state("dropped_full_state").await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(keep_current));
    let seen = record_validations(&mut executor, &key);

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![9])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("a dropped update is not an error");
    assert!(matches!(result, UpsertResult::NoChange), "{result:?}");
    assert_eq!(*seen.lock().unwrap(), vec![vec![9]]);
}

/// A delta the merge ignores leaves the stored state unchanged and validates
/// nothing.
#[tokio::test(flavor = "current_thread")]
async fn an_ignored_delta_validates_nothing() {
    let (mut executor, key) = executor_with_stored_state("ignored_delta").await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(keep_current));
    let seen = record_validations(&mut executor, &key);

    let result = executor
        .upsert_contract_state(
            key,
            Either::Right(StateDelta::from(vec![7])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("an ignored delta is not an error");
    assert!(matches!(result, UpsertResult::NoChange), "{result:?}");
    assert!(
        seen.lock().unwrap().is_empty(),
        "{:?}",
        seen.lock().unwrap()
    );
}

/// The same holds through the deferrable entry the UPDATE driver uses: a no-op
/// completes as `NoChange` without validating.
#[tokio::test(flavor = "current_thread")]
async fn a_no_op_through_the_deferrable_entry_validates_nothing() {
    let (mut executor, key) = executor_with_stored_state("deferrable_no_op").await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(keep_current));
    let seen = record_validations(&mut executor, &key);

    let outcome = executor
        .upsert_contract_state_deferrable(
            key,
            Either::Right(StateDelta::from(vec![7])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("an ignored delta is not an error");
    assert!(
        matches!(outcome, UpsertOutcome::Completed(UpsertResult::NoChange)),
        "{outcome:?}"
    );
    assert!(
        seen.lock().unwrap().is_empty(),
        "{:?}",
        seen.lock().unwrap()
    );
}

/// A no-op whose merge first needed a related contract fetched (the
/// `requires()` retry) completes without validating.
#[tokio::test(flavor = "current_thread")]
async fn a_no_op_after_the_related_state_retry_validates_nothing() {
    let (mut executor, key) = executor_with_stored_state("related_retry_no_op").await;
    let related_id = *ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(b"related_retry_related".to_vec())),
        Parameters::from(vec![]),
    )))
    .key()
    .id();
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::RequiresRelated(vec![related_id]));
    let seen = record_validations(&mut executor, &key);
    let fetches = Rc::new(RefCell::new(Vec::new()));
    let fetches_inner = fetches.clone();
    crate::contract::executor::runtime::set_test_network_fetch_override(Some(Rc::new(move |id| {
        fetches_inner.borrow_mut().push(id);
        Ok(WrappedState::new(vec![42]))
    })));

    // On the retry the mock takes the delta's bytes as the new state, and they
    // equal the stored state, so the merge changes nothing.
    let result = executor
        .upsert_contract_state(
            key,
            Either::Right(StateDelta::from(STORED.to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await;
    crate::contract::executor::runtime::set_test_network_fetch_override(None);

    let result = result.expect("the retried no-op is not an error");
    assert!(matches!(result, UpsertResult::NoChange), "{result:?}");
    assert_eq!(
        *fetches.borrow(),
        vec![related_id],
        "the retry path must have run, or this test checks nothing"
    );
    assert!(
        seen.lock().unwrap().is_empty(),
        "{:?}",
        seen.lock().unwrap()
    );
}

/// A no-op update takes no related-contract fetch for validation: a contract
/// whose `validate_state` would ask for a related contract nobody can supply is
/// not asked.
#[tokio::test(flavor = "current_thread")]
async fn a_no_op_takes_no_related_fetch_for_validation() {
    let (mut executor, key) = executor_with_stored_state("no_op_no_fetch").await;
    let missing = ContractInstanceId::new([7; 32]);
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(keep_current));
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::RequestRelated(vec![missing]));
    let fetches = Rc::new(RefCell::new(Vec::new()));
    let fetches_inner = fetches.clone();
    crate::contract::executor::runtime::set_test_network_fetch_override(Some(Rc::new(move |id| {
        fetches_inner.borrow_mut().push(id);
        Err(crate::contract::ExecutorError::request(
            freenet_stdlib::client_api::ContractError::MissingRelated { key: id },
        ))
    })));

    let result = executor
        .upsert_contract_state(
            key,
            Either::Right(StateDelta::from(vec![7])),
            RelatedContracts::default(),
            None,
        )
        .await;
    crate::contract::executor::runtime::set_test_network_fetch_override(None);

    let result = result.expect("a no-op must not need the related contract");
    assert!(matches!(result, UpsertResult::NoChange), "{result:?}");
    assert!(fetches.borrow().is_empty(), "{:?}", fetches.borrow());
}

/// The behaviour change, pinned: a stored state that has become invalid since it
/// was stored no longer makes a no-op update fail. It is surfaced by the next
/// update that changes it (see `an_invalid_changed_state_is_still_rejected`).
#[tokio::test(flavor = "current_thread")]
async fn a_no_op_on_a_stored_state_that_became_invalid_is_no_change() {
    let (mut executor, key) = executor_with_stored_state("stale_invalid_stored").await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(keep_current));
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::ValidWhen(|s| s != STORED));

    let result = executor
        .upsert_contract_state(
            key,
            Either::Right(StateDelta::from(vec![7])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("a no-op on an unchanged stored state returns NoChange");
    assert!(matches!(result, UpsertResult::NoChange), "{result:?}");
}

/// A merge that changes bytes but not length is still validated and committed:
/// the unchanged check compares bytes, not sizes.
#[tokio::test(flavor = "current_thread")]
async fn a_same_length_change_is_validated_and_committed() {
    let (mut executor, key) = executor_with_stored_state("same_length_change").await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(take_incoming));
    let seen = record_validations(&mut executor, &key);

    let changed = vec![1, 2, 4];
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(changed.clone())),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("a valid change is accepted");
    assert!(
        matches!(&result, UpsertResult::Updated(s) if s.as_ref() == changed.as_slice()),
        "{result:?}"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![changed.clone(), changed],
        "the incoming state and the merged state must both be validated"
    );
}

/// A merge that changes the state still validates the merged state, and the
/// change is committed.
#[tokio::test(flavor = "current_thread")]
async fn a_changed_state_is_still_validated() {
    let (mut executor, key) = executor_with_stored_state("changed_update").await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(union));
    let seen = record_validations(&mut executor, &key);

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![9])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("a valid change is accepted");
    let merged = vec![1, 2, 3, 9];
    assert!(
        matches!(&result, UpsertResult::Updated(s) if s.as_ref() == merged.as_slice()),
        "{result:?}"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![vec![9], merged],
        "the incoming state and the merged state must both be validated"
    );
}

/// A merge that produces an invalid state is still rejected.
#[tokio::test(flavor = "current_thread")]
async fn an_invalid_changed_state_is_still_rejected() {
    let (mut executor, key) = executor_with_stored_state("invalid_update").await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(union));
    // Valid below four bytes: the stored and incoming states pass, their merge
    // does not.
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::ValidWhen(|s| s.len() < 4));

    let err = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![9])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("an invalid merged state must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("invalid"),
        "unexpected error: {err}"
    );
}
