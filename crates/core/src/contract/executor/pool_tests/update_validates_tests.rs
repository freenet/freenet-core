//! A contract that exports `__frnt__update_validates` validates its own input
//! and output in `update_state`, so for a state this node already stores (and
//! has verified once) the node skips the validation it would otherwise repeat
//! around `update_state`. A fresh store, a stored state that fails the one-time
//! check, and contracts without the export all keep the full path.

use either::Either;
use freenet_stdlib::prelude::*;
use std::sync::{Arc, Mutex};

use crate::contract::executor::mock_wasm_runtime::{
    MockWasmRuntime, UpdateOverride, ValidateOverride,
};
use crate::contract::executor::{ContractExecutor, Executor, UpsertResult};
use crate::wasm_runtime::MockStateStorage;

const STORED: [u8; 3] = [1, 2, 3];

type Exec = Executor<MockWasmRuntime, MockStateStorage>;

fn container(name: &str) -> ContractContainer {
    ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(name.as_bytes().to_vec())),
        Parameters::from(vec![]),
    )))
}

async fn new_executor(name: &str) -> Exec {
    Executor::new_mock_wasm(name, MockStateStorage::new(), None, None)
        .await
        .expect("create MockWasmRuntime executor")
}

/// An executor holding `name`'s contract with [`STORED`] stored. When `opt_in`
/// the contract declares `__frnt__update_validates` before the first PUT.
async fn executor_with_stored_state(name: &str, opt_in: bool) -> (Exec, ContractKey) {
    let mut executor = new_executor(name).await;
    let contract = container(name);
    let key = contract.key();
    if opt_in {
        executor.runtime.update_validates_ids.insert(*key.id());
    }
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
fn record_validations(executor: &mut Exec, key: &ContractKey) -> Arc<Mutex<Vec<Vec<u8>>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::Record(seen.clone()));
    seen
}

fn seen(log: &Arc<Mutex<Vec<Vec<u8>>>>) -> Vec<Vec<u8>> {
    log.lock().unwrap().clone()
}

fn union(current: &[u8], incoming: &[u8]) -> Vec<u8> {
    let mut out = [current, incoming].concat();
    out.sort_unstable();
    out.dedup();
    out
}

async fn delta(executor: &mut Exec, key: ContractKey, bytes: Vec<u8>) -> UpsertResult {
    executor
        .upsert_contract_state(
            key,
            Either::Right(StateDelta::from(bytes)),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("delta accepted")
}

async fn full(executor: &mut Exec, key: ContractKey, bytes: Vec<u8>) -> UpsertResult {
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(bytes)),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("state accepted")
}

async fn stored(executor: &Exec, key: &ContractKey) -> Vec<u8> {
    executor
        .state_store
        .get(key)
        .await
        .expect("stored state")
        .as_ref()
        .to_vec()
}

/// (1) Delta, State and NoChange on a hosted key validate nothing beyond the
/// one-time check of the stored state, which happens once.
#[tokio::test(flavor = "current_thread")]
async fn an_opting_in_contract_skips_validation_on_delta_state_and_no_change() {
    let (mut executor, key) = executor_with_stored_state("uv_opt_in_paths", true).await;
    let log = record_validations(&mut executor, &key);

    // Delta: the post-update validate is skipped. The first update pays the
    // one-time check of the stored state.
    let r = delta(&mut executor, key, vec![7]).await;
    assert!(
        matches!(&r, UpsertResult::Updated(s) if s.as_ref() == [7]),
        "{r:?}"
    );
    assert_eq!(seen(&log), vec![STORED.to_vec()], "one-time check only");

    // State: neither the incoming state nor the result is validated.
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(union));
    let r = full(&mut executor, key, vec![9]).await;
    assert!(
        matches!(&r, UpsertResult::Updated(s) if s.as_ref() == [7, 9]),
        "{r:?}"
    );

    // Incoming bytes equal to the stored bytes: NoChange, nothing validated.
    let r = full(&mut executor, key, vec![7, 9]).await;
    assert!(matches!(r, UpsertResult::NoChange), "{r:?}");

    assert_eq!(
        seen(&log),
        vec![STORED.to_vec()],
        "the stored state is verified once; nothing else is validated"
    );
}

/// The one-time check is per (key, stored-state hash): a stored state that was
/// replaced behind the opt-in's back is verified again.
#[tokio::test(flavor = "current_thread")]
async fn a_changed_stored_state_is_verified_again() {
    let (mut executor, key) = executor_with_stored_state("uv_reverify", true).await;
    let log = record_validations(&mut executor, &key);

    delta(&mut executor, key, vec![7]).await;
    delta(&mut executor, key, vec![8]).await;
    assert_eq!(
        seen(&log),
        vec![STORED.to_vec()],
        "verified once, not per update"
    );

    executor
        .state_store
        .update(&key, WrappedState::new(vec![4, 4]))
        .await
        .expect("replace stored state");
    delta(&mut executor, key, vec![5]).await;
    assert_eq!(
        seen(&log),
        vec![STORED.to_vec(), vec![4, 4]],
        "the replaced stored state must be verified before it is trusted"
    );
}

/// The record is shared through `set_stored_state_verified`, so a second
/// executor of a pool does not repeat the check.
#[tokio::test(flavor = "current_thread")]
async fn the_verified_record_can_be_shared_between_executors() {
    let (mut a, key) = executor_with_stored_state("uv_shared", true).await;
    let log = record_validations(&mut a, &key);
    delta(&mut a, key, vec![7]).await;
    assert_eq!(seen(&log), vec![STORED.to_vec()]);
    assert_eq!(a.stored_state_verified.lock().unwrap().len(), 1);

    let mut b = new_executor("uv_shared_b").await;
    b.set_stored_state_verified(a.stored_state_verified.clone());
    assert_eq!(b.stored_state_verified.lock().unwrap().len(), 1);
}

/// (2) An `update_state` error on a full state returns the error: the stored
/// state is not validated and not replaced.
#[tokio::test(flavor = "current_thread")]
async fn an_update_error_on_a_full_state_neither_validates_nor_replaces() {
    let (mut executor, key) = executor_with_stored_state("uv_update_err", true).await;
    let log = record_validations(&mut executor, &key);
    // Prime the one-time check with an identical-bytes push.
    let r = full(&mut executor, key, STORED.to_vec()).await;
    assert!(matches!(r, UpsertResult::NoChange), "{r:?}");
    assert_eq!(seen(&log), vec![STORED.to_vec()]);
    log.lock().unwrap().clear();

    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::TryMergeWith(|_, _| None));
    let err = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![9, 9])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("the contract refused the state");
    assert!(!err.to_string().is_empty());
    assert!(
        seen(&log).is_empty(),
        "no validation at all: {:?}",
        seen(&log)
    );
    assert_eq!(stored(&executor, &key).await, STORED.to_vec());
}

/// (3) A fresh store is validated in full, opted in or not.
#[tokio::test(flavor = "current_thread")]
async fn a_fresh_store_is_still_validated_and_an_invalid_state_rejected() {
    // Valid state: validated before it is stored.
    let mut executor = new_executor("uv_fresh_ok").await;
    let contract = container("uv_fresh_ok");
    let key = contract.key();
    executor.runtime.update_validates_ids.insert(*key.id());
    let log = record_validations(&mut executor, &key);
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![1])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("valid fresh PUT");
    assert_eq!(seen(&log), vec![vec![1]]);

    // Invalid state: rejected.
    let mut executor = new_executor("uv_fresh_bad").await;
    let contract = container("uv_fresh_bad");
    let key = contract.key();
    executor.runtime.update_validates_ids.insert(*key.id());
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::Invalid);
    let err = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![1])),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect_err("an invalid fresh state must be rejected");
    assert!(err.to_string().to_lowercase().contains("invalid"), "{err}");
}

/// (4) A contract without the export keeps today's exact call sequence.
#[tokio::test(flavor = "current_thread")]
async fn a_non_opting_contract_keeps_its_call_sequence() {
    let (mut executor, key) = executor_with_stored_state("uv_non_opt", false).await;
    let log = record_validations(&mut executor, &key);

    delta(&mut executor, key, vec![7]).await;
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(union));
    full(&mut executor, key, vec![9]).await;
    let r = full(&mut executor, key, vec![7, 9]).await;
    assert!(matches!(r, UpsertResult::NoChange), "{r:?}");

    assert_eq!(
        seen(&log),
        vec![vec![7], vec![9], vec![7, 9], vec![7, 9]],
        "delta: result; state: incoming then result; identical: incoming"
    );
}

/// A non-opting contract keeps the recovery validation on an update error.
#[tokio::test(flavor = "current_thread")]
async fn a_non_opting_contract_still_validates_the_stored_state_on_update_error() {
    let (mut executor, key) = executor_with_stored_state("uv_non_opt_err", false).await;
    let log = record_validations(&mut executor, &key);
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::TryMergeWith(|_, _| None));
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![9, 9])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("the contract refused the state");
    assert_eq!(seen(&log), vec![vec![9, 9], STORED.to_vec()]);
}

/// (5) A stored state that parses but is invalid: the one-time check catches it
/// and the full path rejects the result instead of committing it.
#[tokio::test(flavor = "current_thread")]
async fn a_corrupted_but_parsing_stored_state_falls_back_to_the_full_path() {
    let (mut executor, key) = executor_with_stored_state("uv_corrupt", true).await;
    // Both the stored state and the state the update produces are invalid.
    executor.runtime.validate_overrides.insert(
        *key.id(),
        ValidateOverride::ValidWhen(|s| s != [1, 2, 3] && s != [9, 9]),
    );

    let err = executor
        .upsert_contract_state(
            key,
            Either::Right(StateDelta::from(vec![9, 9])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("the full path must reject the invalid result");
    assert!(err.to_string().to_lowercase().contains("invalid"), "{err}");
    assert_eq!(stored(&executor, &key).await, STORED.to_vec());
    assert_eq!(
        executor.stored_state_verified.lock().unwrap().len(),
        0,
        "an unverified stored state is not recorded"
    );
}

/// (6) A stored state that does not parse: the update fails, the one-time check
/// fails, and the corrupted-state recovery replaces it with the validated
/// incoming state instead of rejecting for good.
#[tokio::test(flavor = "current_thread")]
async fn a_stored_state_that_does_not_parse_is_recovered() {
    let (mut executor, key) = executor_with_stored_state("uv_unparsable", true).await;
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::ValidWhen(|s| s == [5, 5]));
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::TryMergeWith(|_, _| None));

    let r = full(&mut executor, key, vec![5, 5]).await;
    assert!(
        matches!(&r, UpsertResult::Updated(s) if s.as_ref() == [5, 5]),
        "{r:?}"
    );
    assert_eq!(stored(&executor, &key).await, vec![5, 5]);
}

/// Source pins: each skip is gated on `trusted_stored`, which is computed only
/// for a stored key, and the merge-error path has no validation of its own.
#[test]
fn the_skips_are_wired_to_the_trust_check() {
    let src = include_str!("../runtime/executor_impl.rs");
    let body = src
        .split("pub(in crate::contract::executor) async fn bridged_upsert_contract_state_inner(")
        .nth(1)
        .expect("upsert body")
        .split("\n    pub(in crate::contract::executor) fn bridged_register_contract_notifier(")
        .next()
        .expect("upsert body end");

    assert!(
        body.contains("let trusted_stored = !is_new_contract"),
        "trust must require a stored state"
    );
    assert!(body.contains("stored_state_trusted_for_opt_in("));
    assert!(
        body.contains("if trusted_stored {"),
        "incoming pre-validate gate"
    );
    assert!(
        body.contains("trusted_stored && !recovery_performed"),
        "post-update validate gate"
    );
    assert!(
        body.contains("let local_valid = trusted_stored"),
        "merge-error path must not validate a trusted stored state"
    );
    assert!(
        body.contains(".invalidate(&key)"),
        "a stored state replaced outside the trusted update must be forgotten"
    );
    // The helper verifies through the contract's own validate_state and trusts
    // only `Valid`.
    let helper = src
        .split("async fn stored_state_trusted_for_opt_in(")
        .nth(1)
        .expect("helper")
        .split("\n    }\n")
        .next()
        .expect("helper end");
    assert!(helper.contains("self.runtime.update_validates("));
    assert!(helper.contains("self.runtime.validate_state("));
    assert!(helper.contains("matches!(verdict, Ok(ValidateResult::Valid))"));
}
