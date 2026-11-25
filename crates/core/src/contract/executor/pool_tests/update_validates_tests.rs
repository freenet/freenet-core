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
use crate::contract::executor::{
    ContractExecutor, Executor, SharedStoredStateVerified, StoredStateVerified, UpsertResult,
};
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
    // The merge is replaced by one that would change the state, so NoChange can
    // only come from the equal-bytes shortcut, which never calls `update_state`.
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(|_, _| vec![0]));
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

/// A stored state replaced between the one-time check and the merge is not
/// trusted: the merge runs on bytes nobody verified, so the full path applies
/// and nothing is recorded for the result.
#[tokio::test(flavor = "current_thread")]
async fn a_stored_state_replaced_after_the_check_takes_the_full_path_for_a_delta() {
    let (mut executor, key) = executor_with_stored_state("uv_replaced_delta", true).await;
    let log = record_validations(&mut executor, &key);
    executor.test_replace_stored_after_gate = Some(vec![4, 4]);

    let r = delta(&mut executor, key, vec![7]).await;
    assert!(
        matches!(&r, UpsertResult::Updated(s) if s.as_ref() == [7]),
        "{r:?}"
    );
    assert_eq!(
        seen(&log),
        vec![STORED.to_vec(), vec![7]],
        "the check, then the result is validated because the merged-into state was not verified"
    );
    assert!(
        !executor
            .stored_state_verified
            .lock()
            .unwrap()
            .is_verified(&key, blake3::hash(&[7]).as_bytes()),
        "a result built on an unverified state must not be recorded"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_stored_state_replaced_after_the_check_takes_the_full_path_for_a_full_state() {
    let (mut executor, key) = executor_with_stored_state("uv_replaced_state", true).await;
    let log = record_validations(&mut executor, &key);
    executor
        .runtime
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(union));
    executor.test_replace_stored_after_gate = Some(vec![4, 4]);

    let r = full(&mut executor, key, vec![9]).await;
    assert!(
        matches!(&r, UpsertResult::Updated(s) if s.as_ref() == [4, 9]),
        "{r:?}"
    );
    assert_eq!(
        seen(&log),
        vec![STORED.to_vec(), vec![9], vec![4, 9]],
        "the incoming state and the result are validated after all"
    );
}

/// The late validation of the incoming full state, when the stored state
/// changed after it was verified, rejects an invalid state with the same error
/// the normal path gives, and stores and records nothing.
#[tokio::test(flavor = "current_thread")]
async fn an_invalid_incoming_state_is_rejected_when_the_stored_state_changed() {
    fn not_nine_nine(s: &[u8]) -> bool {
        s != [9, 9]
    }
    // The normal path's error, from a contract that does not opt in.
    let (mut plain, plain_key) = executor_with_stored_state("uv_late_reject_plain", false).await;
    plain
        .runtime
        .validate_overrides
        .insert(*plain_key.id(), ValidateOverride::ValidWhen(not_nine_nine));
    let expected = plain
        .upsert_contract_state(
            plain_key,
            Either::Left(WrappedState::new(vec![9, 9])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("invalid incoming state is rejected")
        .to_string();

    let (mut executor, key) = executor_with_stored_state("uv_late_reject", true).await;
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::ValidWhen(not_nine_nine));
    executor.test_replace_stored_after_gate = Some(vec![4, 4]);
    let err = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![9, 9])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect_err("the late check must reject the invalid incoming state");
    // Same kind of error as the normal path (the text names the key, which
    // differs between the two contracts).
    assert_eq!(
        err.to_string().replace(&key.to_string(), "K"),
        expected.replace(&plain_key.to_string(), "K")
    );
    assert_eq!(stored(&executor, &key).await, vec![4, 4]);
    let verified = executor.stored_state_verified.lock().unwrap();
    assert!(!verified.is_verified(&key, blake3::hash(&[9, 9]).as_bytes()));
    assert!(!verified.is_verified(&key, blake3::hash(&[4, 4]).as_bytes()));
}

/// The one-time check runs with no related contracts, so a verdict never
/// depends on related state a caller supplied. A stored state that needs
/// related contracts to validate stays untrusted, and the failed check is not
/// remembered against a later check that can succeed.
#[tokio::test(flavor = "current_thread")]
async fn a_stored_state_needing_related_contracts_is_not_trusted() {
    let (mut executor, key) = executor_with_stored_state("uv_related_gate", true).await;
    let needed = ContractInstanceId::new([5; 32]);
    executor
        .runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::RequestRelated(vec![needed]));

    // The caller supplies the related state. Were it passed to the check, the
    // stored state would validate and be trusted.
    let supplied = RelatedContracts::from(std::collections::HashMap::from([(
        needed,
        Some(State::from(vec![1])),
    )]));
    let r = executor
        .upsert_contract_state(
            key,
            Either::Right(StateDelta::from(vec![7])),
            supplied,
            None,
        )
        .await
        .expect("the full path accepts the update");
    assert!(
        matches!(&r, UpsertResult::Updated(s) if s.as_ref() == [7]),
        "{r:?}"
    );
    assert_eq!(
        executor.stored_state_verified.lock().unwrap().len(),
        0,
        "a verdict that depended on caller-supplied related state was recorded"
    );

    // A later update, whose check can succeed, is trusted.
    let log = record_validations(&mut executor, &key);
    delta(&mut executor, key, vec![8]).await;
    assert_eq!(seen(&log), vec![vec![7]], "only the one-time check ran");
    assert_eq!(executor.stored_state_verified.lock().unwrap().len(), 1);
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

/// `s` with every run of whitespace collapsed, so a pin does not depend on how
/// rustfmt wraps a line.
fn squash(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Source pins: each skip is gated on trust that is confirmed against the state
/// being merged into, and a stored state replaced outside the trusted update is
/// forgotten.
#[test]
fn the_skips_are_wired_to_the_trust_check() {
    let src = include_str!("../runtime/executor_impl.rs");
    let body = squash(
        src.split(
            "pub(in crate::contract::executor) async fn bridged_upsert_contract_state_inner(",
        )
        .nth(1)
        .expect("upsert body")
        .split("pub(in crate::contract::executor) fn bridged_register_contract_notifier(")
        .next()
        .expect("upsert body end"),
    );

    assert!(
        body.contains("let verified_hash = if is_new_contract { None }"),
        "a fresh store must never be trusted"
    );
    assert!(body.contains("self.verified_stored_state_hash(&key, &params)"));
    assert!(
        body.contains("let result = if incoming_validation_skipped {"),
        "incoming pre-validate gate"
    );
    assert!(
        body.contains("verified_hash .is_some_and(|hash| *blake3::hash(current_state.as_ref()).as_bytes() == hash)")
            || body.contains("verified_hash.is_some_and(|hash| *blake3::hash(current_state.as_ref()).as_bytes() == hash)"),
        "trust must be tied to the hash of the state that is merged into"
    );
    assert!(
        body.contains("if incoming_validation_skipped && !trusted_stored {"),
        "a skipped incoming validation must be performed when trust is not confirmed"
    );
    assert_eq!(
        body.matches("if trusted_stored && !recovery_performed {")
            .count(),
        2,
        "post-update validate skip and the verdict record"
    );
    assert!(
        body.contains("let local_valid = trusted_stored || self .runtime .validate_state(&key, &params, &current_state"),
        "merge-error path must not validate a trusted stored state"
    );
    assert_eq!(
        body.matches(".invalidate(&key)").count(),
        2,
        "initial install and the pre-commit forget"
    );

    // The helper verifies through the contract's own validate_state with NO
    // related contracts, trusts only `Valid`, and forgets a failed check.
    let helper = squash(
        src.split("async fn verified_stored_state_hash(")
            .nth(1)
            .expect("helper")
            .split("\n    }\n")
            .next()
            .expect("helper end"),
    );
    assert!(helper.contains("self.runtime.update_validates("));
    assert!(helper.contains("&RelatedContracts::default()"));
    assert!(!helper.contains("related_contracts"));
    assert!(helper.contains("matches!(verdict, Ok(ValidateResult::Valid))"));
    assert!(helper.contains(".invalidate(key)"));
}

fn key_n(n: usize) -> ContractKey {
    container(&format!("uv_capacity_{n}")).key()
}

/// Recording more keys than the capacity never grows the record past it.
#[test]
fn the_verified_record_never_exceeds_its_capacity() {
    let mut record = StoredStateVerified::with_capacity(3);
    for n in 0..20 {
        record.record(key_n(n), [n as u8; 32]);
        assert!(
            record.len() <= 3,
            "{} entries after {} records",
            record.len(),
            n + 1
        );
    }
    assert_eq!(record.len(), 3);
}

/// Re-recording a key the record already holds, at capacity, replaces its hash
/// and evicts nothing.
#[test]
fn re_recording_a_held_key_at_capacity_evicts_nothing() {
    let mut record = StoredStateVerified::with_capacity(3);
    for n in 0..3 {
        record.record(key_n(n), [n as u8; 32]);
    }
    for round in 1..=5u8 {
        for n in 0..3 {
            record.record(key_n(n), [100 + round; 32]);
        }
    }
    assert_eq!(record.len(), 3);
    for n in 0..3 {
        assert!(
            record.is_verified(&key_n(n), &[105; 32]),
            "key {n} was evicted"
        );
    }
}

/// A key evicted from a full record is not trusted: its stored state is
/// checked again before the opt-in is honoured.
#[tokio::test(flavor = "current_thread")]
async fn an_evicted_key_is_verified_again() {
    let (mut executor, a) = executor_with_stored_state("uv_evict_a", true).await;
    // A second contract in the same executor, on a record that holds one key.
    let b_container = container("uv_evict_b");
    let b = b_container.key();
    executor.runtime.update_validates_ids.insert(*b.id());
    executor
        .upsert_contract_state(
            b,
            Either::Left(WrappedState::new(STORED.to_vec())),
            RelatedContracts::default(),
            Some(b_container),
        )
        .await
        .expect("PUT of the second contract");
    let small: SharedStoredStateVerified =
        Arc::new(Mutex::new(StoredStateVerified::with_capacity(1)));
    executor.set_stored_state_verified(small);
    let log_a = record_validations(&mut executor, &a);
    let log_b = record_validations(&mut executor, &b);

    // `a`: checked once, then trusted on its own result.
    delta(&mut executor, a, vec![7]).await;
    delta(&mut executor, a, vec![8]).await;
    assert_eq!(
        seen(&log_a),
        vec![STORED.to_vec()],
        "checked once while held"
    );

    // `b` takes the only slot, evicting `a`.
    delta(&mut executor, b, vec![7]).await;
    assert_eq!(seen(&log_b), vec![STORED.to_vec()]);

    // `a` is checked again, on the state it holds now.
    delta(&mut executor, a, vec![9]).await;
    assert_eq!(
        seen(&log_a),
        vec![STORED.to_vec(), vec![8]],
        "an evicted key must be verified again"
    );
}
