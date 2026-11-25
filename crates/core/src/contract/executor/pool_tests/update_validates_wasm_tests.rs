//! `__frnt__update_validates` through the real executor and real WASM.
//!
//! `test-contract-update-validates` and `test-contract-update-validates-off` are
//! the same contract; only the first exports the marker. Their `validate_state`
//! rejects a state starting with 0xFF and their `update_state` accepts any
//! incoming state except one starting with 0xEE, so a state that is invalid
//! under `validate_state` and is committed anyway shows that `validate_state`
//! did not run on it.

use either::Either;
use freenet_stdlib::prelude::*;

use crate::contract::executor::{ContractExecutor, Executor, OperationMode, UpsertResult};
use crate::wasm_runtime::{ContractStore, DelegateStore, Runtime, SecretsStore, StateStore};

const OPTING_IN: &str = "test-contract-update-validates";
const NOT_OPTING_IN: &str = "test-contract-update-validates-off";

const STORED: [u8; 1] = [1];
const POISON: [u8; 2] = [0xFF, 2];
const REFUSED: [u8; 2] = [0xEE, 3];

struct Harness {
    executor: Executor<Runtime>,
    key: ContractKey,
    _temp_dir: tempfile::TempDir,
}

async fn build_executor() -> (Executor<Runtime>, tempfile::TempDir) {
    let temp_dir = crate::util::tests::get_temp_dir();
    let db = crate::contract::storages::Storage::new(temp_dir.path())
        .await
        .unwrap();
    let contract_store =
        ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone()).unwrap();
    let delegate_store =
        DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone()).unwrap();
    let secrets_store = SecretsStore::new(
        temp_dir.path().join("secrets"),
        Default::default(),
        db.clone(),
    )
    .unwrap();
    let state_store = StateStore::new(db, 10_000_000).unwrap();
    let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();
    let executor = Executor::new(state_store, || Ok(()), OperationMode::Local, runtime, None)
        .await
        .unwrap();
    (executor, temp_dir)
}

async fn load(name: &'static str) -> ContractContainer {
    tokio::task::spawn_blocking(move || {
        crate::test_utils::load_contract(name, Parameters::from(vec![]))
    })
    .await
    .expect("join contract compile")
    .expect("compile contract")
}

/// An executor holding `name`'s contract with [`STORED`] stored.
async fn harness(name: &'static str) -> Harness {
    let (mut executor, temp_dir) = build_executor().await;
    let contract = load(name).await;
    let key = contract.key();
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(STORED.to_vec())),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("initial PUT");
    // Every scenario below is meaningful only for the declaration it names.
    {
        use crate::wasm_runtime::ContractRuntimeInterface;
        let declared = executor
            .runtime
            .update_validates(&key, &Parameters::from(vec![]));
        assert_eq!(declared, name == OPTING_IN, "{name}");
    }
    Harness {
        executor,
        key,
        _temp_dir: temp_dir,
    }
}

impl Harness {
    async fn full(&mut self, bytes: &[u8]) -> Result<UpsertResult, String> {
        self.executor
            .upsert_contract_state(
                self.key,
                Either::Left(WrappedState::new(bytes.to_vec())),
                RelatedContracts::default(),
                None,
            )
            .await
            .map_err(|e| e.to_string())
    }

    async fn delta(&mut self, bytes: &[u8]) -> Result<UpsertResult, String> {
        self.executor
            .upsert_contract_state(
                self.key,
                Either::Right(StateDelta::from(bytes.to_vec())),
                RelatedContracts::default(),
                None,
            )
            .await
            .map_err(|e| e.to_string())
    }

    async fn stored(&self) -> Vec<u8> {
        self.executor
            .state_store
            .get(&self.key)
            .await
            .expect("stored state")
            .as_ref()
            .to_vec()
    }
}

/// The compiled contract declares the marker exactly when it exports it.
#[tokio::test]
async fn the_export_decides_the_declaration() {
    use crate::wasm_runtime::ContractRuntimeInterface;
    for (name, expected) in [(OPTING_IN, true), (NOT_OPTING_IN, false)] {
        let mut h = harness(name).await;
        let params = Parameters::from(vec![]);
        assert_eq!(
            h.executor.runtime.update_validates(&h.key, &params),
            expected
        );
    }
}

/// A delta result and a full-state result are not validated for the opting-in
/// contract; the same contract without the export has both rejected.
#[tokio::test]
async fn results_are_not_revalidated_only_when_declared() {
    let mut h = harness(OPTING_IN).await;
    assert!(matches!(
        h.delta(&POISON).await,
        Ok(UpsertResult::Updated(_))
    ));
    assert_eq!(h.stored().await, POISON.to_vec());

    let mut h = harness(OPTING_IN).await;
    assert!(matches!(
        h.full(&POISON).await,
        Ok(UpsertResult::Updated(_))
    ));
    assert_eq!(h.stored().await, POISON.to_vec());

    let mut h = harness(NOT_OPTING_IN).await;
    let err = h.delta(&POISON).await.expect_err("validated and rejected");
    assert!(err.to_lowercase().contains("invalid"), "{err}");
    let err = h.full(&POISON).await.expect_err("validated and rejected");
    assert!(err.to_lowercase().contains("invalid"), "{err}");
    assert_eq!(h.stored().await, STORED.to_vec());
}

/// The same bytes as the stored state are a no-change for both contracts.
#[tokio::test]
async fn equal_bytes_are_no_change() {
    for name in [OPTING_IN, NOT_OPTING_IN] {
        let mut h = harness(name).await;
        assert!(matches!(h.full(&STORED).await, Ok(UpsertResult::NoChange)));
        assert_eq!(h.stored().await, STORED.to_vec());
    }
}

/// An `update_state` error on a full state is returned and the stored state is
/// unchanged, with and without the export.
#[tokio::test]
async fn an_update_error_leaves_the_stored_state_unchanged() {
    for name in [OPTING_IN, NOT_OPTING_IN] {
        let mut h = harness(name).await;
        h.full(&REFUSED).await.expect_err("refused by the contract");
        h.delta(&REFUSED)
            .await
            .expect_err("refused by the contract");
        assert_eq!(h.stored().await, STORED.to_vec(), "{name}");
    }
}

/// A fresh store is validated in full whether or not the contract declares it.
#[tokio::test]
async fn a_fresh_store_of_an_invalid_state_is_rejected() {
    for name in [OPTING_IN, NOT_OPTING_IN] {
        let (mut executor, _dir) = build_executor().await;
        let contract = load(name).await;
        let key = contract.key();
        let err = executor
            .upsert_contract_state(
                key,
                Either::Left(WrappedState::new(POISON.to_vec())),
                RelatedContracts::default(),
                Some(contract),
            )
            .await
            .expect_err("an invalid fresh state is rejected");
        assert!(
            err.to_string().to_lowercase().contains("invalid"),
            "{name}: {err}"
        );
    }
}

/// A stored state that does not validate is not trusted, even for a contract
/// that declares the marker: the full path applies, so an update that would
/// produce an invalid state is rejected, and an update the contract refuses
/// recovers through the corrupted-state replacement.
#[tokio::test]
async fn an_invalid_stored_state_is_not_trusted() {
    // Not trusted: a result that is invalid is rejected, not committed.
    let mut h = harness(OPTING_IN).await;
    h.executor
        .state_store
        .update(&h.key, WrappedState::new(POISON.to_vec()))
        .await
        .expect("plant an invalid stored state");
    let err = h
        .delta(&[0xFF, 3])
        .await
        .expect_err("validated on the full path and rejected");
    assert!(err.to_lowercase().contains("invalid"), "{err}");
    assert_eq!(h.stored().await, POISON.to_vec());

    // Recovery: the contract refuses the incoming state and the stored state
    // is invalid, so the stored state is replaced with the incoming one.
    let mut h = harness(OPTING_IN).await;
    h.executor
        .state_store
        .update(&h.key, WrappedState::new(POISON.to_vec()))
        .await
        .expect("plant an invalid stored state");
    let r = h.full(&REFUSED).await;
    assert!(matches!(r, Ok(UpsertResult::Updated(_))), "{r:?}");
    assert_eq!(h.stored().await, REFUSED.to_vec());
}
