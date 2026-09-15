//! A container's parameters may only reach the durable params row once the
//! container's key has been verified as derived from its code and parameters.
//!
//! The params row is keyed by instance id alone, and every later operation that
//! arrives without code (UPDATE, summarize, delta, serving the contract) reads
//! its parameters from there. So a container that claims an existing contract's
//! instance id, but carries other parameters, must be refused before anything
//! is written, not merely refused.
//!
//! These tests need a real `Executor<Runtime>`. The identity check lives in
//! `ContractStore::store_contract`, and the mock executor's in-memory store
//! performs none (see the note in `wasm_runtime/simulation_runtime.rs`), so a
//! mock-runtime test cannot observe a refused container at all.

use either::Either;
use freenet_stdlib::client_api::ContractRequest;
use freenet_stdlib::prelude::*;
use std::sync::Arc;

use crate::client_events::ClientId;
use crate::contract::executor::{ContractExecutor, Executor, OperationMode};
use crate::wasm_runtime::{
    ContractStore, DelegateStore, Runtime, SecretsStore, StateStorage, StateStore,
};

/// `validate_state` always returns `Valid` and `update_state` takes the last
/// incoming state. It ignores its parameters, so the same binary under
/// different parameters gives distinct, individually valid instances — which
/// is exactly what a forged container needs to look like.
const CONTRACT: &str = "test-contract-mock-aligned";

struct Harness {
    executor: Executor<Runtime>,
    _temp_dir: tempfile::TempDir,
}

async fn build_harness() -> Result<Harness, Box<dyn std::error::Error>> {
    let temp_dir = crate::util::tests::get_temp_dir();
    let db = crate::contract::storages::Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())?;
    let secrets_store = SecretsStore::new(
        temp_dir.path().join("secrets"),
        Default::default(),
        db.clone(),
    )?;
    let state_store = StateStore::new(db, 10_000_000)?;
    let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)?;
    let executor = Executor::new(
        state_store,
        || Ok(()),
        OperationMode::Local,
        runtime,
        None,
    )
    .await?;
    Ok(Harness {
        executor,
        _temp_dir: temp_dir,
    })
}

async fn load(params: Parameters<'static>) -> ContractContainer {
    tokio::task::spawn_blocking(move || crate::test_utils::load_contract(CONTRACT, params))
        .await
        .expect("join contract compile")
        .expect("compile contract")
}

/// Re-key `container` onto `victim`'s instance id, keeping the true hash of
/// the container's code. Only the instance derivation is wrong, which is the
/// shape that passes the executor's own `key.id() == container.key().id()`
/// comparison, since the sender chooses both sides of it.
fn claim_instance(victim: &ContractKey, container: ContractContainer) -> ContractContainer {
    let ContractContainer::Wasm(ContractWasmAPIVersion::V1(mut contract)) = container else {
        panic!("unexpected container version");
    };
    contract.key = ContractKey::from_id_and_code(*victim.id(), *contract.key.code_hash());
    ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract))
}

async fn stored_params(executor: &Executor<Runtime>, key: &ContractKey) -> Option<Vec<u8>> {
    executor
        .state_store
        .get_params(key)
        .await
        .expect("read stored params")
        .map(|p| p.as_ref().to_vec())
}

async fn stored_state(executor: &Executor<Runtime>, key: &ContractKey) -> Vec<u8> {
    executor
        .state_store
        .get(key)
        .await
        .expect("read stored state")
        .as_ref()
        .to_vec()
}

async fn upsert(
    executor: &mut Executor<Runtime>,
    container: ContractContainer,
    state: &[u8],
) -> Result<(), String> {
    executor
        .upsert_contract_state(
            container.key(),
            Either::Left(WrappedState::new(state.to_vec())),
            RelatedContracts::default(),
            Some(container),
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

async fn local_put(
    executor: &mut Executor<Runtime>,
    container: ContractContainer,
    state: &[u8],
) -> Result<(), String> {
    executor
        .contract_requests(
            ContractRequest::Put {
                contract: container,
                state: WrappedState::new(state.to_vec()),
                related_contracts: RelatedContracts::default(),
                subscribe: false,
                blocking_subscribe: false,
            },
            ClientId::FIRST,
            None,
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Install an honest contract and return its key.
async fn install_honest(
    executor: &mut Executor<Runtime>,
    params: &Parameters<'static>,
    state: &[u8],
) -> ContractKey {
    let honest = load(params.clone()).await;
    let key = honest.key();
    upsert(executor, honest, state)
        .await
        .expect("honest PUT must succeed");
    assert_eq!(
        stored_params(executor, &key).await.as_deref(),
        Some(params.as_ref()),
        "sanity: the honest PUT stored its parameters"
    );
    key
}

/// Blob-present path: the forged container reuses the stored binary, so the
/// executor takes its "code already on disk" branch.
#[tokio::test(flavor = "multi_thread")]
async fn forged_container_for_stored_code_leaves_stored_params_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    let honest_params = Parameters::from(vec![0x51, 0x01]);
    let honest_key = install_honest(&mut h.executor, &honest_params, b"honest state").await;

    let forged = claim_instance(&honest_key, load(Parameters::from(vec![0x51, 0xEE])).await);
    assert!(
        h.executor
            .runtime
            .contract_store
            .code_blob_stored(forged.key().code_hash()),
        "fixture must exercise the blob-present branch"
    );

    let err = upsert(&mut h.executor, forged, b"forged state")
        .await
        .expect_err("a container whose key is not derived from its code and parameters must be refused");

    assert_eq!(
        stored_params(&h.executor, &honest_key).await.as_deref(),
        Some(honest_params.as_ref()),
        "a refused container must leave the stored parameters byte-for-byte unchanged \
         (refusal was: {err})"
    );
    assert_eq!(
        stored_state(&h.executor, &honest_key).await,
        b"honest state",
        "a refused container must not change the stored state"
    );
    Ok(())
}

/// Blob-absent path: the forged container carries code this node has never
/// stored, so the executor takes its "store new code" branch.
#[tokio::test(flavor = "multi_thread")]
async fn forged_container_with_new_code_leaves_stored_params_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    let honest_params = Parameters::from(vec![0x52, 0x01]);
    let honest_key = install_honest(&mut h.executor, &honest_params, b"honest state").await;

    let unstored = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(b"code this node never stored".to_vec())),
        Parameters::from(vec![0x52, 0xEE]),
    )));
    let forged = claim_instance(&honest_key, unstored);
    assert!(
        !h.executor
            .runtime
            .contract_store
            .code_blob_stored(forged.key().code_hash()),
        "fixture must exercise the blob-absent branch"
    );

    let err = upsert(&mut h.executor, forged, b"forged state")
        .await
        .expect_err("a container whose key is not derived from its code and parameters must be refused");

    assert_eq!(
        stored_params(&h.executor, &honest_key).await.as_deref(),
        Some(honest_params.as_ref()),
        "a refused container must leave the stored parameters byte-for-byte unchanged \
         (refusal was: {err})"
    );
    assert_eq!(
        stored_state(&h.executor, &honest_key).await,
        b"honest state",
        "a refused container must not change the stored state"
    );
    assert_eq!(
        h.executor
            .lookup_key(honest_key.id())
            .map(|k| *k.code_hash()),
        Some(*honest_key.code_hash()),
        "the honest instance must still resolve to its own code"
    );
    Ok(())
}

/// The local-mode re-PUT into an existing contract merges with the container's
/// parameters. It must verify the container before trusting them.
#[tokio::test(flavor = "multi_thread")]
async fn local_reput_of_forged_container_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    let honest_params = Parameters::from(vec![0x53, 0x01]);
    let honest = load(honest_params.clone()).await;
    let honest_key = honest.key();
    local_put(&mut h.executor, honest, b"honest state")
        .await
        .map_err(|e| format!("honest local PUT failed: {e}"))?;

    let forged = claim_instance(&honest_key, load(Parameters::from(vec![0x53, 0xEE])).await);
    let result = local_put(&mut h.executor, forged, b"forged state").await;

    assert!(
        result.is_err(),
        "a local re-PUT whose container key is not derived from its code and parameters \
         must be refused, not merged under the container's parameters"
    );
    assert_eq!(
        stored_state(&h.executor, &honest_key).await,
        b"honest state",
        "a refused re-PUT must not change the stored state"
    );
    assert_eq!(
        stored_params(&h.executor, &honest_key).await.as_deref(),
        Some(honest_params.as_ref()),
        "a refused re-PUT must not change the stored parameters"
    );
    Ok(())
}

/// A VALID container for a contract whose state is stored but whose params
/// row is missing must restore the params on the merge path. The merge
/// commits through `state_store.update`, which writes state only, so this is
/// the case the executor's explicit params write exists for. `blob_stored`
/// selects which of the executor's two code branches the upsert takes.
async fn valid_container_restores_missing_params_on_merge(
    blob_stored: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    let tag = if blob_stored { 0x55 } else { 0x54 };
    if blob_stored {
        // Another instance of the same binary puts the blob on disk.
        install_honest(&mut h.executor, &Parameters::from(vec![tag, 0x02]), b"other").await;
    }

    let params = Parameters::from(vec![tag, 0x01]);
    let container = load(params.clone()).await;
    let key = container.key();
    assert_eq!(
        h.executor
            .runtime
            .contract_store
            .code_blob_stored(key.code_hash()),
        blob_stored,
        "fixture must exercise the intended code branch"
    );

    // State without params: the backend's `store` writes the state row only.
    h.executor
        .state_store
        .inner()
        .store(key, WrappedState::new(b"pre-existing".to_vec()))
        .await
        .expect("seed state without params");
    assert_eq!(
        stored_params(&h.executor, &key).await,
        None,
        "sanity: the seeded contract has no params row"
    );

    upsert(&mut h.executor, container, b"merged")
        .await
        .map_err(|e| format!("valid container upsert failed: {e}"))?;

    assert_eq!(
        stored_state(&h.executor, &key).await,
        b"merged",
        "sanity: the upsert took the merge path and committed"
    );
    assert_eq!(
        stored_params(&h.executor, &key).await.as_deref(),
        Some(params.as_ref()),
        "a verified container must still persist its parameters on the merge path"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_container_restores_missing_params_on_merge_with_new_code()
-> Result<(), Box<dyn std::error::Error>> {
    valid_container_restores_missing_params_on_merge(false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_container_restores_missing_params_on_merge_with_stored_code()
-> Result<(), Box<dyn std::error::Error>> {
    valid_container_restores_missing_params_on_merge(true).await
}

/// A valid new instance of an already-stored binary is stored with its own
/// parameters and leaves the first instance's parameters alone.
#[tokio::test(flavor = "multi_thread")]
async fn valid_new_instance_of_stored_code_stores_its_own_params()
-> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    let first_params = Parameters::from(vec![0x56, 0x01]);
    let first_key = install_honest(&mut h.executor, &first_params, b"first").await;

    let second_params = Parameters::from(vec![0x56, 0x02]);
    let second_key = install_honest(&mut h.executor, &second_params, b"second").await;
    assert_ne!(first_key.id(), second_key.id());
    assert_eq!(first_key.code_hash(), second_key.code_hash());

    assert_eq!(
        stored_params(&h.executor, &first_key).await.as_deref(),
        Some(first_params.as_ref()),
        "storing a second instance must not touch the first instance's parameters"
    );
    Ok(())
}
