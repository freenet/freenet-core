//! A container's parameters may only reach the durable params row once the
//! container's key has been verified as derived from its code and parameters.
//!
//! The params row is keyed by instance id alone, and every later operation that
//! arrives without code (UPDATE, summarize, delta, serving the contract) reads
//! its parameters from there, so a container's parameters must not be written
//! before its key has been verified.
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
/// different parameters gives distinct, individually valid instances, and any
/// refusal a test observes comes from the identity check rather than from the
/// contract.
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
    let executor =
        Executor::new(state_store, || Ok(()), OperationMode::Local, runtime, None).await?;
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

/// A container whose key names `victim`'s instance id while its code and
/// parameters derive a different one. The key keeps the true hash of the
/// container's code, so the instance derivation is the only thing wrong with it.
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
        .expect_err(
            "a container whose key is not derived from its code and parameters must be refused",
        );

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
        .expect_err(
            "a container whose key is not derived from its code and parameters must be refused",
        );

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
        install_honest(
            &mut h.executor,
            &Parameters::from(vec![tag, 0x02]),
            b"other",
        )
        .await;
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

/// A params row that does not derive its instance id, such as one written
/// before the write was ordered after verification, must not be used by the
/// operations that arrive without code, and must not be served. A verified
/// container for the instance repairs it.
#[tokio::test(flavor = "multi_thread")]
async fn stored_params_not_deriving_the_instance_are_not_used()
-> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    let honest_params = Parameters::from(vec![0x57, 0x01]);
    let honest = load(honest_params.clone()).await;
    let honest_key = install_honest(&mut h.executor, &honest_params, b"honest state").await;

    h.executor
        .state_store
        .inner()
        .store_params(honest_key, Parameters::from(vec![0x57, 0xEE]))
        .await
        .expect("overwrite the params row directly");

    // A code-less update must not run the contract with the row's parameters.
    let result = h
        .executor
        .upsert_contract_state(
            honest_key,
            Either::Left(WrappedState::new(b"code-less update".to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await;
    assert!(
        result.is_err(),
        "a code-less update must be refused while the stored parameters do not derive \
         the instance id"
    );
    assert_eq!(
        stored_state(&h.executor, &honest_key).await,
        b"honest state",
        "a refused code-less update must not change the stored state"
    );

    // Serving must not build a container from the row's parameters.
    let (_, served) = h.executor.fetch_contract(honest_key, true).await?;
    assert!(
        served.is_none(),
        "no container may be built from stored parameters that do not derive the \
         instance id (served key: {:?})",
        served.map(|c| c.key())
    );

    // A verified container repairs the row, and code-less operations work again.
    upsert(&mut h.executor, honest, b"repaired")
        .await
        .map_err(|e| format!("verified container upsert failed: {e}"))?;
    assert_eq!(
        stored_params(&h.executor, &honest_key).await.as_deref(),
        Some(honest_params.as_ref()),
        "a verified container must rewrite the params row"
    );
    h.executor
        .upsert_contract_state(
            honest_key,
            Either::Left(WrappedState::new(b"after repair".to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await
        .map_err(|e| format!("code-less update after repair failed: {e}"))?;
    assert_eq!(
        stored_state(&h.executor, &honest_key).await,
        b"after repair"
    );
    Ok(())
}

/// A forged container for an instance this node does not hold must not leave a
/// params row behind for the instance id it claimed. The claim carries EMPTY
/// parameters, the smallest input the row can take.
#[tokio::test(flavor = "multi_thread")]
async fn forged_container_for_unheld_instance_leaves_no_params_row()
-> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    // Never installed on this node.
    let claimed = load(Parameters::from(vec![0x58, 0x01])).await.key();
    let forged = claim_instance(&claimed, load(Parameters::from(Vec::<u8>::new())).await);

    let err = upsert(&mut h.executor, forged, b"forged state")
        .await
        .expect_err(
            "a container whose key is not derived from its code and parameters must be refused",
        );

    assert_eq!(
        stored_params(&h.executor, &claimed).await,
        None,
        "a refused container must not create a params row for the instance it claimed \
         (refusal was: {err})"
    );
    assert!(
        h.executor.lookup_key(claimed.id()).is_none(),
        "a refused container must not index the instance it claimed"
    );
    Ok(())
}

/// Ordering pin: the bridged upsert has exactly one params write, and it follows
/// every `store_contract` call (the calls that verify a supplied container).
///
/// The behavioural tests above catch a reverted ordering on both code branches.
/// This also catches a SECOND params write added to the function ahead of
/// verification, which none of them would notice if it sat on a path they do
/// not take. It scrapes `executor_impl.rs` from this file, so its own needle
/// strings cannot satisfy it, and it cuts off that file's test modules first,
/// whose pins quote the same signatures.
#[test]
fn upsert_writes_container_params_only_after_store_contract() {
    let full = include_str!("../runtime/executor_impl.rs");
    let production = &full[..full
        .find("\n#[cfg(test)]\nmod ")
        .expect("executor_impl.rs must have a top-level #[cfg(test)] mod section")];
    let start = production
        .find("async fn bridged_upsert_contract_state_inner(")
        .expect("bridged_upsert_contract_state_inner not found");
    let after = &production[start..];
    let end = after
        .find("fn bridged_register_contract_notifier(")
        .expect("the method following bridged_upsert_contract_state_inner moved");
    // Whole-line `//` comments stripped, so prose naming a call cannot match.
    let body = after[..end]
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");

    let writes: Vec<usize> = body
        .match_indices(".ensure_params(")
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        writes.len(),
        1,
        "expected exactly one params write in the bridged upsert"
    );
    assert!(
        !body.contains("store_params("),
        "the bridged upsert must not write the params row directly"
    );
    let last_verify = body
        .rfind(".store_contract(")
        .expect("the bridged upsert no longer calls store_contract");
    assert!(
        last_verify < writes[0],
        "the params write ({}) must follow every store_contract call (last at {last_verify}): \
         only parameters that verification has bound to the instance id may be written",
        writes[0]
    );
    // The initial-state install also writes the params row, through
    // `state_store.store`; it must follow verification too.
    let install = body
        .find(".store(key, state_to_store, params.clone())")
        .expect("the bridged upsert's initial-state install moved");
    assert!(
        last_verify < install,
        "the initial-state install ({install}) writes the params row and must follow every \
         store_contract call (last at {last_verify})"
    );
}

/// A local re-PUT of the verified container repairs a params row that the
/// read-side check refuses, which is the remedy the refusal's WARN names.
#[tokio::test(flavor = "multi_thread")]
async fn local_reput_repairs_a_refused_params_row() -> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    let params = Parameters::from(vec![0x5C, 0x01]);
    let honest = load(params.clone()).await;
    let key = honest.key();
    local_put(&mut h.executor, honest.clone(), b"honest state")
        .await
        .map_err(|e| format!("honest local PUT failed: {e}"))?;

    h.executor
        .state_store
        .inner()
        .store_params(key, Parameters::from(vec![0x5C, 0xEE]))
        .await
        .expect("overwrite the params row directly");
    assert!(
        h.executor
            .verified_stored_params(&key)
            .await
            .map_err(|e| e.to_string())?
            .is_none(),
        "sanity: the overwritten row is refused"
    );

    local_put(&mut h.executor, honest.clone(), b"re-put state")
        .await
        .map_err(|e| format!("re-PUT of the verified container failed: {e}"))?;
    assert_eq!(
        stored_params(&h.executor, &key).await.as_deref(),
        Some(params.as_ref()),
        "a local re-PUT of the verified container must repair the params row"
    );
    assert!(
        h.executor
            .verified_stored_params(&key)
            .await
            .map_err(|e| e.to_string())?
            .is_some(),
        "the repaired row must pass the read-side check again"
    );

    // The usual remedy re-sends the SAME state, which takes the merge's
    // no-change early return. The repair must not depend on the state changing.
    h.executor
        .state_store
        .inner()
        .store_params(key, Parameters::from(vec![0x5C, 0xEF]))
        .await
        .expect("overwrite the params row directly");
    local_put(&mut h.executor, honest, b"re-put state")
        .await
        .map_err(|e| format!("same-state re-PUT of the verified container failed: {e}"))?;
    assert_eq!(
        stored_params(&h.executor, &key).await.as_deref(),
        Some(params.as_ref()),
        "a same-state local re-PUT of the verified container must also repair the params row"
    );
    Ok(())
}

/// Pins `verified_stored_params` to exactly one derivation: stdlib
/// `ContractKey::from_params` over the
/// stored parameter bytes and the instance's INDEXED code hash, with an
/// instance that has no index row passed through unchanged. The derivation is
/// computed here independently of the helper, and each case asserts the helper
/// agrees with it.
#[tokio::test(flavor = "multi_thread")]
async fn verified_stored_params_is_the_indexed_code_hash_derivation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut h = build_harness().await?;
    let params = Parameters::from(vec![0x59, 0x01]);
    let key = install_honest(&mut h.executor, &params, b"state").await;
    let indexed = h
        .executor
        .runtime
        .contract_store
        .code_hash_from_id(key.id())
        .expect("an honest install indexes the instance");
    let derives = |bytes: &[u8], code_hash: CodeHash| {
        let encoded = ContractKey::from_id_and_code(*key.id(), code_hash).encoded_code_hash();
        ContractKey::from_params(encoded, Parameters::from(bytes.to_vec()))
            .map(|derived| derived.id() == key.id())
            .unwrap_or(false)
    };

    // (a) The honest row: the derivation holds, and the helper returns the
    // stored bytes unchanged.
    assert!(derives(params.as_ref(), indexed));
    let got = h
        .executor
        .verified_stored_params(&key)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        got.as_ref().map(|p| p.as_ref()),
        Some(params.as_ref()),
        "the helper must accept an honest row and return its parameters unchanged"
    );

    // (b1) Tampered parameters under the same index row.
    let tampered = Parameters::from(vec![0x59, 0xEE]);
    assert!(!derives(tampered.as_ref(), indexed));
    h.executor
        .state_store
        .inner()
        .store_params(key, tampered)
        .await
        .expect("write tampered params row");
    assert!(
        h.executor
            .verified_stored_params(&key)
            .await
            .map_err(|e| e.to_string())?
            .is_none(),
        "the helper must refuse parameters that do not derive the instance id"
    );

    // (b2) Honest parameters, but the index names another code hash. The check
    // must use the INDEXED hash, not a hash that would make the row pass.
    h.executor
        .state_store
        .inner()
        .store_params(key, params.clone())
        .await
        .expect("restore honest params row");
    assert!(
        h.executor
            .verified_stored_params(&key)
            .await
            .map_err(|e| e.to_string())?
            .is_some(),
        "sanity: the restored honest row is accepted again"
    );
    let other = CodeHash::new([0x42; 32]);
    assert!(!derives(params.as_ref(), other));
    h.executor
        .runtime
        .contract_store
        .ensure_key_indexed(&ContractKey::from_id_and_code(*key.id(), other))
        .expect("re-point the index row");
    assert_eq!(
        h.executor
            .runtime
            .contract_store
            .code_hash_from_id(key.id()),
        Some(other)
    );
    assert!(
        h.executor
            .verified_stored_params(&key)
            .await
            .map_err(|e| e.to_string())?
            .is_none(),
        "the helper must check against the code hash the index holds"
    );

    // (c) No index row: the parameters pass through unchanged, whatever they are.
    let unindexed = ContractKey::from_id_and_code(
        ContractInstanceId::new([0x5A; 32]),
        CodeHash::new([0x5B; 32]),
    );
    assert!(
        h.executor
            .runtime
            .contract_store
            .code_hash_from_id(unindexed.id())
            .is_none()
    );
    let arbitrary = Parameters::from(vec![1, 2, 3]);
    h.executor
        .state_store
        .inner()
        .store_params(unindexed, arbitrary.clone())
        .await
        .expect("write params row for an unindexed instance");
    let got = h
        .executor
        .verified_stored_params(&unindexed)
        .await
        .map_err(|e| e.to_string())?;
    assert_eq!(
        got.as_ref().map(|p| p.as_ref()),
        Some(arbitrary.as_ref()),
        "with no index row the helper must return the stored parameters unchanged"
    );
    Ok(())
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
