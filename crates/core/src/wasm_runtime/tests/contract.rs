use freenet_stdlib::prelude::*;

use crate::wasm_runtime::tests::TestSetup;

use super::super::Runtime;
use super::super::contract::*;

const TEST_CONTRACT_1: &str = "test_contract_1";

#[tokio::test(flavor = "multi_thread")]
async fn validate_state() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1).await?;
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    let is_valid = runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &WrappedState::new(vec![1, 2, 3, 4]),
        &Default::default(),
    )?;
    assert!(is_valid == ValidateResult::Valid);

    let not_valid = runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &WrappedState::new(vec![1, 0, 0, 1]),
        &Default::default(),
    )?;
    assert!(matches!(not_valid, ValidateResult::RequestRelated(_)));
    std::mem::drop(temp_dir);
    Ok(())
}

/// Regression test for issue #2216: `validate_state_with_contract` must
/// compile and validate directly from the caller-supplied `ContractContainer`
/// WITHOUT needing the contract to already be resolvable via
/// `contract_store.fetch_contract` — i.e. it must not depend on the
/// store→fetch round-trip `verify_and_store_contract` used to require.
///
/// This is proven by deliberately NOT storing the contract in `contract_store`
/// (unlike `setup_test_contract`, which always does) before calling
/// `validate_state_with_contract`: if the method secretly still needed
/// `contract_store.fetch_contract` on the module-cache miss, this would fail
/// with `ContractNotFound` instead of validating successfully.
#[tokio::test(flavor = "multi_thread")]
async fn validate_state_with_contract_does_not_require_prior_store()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;

    use freenet_stdlib::prelude::{
        ContractCode, ContractContainer, ContractWasmAPIVersion, WrappedContract,
    };

    use crate::contract::storages::Storage;
    use crate::util::tests::get_temp_dir;

    let temp_dir = get_temp_dir();
    let module_bytes = crate::wasm_runtime::tests::get_test_module(TEST_CONTRACT_1)?;
    let contract_bytes =
        WrappedContract::new(Arc::new(ContractCode::from(module_bytes)), vec![].into());
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_bytes));
    let contract_key = contract.key();
    let params = Parameters::from([].as_ref());

    let storage = Storage::new(temp_dir.path()).await?;
    // Deliberately empty: the contract is NEVER stored here.
    let contract_store = super::super::ContractStore::new(
        temp_dir.path().join("contract"),
        10_000,
        storage.clone(),
    )?;
    let delegate_store = super::super::DelegateStore::new(
        temp_dir.path().join("delegate"),
        10_000,
        storage.clone(),
    )?;
    let secrets_store = super::super::SecretsStore::new(
        temp_dir.path().join("secrets"),
        Default::default(),
        storage,
    )?;
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    // Sanity check: fetch_contract genuinely misses (proves the round-trip
    // this test guards against would fail if it were still required).
    assert!(
        runtime
            .contract_store
            .fetch_contract(&contract_key, &params)
            .is_none(),
        "test setup invariant: the contract must NOT be in contract_store yet"
    );

    let is_valid = runtime.validate_state_with_contract(
        &contract_key,
        &params,
        &WrappedState::new(vec![1, 2, 3, 4]),
        &Default::default(),
        &contract,
    )?;
    assert_eq!(
        is_valid,
        ValidateResult::Valid,
        "validate_state_with_contract must validate directly from the \
         supplied ContractContainer, without needing it in contract_store"
    );

    // The invalid-state branch must behave identically to `validate_state`.
    let not_valid = runtime.validate_state_with_contract(
        &contract_key,
        &params,
        &WrappedState::new(vec![1, 0, 0, 1]),
        &Default::default(),
        &contract,
    )?;
    assert!(matches!(not_valid, ValidateResult::RequestRelated(_)));

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn update_state() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1).await?;
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    let new_state = runtime
        .update_state(
            &contract_key,
            &Parameters::from([].as_ref()),
            &WrappedState::new(vec![5, 2, 3]),
            &[StateDelta::from([4].as_ref()).into()],
        )?
        .unwrap_valid();
    assert!(new_state.as_ref().len() == 4);
    assert!(new_state.as_ref()[3] == 4);
    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn summarize_state() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1).await?;
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    let summary = runtime.summarize_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &WrappedState::new(vec![5, 2, 3, 4]),
    )?;
    assert_eq!(summary.as_ref(), &[5, 2, 3]);
    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn get_state_delta() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1).await?;
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    let delta = runtime.get_state_delta(
        &contract_key,
        &Parameters::from([].as_ref()),
        &WrappedState::new(vec![5, 2, 3, 4]),
        &StateSummary::from([2, 3].as_ref()),
    )?;
    assert!(delta.as_ref().len() == 1);
    assert!(delta.as_ref()[0] == 4);
    std::mem::drop(temp_dir);
    Ok(())
}
