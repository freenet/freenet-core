use locutus_stdlib::prelude::*;

use crate::contract::*;
use crate::{secrets_store::SecretsStore, tests::setup_test_contract, DelegateStore, Runtime};

const TEST_CONTRACT_1: &str = "test_contract_1";

#[test]
fn validate_state() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = setup_test_contract(TEST_CONTRACT_1)?;
    let mut runtime = Runtime::build(
        store,
        DelegateStore::default(),
        SecretsStore::default(),
        false,
    )
    .unwrap();

    let is_valid = runtime.validate_state(
        &key,
        &Parameters::from([].as_ref()),
        &WrappedState::new(vec![1, 2, 3, 4]),
        &Default::default(),
    )?;
    assert!(is_valid == ValidateResult::Valid);

    let not_valid = runtime.validate_state(
        &key,
        &Parameters::from([].as_ref()),
        &WrappedState::new(vec![1, 0, 0, 1]),
        &Default::default(),
    )?;
    assert!(matches!(not_valid, ValidateResult::RequestRelated(_)));

    Ok(())
}

#[test]
fn validate_delta() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = setup_test_contract(TEST_CONTRACT_1)?;
    let mut runtime = Runtime::build(
        store,
        DelegateStore::default(),
        SecretsStore::default(),
        false,
    )
    .unwrap();

    let is_valid = runtime.validate_delta(
        &key,
        &Parameters::from([].as_ref()),
        &StateDelta::from([1, 2, 3, 4].as_ref()),
    )?;
    assert!(is_valid);

    let not_valid = !runtime.validate_delta(
        &key,
        &Parameters::from([].as_ref()),
        &StateDelta::from([1, 0, 0, 1].as_ref()),
    )?;
    assert!(not_valid);

    Ok(())
}

#[test]
fn update_state() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = setup_test_contract(TEST_CONTRACT_1)?;
    let mut runtime = Runtime::build(
        store,
        DelegateStore::default(),
        SecretsStore::default(),
        false,
    )
    .unwrap();

    let new_state = runtime
        .update_state(
            &key,
            &Parameters::from([].as_ref()),
            &WrappedState::new(vec![5, 2, 3]),
            &[StateDelta::from([4].as_ref()).into()],
        )?
        .unwrap_valid();
    assert!(new_state.as_ref().len() == 4);
    assert!(new_state.as_ref()[3] == 4);
    Ok(())
}

#[test]
fn summarize_state() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = setup_test_contract(TEST_CONTRACT_1)?;
    let mut runtime = Runtime::build(
        store,
        DelegateStore::default(),
        SecretsStore::default(),
        false,
    )
    .unwrap();

    let summary = runtime.summarize_state(
        &key,
        &Parameters::from([].as_ref()),
        &WrappedState::new(vec![5, 2, 3, 4]),
    )?;
    assert_eq!(summary.as_ref(), &[5, 2, 3]);
    Ok(())
}

#[test]
fn get_state_delta() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = setup_test_contract(TEST_CONTRACT_1)?;
    let mut runtime = Runtime::build(
        store,
        DelegateStore::default(),
        SecretsStore::default(),
        false,
    )
    .unwrap();

    let delta = runtime.get_state_delta(
        &key,
        &Parameters::from([].as_ref()),
        &WrappedState::new(vec![5, 2, 3, 4]),
        &StateSummary::from([2, 3].as_ref()),
    )?;
    assert!(delta.as_ref().len() == 1);
    assert!(delta.as_ref()[0] == 4);
    Ok(())
}
