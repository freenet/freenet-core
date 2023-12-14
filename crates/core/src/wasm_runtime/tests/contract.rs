use freenet_stdlib::prelude::*;

use crate::wasm_runtime::tests::TestSetup;

use super::super::contract::*;
use super::super::Runtime;

const TEST_CONTRACT_1: &str = "test_contract_1";

#[test]
fn validate_state() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1)?;
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

#[test]
fn validate_delta() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1)?;
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    let is_valid = runtime.validate_delta(
        &contract_key,
        &Parameters::from([].as_ref()),
        &StateDelta::from([1, 2, 3, 4].as_ref()),
    )?;
    assert!(is_valid);

    let not_valid = !runtime.validate_delta(
        &contract_key,
        &Parameters::from([].as_ref()),
        &StateDelta::from([1, 0, 0, 1].as_ref()),
    )?;
    assert!(not_valid);
    std::mem::drop(temp_dir);
    Ok(())
}

#[test]
fn update_state() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1)?;
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

#[test]
fn summarize_state() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1)?;
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

#[test]
fn get_state_delta() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_1)?;
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
