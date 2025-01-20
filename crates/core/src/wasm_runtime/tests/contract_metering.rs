use crate::wasm_runtime::tests::TestSetup;
use crate::wasm_runtime::{ContractExecError, RuntimeInnerError};
use freenet_stdlib::prelude::*;

use super::super::contract::*;
use super::super::Runtime;

const TEST_CONTRACT_MATERING: &str = "test_contract_metering";

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct TestConditions {
    pub max_iterations: u64,
}

#[test]
fn test_validate_state_metering_limits() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_MATERING)?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    // Create test conditions with very high iterations to trigger metering
    let test_conditions = TestConditions {
        max_iterations: 100_000_000,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);

    let result = runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );

    assert!(result.is_err());
    let result_err = result.unwrap_err();

    let is_gas_error = match result_err.deref() {
        RuntimeInnerError::ContractExecError(ContractExecError::OutOfGas) => true,
        _ => false,
    };

    assert!(is_gas_error);

    // Test with reasonable iterations
    let test_conditions = TestConditions {
        max_iterations: 1000, // This should work fine
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);

    let result = runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );

    assert!(result.is_ok());

    std::mem::drop(temp_dir);
    Ok(())
}

#[test]
fn test_update_state_metering_limits() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_MATERING)?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    // Create test conditions with very high iterations to trigger metering
    let test_conditions = TestConditions {
        max_iterations: 100_000_000,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);

    let result = runtime.update_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &[StateDelta::from([].as_ref()).into()],
    );

    assert!(result.is_err());
    let result_err = result.unwrap_err();

    let is_gas_error = match result_err.deref() {
        RuntimeInnerError::ContractExecError(ContractExecError::OutOfGas) => true,
        _ => false,
    };

    assert!(is_gas_error);

    // Test with reasonable iterations
    let test_conditions = TestConditions {
        max_iterations: 1000, // This should work fine
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);

    let result = runtime.update_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &[StateDelta::from([].as_ref()).into()],
    );

    assert!(result.is_ok());

    std::mem::drop(temp_dir);
    Ok(())
}

#[test]
fn test_summarize_state_metering_limits() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_MATERING)?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    // Create test conditions with very high iterations to trigger metering
    let test_conditions = TestConditions {
        max_iterations: 100_000_000,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);

    let result = runtime.summarize_state(&contract_key, &Parameters::from([].as_ref()), &state);

    assert!(result.is_err());
    let result_err = result.unwrap_err();

    let is_gas_error = match result_err.deref() {
        RuntimeInnerError::ContractExecError(ContractExecError::OutOfGas) => true,
        _ => false,
    };

    assert!(is_gas_error);

    // Test with reasonable iterations
    let test_conditions = TestConditions {
        max_iterations: 1000, // This should work fine
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);

    let result = runtime.summarize_state(&contract_key, &Parameters::from([].as_ref()), &state);

    assert!(result.is_ok());

    std::mem::drop(temp_dir);
    Ok(())
}

#[test]
fn test_get_state_delta_metering_limits() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_MATERING)?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();

    // Create test conditions with very high iterations to trigger metering
    let test_conditions = TestConditions {
        max_iterations: 100_000_000,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);

    let summary = StateSummary::from(vec![]);

    let result = runtime.get_state_delta(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &summary,
    );

    assert!(result.is_err());
    let result_err = result.unwrap_err();

    let is_gas_error = match result_err.deref() {
        RuntimeInnerError::ContractExecError(ContractExecError::OutOfGas) => true,
        _ => false,
    };

    assert!(is_gas_error);

    // Test with reasonable iterations
    let test_conditions = TestConditions {
        max_iterations: 1000, // This should work fine
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);

    let result = runtime.get_state_delta(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &summary,
    );

    assert!(result.is_ok());

    std::mem::drop(temp_dir);
    Ok(())
}
