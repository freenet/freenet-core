use super::super::contract::*;
use super::super::Runtime;
use crate::wasm_runtime::runtime::RuntimeConfig;
use crate::wasm_runtime::tests::TestSetup;
use crate::wasm_runtime::{ContractExecError, RuntimeInnerError};
use freenet_stdlib::prelude::*;
use std::time::Instant;

const TEST_CONTRACT_METERING: &str = "test_contract_metering";

const HIGH_ITERATIONS: u64 = 5_000_000_000;

const TIMEOUT_ITERATIONS: u64 = 100_000_000_000;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct TestConditions {
    pub iterations: u64,
}

#[test]
fn validate_state_metering() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_METERING)?;

    let config = RuntimeConfig {
        max_execution_seconds: 5.0,
        cpu_cycles_per_second: Some(1_000_000), // Lower limit to force gas error
        safety_margin: 0.1,
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    let test_conditions = TestConditions {
        iterations: HIGH_ITERATIONS,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);
    let time = Instant::now();

    let result = runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );

    let duration = time.elapsed().as_secs_f64();
    println!("Duration: {:.2}s", duration);

    assert!(duration < 5.0, "Should not timeout");
    assert!(
        matches!(
            result.as_ref().err().map(|e| e.deref()),
            Some(RuntimeInnerError::ContractExecError(
                ContractExecError::OutOfGas
            ))
        ),
        "Should fail with gas error"
    );
    std::mem::drop(temp_dir);
    Ok(())
}

#[test]
fn test_update_state_metering() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_METERING)?;

    let config = RuntimeConfig {
        max_execution_seconds: 5.0,
        cpu_cycles_per_second: Some(2_000_000),
        safety_margin: 0.1,
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    let test_conditions = TestConditions {
        iterations: HIGH_ITERATIONS,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);
    let time = Instant::now();

    let result = runtime.update_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &[StateDelta::from([].as_ref()).into()],
    );

    let duration = time.elapsed().as_secs_f64();
    println!("Duration: {:.2}s", duration);

    assert!(duration < 5.0, "Should not timeout");
    assert!(
        matches!(
            result.as_ref().err().map(|e| e.deref()),
            Some(RuntimeInnerError::ContractExecError(
                ContractExecError::OutOfGas
            ))
        ),
        "Should fail with gas error"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

#[test]
fn test_summarize_state_metering() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_METERING)?;

    let config = RuntimeConfig {
        max_execution_seconds: 5.0,
        cpu_cycles_per_second: Some(3_000_000),
        safety_margin: 0.1,
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    let test_conditions = TestConditions {
        iterations: HIGH_ITERATIONS,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);
    let time = Instant::now();

    let result = runtime.summarize_state(&contract_key, &Parameters::from([].as_ref()), &state);

    let duration = time.elapsed().as_secs_f64();
    println!("Duration: {:.2}s", duration);

    assert!(duration < 5.0, "Should not timeout");
    assert!(
        matches!(
            result.as_ref().err().map(|e| e.deref()),
            Some(RuntimeInnerError::ContractExecError(
                ContractExecError::OutOfGas
            ))
        ),
        "Should fail with gas error"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

#[test]
fn test_get_state_delta_metering() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_METERING)?;

    let config = RuntimeConfig {
        max_execution_seconds: 5.0,
        cpu_cycles_per_second: Some(4_000_000),
        safety_margin: 0.1,
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    let test_conditions = TestConditions {
        iterations: HIGH_ITERATIONS,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);
    let time = Instant::now();

    let result = runtime.get_state_delta(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &StateSummary::from([].as_ref()),
    );

    let duration = time.elapsed().as_secs_f64();
    println!("Duration: {:.2}s", duration);

    assert!(duration < 5.0, "Should not timeout");
    assert!(
        matches!(
            result.as_ref().err().map(|e| e.deref()),
            Some(RuntimeInnerError::ContractExecError(
                ContractExecError::OutOfGas
            ))
        ),
        "Should fail with gas error"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

#[test]
fn test_timeout_metering() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = super::setup_test_contract(TEST_CONTRACT_METERING)?;

    let config = RuntimeConfig {
        max_execution_seconds: 5.0,
        cpu_cycles_per_second: Some(u64::MAX),
        safety_margin: 0.1,
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    let test_conditions = TestConditions {
        iterations: TIMEOUT_ITERATIONS,
    };
    let state = WrappedState::new(serde_json::to_vec(&test_conditions)?);
    let time = Instant::now();

    let result = runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );

    let duration = time.elapsed().as_secs_f64();
    println!("Duration: {:.2}s", duration);

    assert!(
        matches!(
            result.as_ref().err().map(|e| e.deref()),
            Some(RuntimeInnerError::ContractExecError(
                ContractExecError::MaxComputeTimeExceeded
            ))
        ),
        "Should fail with timeout error"
    );

    std::mem::drop(temp_dir);
    Ok(())
}
