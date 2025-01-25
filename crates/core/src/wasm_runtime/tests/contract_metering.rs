use crate::wasm_runtime::tests::TestSetup;
use crate::wasm_runtime::{ContractExecError, RuntimeInnerError, RuntimeResult};
use freenet_stdlib::prelude::*;
use std::time::Instant;

use super::super::contract::*;
use super::super::Runtime;

const TEST_CONTRACT_METERING: &str = "test_contract_metering";

// Maximum cycles = 20s * 3GHz * 1.2 = 72 billion cycles
// Cost function:
// - Loop operations: 25 cycles
// - All other operations: 1 cycle
//
// Cycle costs per iteration in each function:
// 1. validate_state: 26 cycles (loop=25, add=1)
// 2. update_state: 28 cycles (loop=25, mod=1, cmp=1, add=1)
// 3. summarize_state: 32 cycles (loop=25, mod*2=2, cmp*3=3, add*2=2)
// 4. get_state_delta: 35 cycles (loop=25, mod*3=3, cmp*4=4, add*3=3)

// For high iterations, we want to exceed the gas limit (72B cycles)
// Using validate_state (26 cycles/iter), we need:
// 72B / 26 = ~2.77B iterations to hit the limit
const HIGH_ITERATIONS: u64 = 3_000_000_000; // 3B iterations * 26 cycles = 78B cycles

// For reasonable iterations, we target ~1% of max cycles (720M cycles)
// Using get_state_delta (35 cycles/iter), we need:
// 720M / 35 = ~20.6M iterations to hit 1%
const REASONABLE_ITERATIONS: u64 = 15_000_000; // 15M iterations * 35 cycles = 525M cycles (~0.73% of limit)

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct TestConditions {
    pub iterations: u64,
}

struct MeteringTest {
    runtime: Runtime,
    contract_key: ContractKey,
}

impl MeteringTest {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let TestSetup {
            contract_store,
            delegate_store,
            secrets_store,
            contract_key,
            temp_dir,
        } = super::setup_test_contract(TEST_CONTRACT_METERING)?;

        let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)?;

        Ok(Self {
            runtime,
            contract_key,
        })
    }

    fn create_state(&self, iterations: u64) -> Result<WrappedState, Box<dyn std::error::Error>> {
        let test_conditions = TestConditions { iterations };
        Ok(WrappedState::new(serde_json::to_vec(&test_conditions)?))
    }

    fn test_operation<F>(&mut self, iterations: u64, operation: F) -> Result<(bool, f64), Box<dyn std::error::Error>> 
    where
        F: FnOnce(&mut Runtime, &ContractKey, &WrappedState) -> RuntimeResult<()>
    {
        let state = self.create_state(iterations)?;
        let start = Instant::now();
        let result = operation(&mut self.runtime, &self.contract_key, &state);
        let duration = start.elapsed().as_secs_f64();

        let is_gas_error = match &result {
            Err(err) => matches!(
                err.deref(),
                RuntimeInnerError::ContractExecError(ContractExecError::OutOfGas)
            ),
            Ok(_) => false,
        };

        Ok((is_gas_error, duration))
    }
}

#[test]
fn test_validate_state_metering() -> Result<(), Box<dyn std::error::Error>> {
    let mut test = MeteringTest::new()?;

    // Test with high iterations (78B cycles = 3B * 26)
    let (is_gas_error, duration) = test.test_operation(HIGH_ITERATIONS, |runtime, key, state| {
        runtime.validate_state(key, &Parameters::from([].as_ref()), state, &Default::default())?;
        Ok(())
    })?;
    assert!(is_gas_error, "High iterations should cause gas error");
    println!("Validate state with {} iterations took {:.2}s before failing", HIGH_ITERATIONS, duration);

    // Test with reasonable iterations (390M cycles = 15M * 26)
    let (is_gas_error, duration) = test.test_operation(REASONABLE_ITERATIONS, |runtime, key, state| {
        runtime.validate_state(key, &Parameters::from([].as_ref()), state, &Default::default())?;
        Ok(())
    })?;
    assert!(!is_gas_error, "Reasonable iterations should succeed");
    println!("Validate state with {} iterations took {:.2}s", REASONABLE_ITERATIONS, duration);

    Ok(())
}

#[test]
fn test_update_state_metering() -> Result<(), Box<dyn std::error::Error>> {
    let mut test = MeteringTest::new()?;

    // Test with high iterations (84B cycles = 3B * 28)
    let (is_gas_error, duration) = test.test_operation(HIGH_ITERATIONS, |runtime, key, state| {
        runtime.update_state(
            key,
            &Parameters::from([].as_ref()),
            state,
            &[StateDelta::from([].as_ref()).into()],
        )?;
        Ok(())
    })?;
    assert!(is_gas_error, "High iterations should cause gas error");
    println!("Update state with {} iterations took {:.2}s before failing", HIGH_ITERATIONS, duration);

    // Test with reasonable iterations (420M cycles = 15M * 28)
    let (is_gas_error, duration) = test.test_operation(REASONABLE_ITERATIONS, |runtime, key, state| {
        runtime.update_state(
            key,
            &Parameters::from([].as_ref()),
            state,
            &[StateDelta::from([].as_ref()).into()],
        )?;
        Ok(())
    })?;
    assert!(!is_gas_error, "Reasonable iterations should succeed");
    println!("Update state with {} iterations took {:.2}s", REASONABLE_ITERATIONS, duration);

    Ok(())
}

#[test]
fn test_summarize_state_metering() -> Result<(), Box<dyn std::error::Error>> {
    let mut test = MeteringTest::new()?;

    // Test with high iterations (96B cycles = 3B * 32)
    let (is_gas_error, duration) = test.test_operation(HIGH_ITERATIONS, |runtime, key, state| {
        runtime.summarize_state(key, &Parameters::from([].as_ref()), state)?;
        Ok(())
    })?;
    assert!(is_gas_error, "High iterations should cause gas error");
    println!("Summarize state with {} iterations took {:.2}s before failing", HIGH_ITERATIONS, duration);

    // Test with reasonable iterations (480M cycles = 15M * 32)
    let (is_gas_error, duration) = test.test_operation(REASONABLE_ITERATIONS, |runtime, key, state| {
        runtime.summarize_state(key, &Parameters::from([].as_ref()), state)?;
        Ok(())
    })?;
    assert!(!is_gas_error, "Reasonable iterations should succeed");
    println!("Summarize state with {} iterations took {:.2}s", REASONABLE_ITERATIONS, duration);

    Ok(())
}

#[test]
fn test_get_state_delta_metering() -> Result<(), Box<dyn std::error::Error>> {
    let mut test = MeteringTest::new()?;

    // Test with high iterations (105B cycles = 3B * 35)
    let (is_gas_error, duration) = test.test_operation(HIGH_ITERATIONS, |runtime, key, state| {
        runtime.get_state_delta(
            key,
            &Parameters::from([].as_ref()),
            state,
            &StateSummary::from([].as_ref()),
        )?;
        Ok(())
    })?;
    assert!(is_gas_error, "High iterations should cause gas error");
    println!("Get state delta with {} iterations took {:.2}s before failing", HIGH_ITERATIONS, duration);

    // Test with reasonable iterations (525M cycles = 15M * 35)
    let (is_gas_error, duration) = test.test_operation(REASONABLE_ITERATIONS, |runtime, key, state| {
        runtime.get_state_delta(
            key,
            &Parameters::from([].as_ref()),
            state,
            &StateSummary::from([].as_ref()),
        )?;
        Ok(())
    })?;
    assert!(!is_gas_error, "Reasonable iterations should succeed");
    println!("Get state delta with {} iterations took {:.2}s", REASONABLE_ITERATIONS, duration);

    Ok(())
}
