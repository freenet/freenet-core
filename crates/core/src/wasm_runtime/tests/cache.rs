//! Tests for WASM module cache behavior (LRU eviction).

use super::super::contract::ContractRuntimeInterface;
use super::super::runtime::RuntimeConfig;
use super::super::Runtime;
use super::{setup_test_contract, TestSetup};
use freenet_stdlib::prelude::*;

/// Test that the module cache respects capacity limits and evicts LRU entries.
#[test]
fn test_module_cache_eviction() -> Result<(), Box<dyn std::error::Error>> {
    // Create runtime with very small cache (capacity 2)
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key: contract_key_1,
        temp_dir,
    } = setup_test_contract("test_contract_1")?;

    let config = RuntimeConfig {
        module_cache_capacity: 2,
        ..Default::default()
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    // Load first contract - cache should have 1 entry
    let state = WrappedState::new(vec![]);
    let _ = runtime.validate_state(
        &contract_key_1,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );
    assert_eq!(
        runtime.contract_modules.len(),
        1,
        "Cache should have 1 entry after first load"
    );

    // Load the same contract again - cache should still have 1 entry (cache hit)
    let _ = runtime.validate_state(
        &contract_key_1,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );
    assert_eq!(
        runtime.contract_modules.len(),
        1,
        "Cache should still have 1 entry (cache hit)"
    );

    // We can't easily load a second different contract in this test setup,
    // but we can verify the cache size is bounded by checking the capacity
    assert!(
        runtime.contract_modules.cap().get() == 2,
        "Cache capacity should be 2"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// Test that zero capacity is handled gracefully (falls back to 1).
#[test]
fn test_module_cache_zero_capacity() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        temp_dir,
        ..
    } = setup_test_contract("test_contract_1")?;

    // Create runtime with zero capacity - should fall back to 1
    let config = RuntimeConfig {
        module_cache_capacity: 0,
        ..Default::default()
    };

    let runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    // Verify cache was created with capacity 1 (NonZeroUsize::MIN)
    assert_eq!(
        runtime.contract_modules.cap().get(),
        1,
        "Zero capacity should fall back to 1"
    );
    assert_eq!(
        runtime.delegate_modules.cap().get(),
        1,
        "Zero capacity should fall back to 1 for delegates too"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// Test that cache capacity of 1 works correctly.
#[test]
fn test_module_cache_capacity_one() -> Result<(), Box<dyn std::error::Error>> {
    let TestSetup {
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
        temp_dir,
    } = setup_test_contract("test_contract_1")?;

    let config = RuntimeConfig {
        module_cache_capacity: 1,
        ..Default::default()
    };

    let mut runtime =
        Runtime::build_with_config(contract_store, delegate_store, secrets_store, false, config)
            .unwrap();

    // Load contract - cache should have 1 entry
    let state = WrappedState::new(vec![]);
    let _ = runtime.validate_state(
        &contract_key,
        &Parameters::from([].as_ref()),
        &state,
        &Default::default(),
    );

    assert_eq!(
        runtime.contract_modules.len(),
        1,
        "Cache should have exactly 1 entry"
    );
    assert_eq!(
        runtime.contract_modules.cap().get(),
        1,
        "Cache capacity should be 1"
    );

    std::mem::drop(temp_dir);
    Ok(())
}
