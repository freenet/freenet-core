use std::path::PathBuf;

use locutus_runtime::prelude::*;
use locutus_stdlib::prelude::*;

fn test_dir() -> PathBuf {
    let test_dir = std::env::temp_dir().join("locutus").join("contracts");
    if !test_dir.exists() {
        std::fs::create_dir_all(&test_dir).unwrap();
    }
    test_dir
}

fn test_contract(contract_path: &str) -> WrappedContract {
    const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let contracts = PathBuf::from(CONTRACTS_DIR);
    let mut dirs = contracts.ancestors();
    let path = dirs.nth(2).unwrap();
    let contract_path = path
        .join("contracts")
        .join("test_contract")
        .join(contract_path);
    WrappedContract::try_from((&*contract_path, Parameters::from(vec![]))).expect("contract found")
}

fn get_guest_test_contract() -> RuntimeResult<(ContractStore, ContractKey)> {
    let mut store = ContractStore::new(test_dir(), 10_000);
    let contract = test_contract("test_contract_guest.wasm");
    let key = *contract.key();
    store.store_contract(contract)?;
    Ok((store, key))
}

#[test]
fn validate_compiled_with_guest_mem() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = get_guest_test_contract()?;

    let mut runtime = Runtime::build(store, false).unwrap();
    // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires buding for wasi
    let is_valid = runtime.validate_state(
        &key,
        Parameters::from([].as_ref()),
        State::from([1, 2, 3, 4].as_ref()),
    )?;
    assert!(is_valid);
    let not_valid = !runtime.validate_state(
        &key,
        Parameters::from([].as_ref()),
        State::from([1, 0, 0, 1].as_ref()),
    )?;
    assert!(not_valid);
    Ok(())
}

#[test]
fn validate_compiled_with_host_mem() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = ContractStore::new(test_dir(), 10_000);
    let contract = test_contract("test_contract_host.wasm");
    let key = *contract.key();
    store.store_contract(contract)?;

    let mut runtime = Runtime::build(store, true).unwrap();
    // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
    let is_valid = runtime.validate_state(
        &key,
        Parameters::from([].as_ref()),
        State::from([1, 2, 3, 4].as_ref()),
    )?;
    assert!(is_valid);
    let not_valid = !runtime.validate_state(
        &key,
        Parameters::from([].as_ref()),
        State::from([1, 0, 0, 1].as_ref()),
    )?;
    assert!(not_valid);
    Ok(())
}

#[test]
fn validate_delta() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = ContractStore::new(test_dir(), 10_000);
    let contract = test_contract("test_contract_host.wasm");
    let key = *contract.key();
    store.store_contract(contract)?;

    let mut runtime = Runtime::build(store, true).unwrap();
    // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
    let is_valid = runtime.validate_delta(
        &key,
        Parameters::from([].as_ref()),
        StateDelta::from([1, 2, 3, 4].as_ref()),
    )?;
    assert!(is_valid);
    let not_valid = !runtime.validate_delta(
        &key,
        Parameters::from([].as_ref()),
        StateDelta::from([1, 0, 0, 1].as_ref()),
    )?;
    assert!(not_valid);
    Ok(())
}

#[test]
fn update_state() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = get_guest_test_contract()?;
    let mut runtime = Runtime::build(store, false).unwrap();
    // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
    let new_state = runtime.update_state(
        &key,
        Parameters::from([].as_ref()),
        State::from([5, 2, 3].as_ref()),
        StateDelta::from([4].as_ref()),
    )?;
    assert!(new_state.as_ref().len() == 4);
    assert!(new_state.as_ref()[3] == 4);
    Ok(())
}

#[test]
fn summarize_state() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = get_guest_test_contract()?;
    let mut runtime = Runtime::build(store, false).unwrap();
    // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
    let summary = runtime.summarize_state(
        &key,
        Parameters::from([].as_ref()),
        State::from([5, 2, 3, 4].as_ref()),
    )?;
    assert!(summary.as_ref().len() == 1);
    assert!(summary.as_ref()[0] == 5);
    Ok(())
}

#[test]
fn get_state_delta() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = get_guest_test_contract()?;
    let mut runtime = Runtime::build(store, false).unwrap();
    // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
    let delta = runtime.get_state_delta(
        &key,
        Parameters::from([].as_ref()),
        State::from([5, 2, 3, 4].as_ref()),
        StateSummary::from([2, 3].as_ref()),
    )?;
    assert!(delta.as_ref().len() == 1);
    assert!(delta.as_ref()[0] == 4);
    Ok(())
}

#[test]
fn update_state_from_summary() -> Result<(), Box<dyn std::error::Error>> {
    let (store, key) = get_guest_test_contract()?;
    let mut runtime = Runtime::build(store, false).unwrap();
    // runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
    let new_state = runtime.update_state_from_summary(
        &key,
        Parameters::from([].as_ref()),
        State::from([5, 2, 3].as_ref()),
        StateSummary::from([4].as_ref()),
    )?;
    assert!(new_state.as_ref().len() == 4);
    assert!(new_state.as_ref()[3] == 4);
    Ok(())
}
