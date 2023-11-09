use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use freenet_stdlib::prelude::{
    ContractCode, ContractContainer, ContractKey, ContractWasmAPIVersion, WrappedContract,
};

use crate::util::tests::get_temp_dir;

use super::{ContractStore, DelegateStore, SecretsStore};

mod contract;
mod time;

pub(crate) fn get_test_module(name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let module_path = {
        const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let contracts = PathBuf::from(CONTRACTS_DIR);
        let mut dirs = contracts.ancestors();
        let path = dirs.nth(2).unwrap();
        path.join("tests").join(name.replace('_', "-"))
    };
    const TARGET_DIR_VAR: &str = "CARGO_TARGET_DIR";
    let target = std::env::var(TARGET_DIR_VAR).map_err(|_| "CARGO_TARGET_DIR should be set")?;
    println!("trying to compile the test contract, target: {target}");
    // attempt to compile it
    const RUST_TARGET_ARGS: &[&str] = &["build", "--target"];
    const WASM_TARGET: &str = "wasm32-unknown-unknown";
    let cmd_args = RUST_TARGET_ARGS
        .iter()
        .copied()
        .chain([WASM_TARGET])
        .collect::<Vec<_>>();
    let mut child = Command::new("cargo")
        .args(&cmd_args)
        .current_dir(&module_path)
        .spawn()?;
    child.wait()?;
    let output_file = Path::new(&target)
        .join(WASM_TARGET)
        .join("debug")
        .join(name)
        .with_extension("wasm");
    println!("output file: {output_file:?}");
    Ok(std::fs::read(output_file)?)
}

pub(crate) struct TestSetup {
    #[allow(unused)]
    temp_dir: tempfile::TempDir,
    contract_store: ContractStore,
    delegate_store: DelegateStore,
    secrets_store: SecretsStore,
    contract_key: ContractKey,
}

pub(crate) fn setup_test_contract(name: &str) -> Result<TestSetup, Box<dyn std::error::Error>> {
    // let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
    let temp_dir = get_temp_dir();

    let mut contract_store = ContractStore::new(temp_dir.path().join("contract"), 10_000)?;
    let delegate_store = DelegateStore::new(temp_dir.path().join("delegate"), 10_000)?;
    let secrets_store = SecretsStore::new(temp_dir.path().join("secrets"))?;
    let contract_bytes = WrappedContract::new(
        Arc::new(ContractCode::from(get_test_module(name)?)),
        vec![].into(),
    );
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_bytes));
    let contract_key = contract.key();
    contract_store.store_contract(contract)?;
    Ok(TestSetup {
        temp_dir,
        contract_store,
        delegate_store,
        secrets_store,
        contract_key,
    })
}
